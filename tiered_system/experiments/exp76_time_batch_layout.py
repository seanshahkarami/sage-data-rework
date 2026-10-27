"""Build the chronological-batch physical candidate locally with DuckDB 2.0.

Uses the seeder's actual staging/window SQL and Go layoutpack, without REST
commits, Pelican writes or API changes. Retains output for exp74 comparisons.
All source files are read-only; --work-dir must be a fresh isolated directory.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import time

import pyarrow.ipc as ipc
import pyarrow.parquet as pq

HERE=Path(__file__).resolve().parent
sys.path[:0]=[str(HERE),str(HERE.parent/'demo/tools')]
from exp59_arrow_ipc_compression import monitored
from exp66_duckdb20_evaluation import validate_version
from exp72_plugin_layout import scan
from exp74_layout_scan_profiles import inventory
from v3_layout import ordered_day_sql,batch_sql,FIELD_IDS,FILE_ROWS,ROW_GROUP_ROWS,sql_quote,audit_data_files


def native(binary,sql,memory,root):
    root.mkdir();statement=root/'query.sql';statement.write_text(sql)
    _,peak=monitored([str(binary),'-test.run=^TestColdDuckDBEvaluationProbe$','-test.count=1'],
        dict(os.environ,COLD_EVAL_DIR=str(root),COLD_EVAL_SQL=str(statement),COLD_EVAL_MEMORY=memory,
             COLD_EVAL_ASYNC_THREADS='8',COLD_EVAL_PROFILE='0'))
    report=json.loads((root/'run.json').read_text());validate_version('2.0.0',report)
    with (root/'body.arrows').open('rb') as stream:result=ipc.open_stream(stream).read_all()
    return result,peak


def copy_result(sql):
    return "WITH w AS ("+sql+") SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'copy'::VARCHAR AS name,Count::VARCHAR AS value,'{}'::VARCHAR AS meta FROM w"


def fingerprint_sql(source):
    h='hash(time,name,value,meta,plugin,vsn,node,host,task,job,zone)'
    return f"SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,count(*)::VARCHAR AS name,sum({h})::VARCHAR AS value,bit_xor({h})::VARCHAR AS meta FROM {source}"


def fingerprint(table):
    row=table.to_pylist()[0];return [int(row['name']),row['value'],row['meta']]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--native-probe',type=Path,required=True)
    p.add_argument('--layout-helper',type=Path,required=True)
    p.add_argument('--work-dir',type=Path,required=True)
    p.add_argument('--memory',default='6GB')
    p.add_argument('--row-group-rows',type=int,default=ROW_GROUP_ROWS)
    p.add_argument('--output',type=Path,default=HERE/'results/exp76_bounded_time_batch_layout.json')
    a=p.parse_args();root=a.work_dir.resolve()
    if 'node-data' in root.parts or root.exists():p.error('use a fresh isolated work directory')
    if not 100000<=a.row_group_rows<=1000000:p.error('row-group-rows must be between 100000 and 1000000')
    files,info=inventory(a.source)
    root.mkdir(parents=True);(root/'probes').mkdir();data=root/'data';data.mkdir()
    report=dict(scope=__doc__,status='running',work_dir=str(root),source_inventory=info,batch_order='time',
                memory=a.memory,file_row_limit=FILE_ROWS,row_group_rows=a.row_group_rows,batches=[],native_peak_rss_bytes=0,helper_peak_rss_bytes=0)
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    def execute(sql,label):
        table,peak=native(a.native_probe.resolve(),sql,a.memory,root/'probes'/label)
        report['native_peak_rss_bytes']=max(report['native_peak_rss_bytes'],peak);save();return table
    began=time.perf_counter();save()
    expected=fingerprint(execute(fingerprint_sql(scan(files)),'source-fingerprint'))
    stage=root/'ordered-day.parquet'
    print('Sorting complete day to local Parquet',flush=True)
    staged=execute(copy_result(ordered_day_sql('SELECT * FROM '+scan(files),stage)),'stage')
    if int(staged.column('value')[0].as_py())!=expected[0]:raise ValueError('staging row count changed')
    if fingerprint(execute(fingerprint_sql(scan([stage])),'stage-fingerprint'))!=expected:raise ValueError('staging changed raw rows')
    metadata={'format-version':3,'table-uuid':'local-physical-trial','current-schema-id':0,'snapshots':[],
              'properties':{'sage.batch.order':'time','sage.row-group.rows':str(a.row_group_rows)},'schemas':[{'schema-id':0,'fields':[{'id':11,'name':'value','type':'variant'}]}]}
    for index,offset in enumerate(range(0,expected[0],FILE_ROWS)):
        batch=batch_sql('',offset,FILE_ROWS,stage)
        expected_batch=fingerprint(execute(fingerprint_sql(batch),f'batch-{index}-fingerprint'))
        if expected_batch[0]!=min(FILE_ROWS,expected[0]-offset):raise ValueError('incorrect fixed batch size')
        intermediate=root/'batch.parquet';out=data/f'batch-{index:04d}.parquet'
        statement='COPY (SELECT * FROM '+batch+' ORDER BY plugin NULLS LAST,vsn NULLS LAST,time NULLS LAST) TO '+sql_quote(intermediate)+f' (FORMAT parquet,COMPRESSION zstd,COMPRESSION_LEVEL 3,FIELD_IDS {FIELD_IDS})'
        execute(copy_result(statement),f'batch-{index}-write')
        _,peak=monitored([str(a.layout_helper.resolve()),'--input',str(intermediate),'--output',str(out),
                         '--row-group-rows',str(a.row_group_rows)],dict(os.environ))
        report['helper_peak_rss_bytes']=max(report['helper_peak_rss_bytes'],peak)
        audit=audit_data_files(metadata,{out:expected_batch[0]},'plugin-vsn-time')
        actual=fingerprint(execute(fingerprint_sql(scan([out])),f'batch-{index}-verify'))
        if actual!=expected_batch:raise ValueError('packed batch changed raw rows')
        intermediate.unlink();report['batches'].append(dict(path=str(out),fingerprint=actual,**audit));save()
        print(f'Batch {index+1}: {actual[0]} rows, {audit["row_groups"]} plugin groups',flush=True)
    outputs=sorted(data.glob('*.parquet'))
    report['audit']=audit_data_files(metadata,{f:pq.ParquetFile(f).metadata.num_rows for f in outputs},'plugin-vsn-time')
    report['fingerprint']=fingerprint(execute(fingerprint_sql(scan(outputs)),'final-fingerprint'))
    if report['fingerprint']!=expected:raise ValueError('complete packed day changed raw rows')
    stage.unlink();report['total_wall_seconds']=time.perf_counter()-began;report['status']='complete';save()
    print('Report:',a.output)


if __name__=='__main__':main()
