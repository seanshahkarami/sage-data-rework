"""Native DuckDB 2.0 layout comparison on a complete read-only v3 day.

Original files are read-only. Repacked time and plugin/vsn/time datasets use
isolated temporary files, identical Zstd/row-group/shredding settings. Plugin
boundaries are enforced by plugin-specific files (not an Iceberg partition
spec change). Explicit file lists and hive_partitioning=false avoid directory
pruning shortcuts. No Pelican, API deployment or catalog writes.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time

import pyarrow.parquet as pq

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp66_duckdb20_evaluation import run_probe,signature,sql_quote
from exp67_metadata_projection import native_table,MARKER


def scan(files):
    return 'read_parquet(['+','.join(sql_quote(p.resolve()) for p in files)+'],hive_partitioning=false)'


def write_sql(source,path,plugin,row_group_rows):
    options=f"FORMAT parquet,COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE {row_group_rows},SHREDDING {{'value':'DOUBLE'}}"
    options+=',PARTITION_BY(plugin),WRITE_PARTITION_COLUMNS true,ORDER_BY(vsn,time)' if plugin else ',ORDER_BY(time)'
    return f"""WITH written AS (COPY (SELECT * FROM {source}) TO {sql_quote(path)} ({options}))
 SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'write'::VARCHAR AS name,Count::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM written"""


def info(files):
    out=dict(files=len(files),rows=0,row_groups=0,bytes=0,mixed_plugin_groups=0,row_group_rows=[],codecs=set())
    for path in files:
        f=pq.ParquetFile(path);out['bytes']+=path.stat().st_size;out['rows']+=f.metadata.num_rows
        for i in range(f.metadata.num_row_groups):
            g=f.metadata.row_group(i);out['row_groups']+=1;out['row_group_rows'].append(g.num_rows)
            for j in range(g.num_columns):
                col=g.column(j);out['codecs'].add(col.compression)
                if col.path_in_schema=='plugin':
                    stats=col.statistics
                    if stats and stats.has_min_max and stats.min!=stats.max:out['mixed_plugin_groups']+=1
    out['codecs']=sorted(out['codecs'])
    return out


def eligible_groups(files,filters,start_ns,end_ns):
    total=rows=0
    for path in files:
        f=pq.ParquetFile(path)
        for i in range(f.metadata.num_row_groups):
            g=f.metadata.row_group(i)
            stats={g.column(j).path_in_schema:g.column(j).statistics for j in range(g.num_columns)}
            keep=True
            for key,value in filters.items():
                s=stats.get(key)
                if s and s.has_min_max and not s.min<=value<=s.max:keep=False
            s=stats.get('time')
            if s and s.has_min_max:
                if s.max.value<start_ns or s.min.value>=end_ns:keep=False
            if keep:total+=1;rows+=g.num_rows
    return dict(groups_not_excluded_by_minmax=total,rows_in_those_groups=rows)


def day_files(paths,start_ns,end_ns):
    """Select complete day-local files from a source that may now span months.

    Refuse boundary-spanning files rather than accidentally benchmarking extra
    days or using whole-file row counts for a clipped source.
    """
    selected=[]
    for path in paths:
        f=pq.ParquetFile(path);bounds=[]
        for i in range(f.metadata.num_row_groups):
            group=f.metadata.row_group(i)
            for j in range(group.num_columns):
                col=group.column(j)
                if col.path_in_schema=='time':
                    s=col.statistics
                    if not s or not s.has_min_max:raise ValueError('day selection requires time statistics')
                    bounds.append((s.min.value,s.max.value))
        if not bounds:raise ValueError('missing time column')
        if any(hi>=start_ns and lo<end_ns for lo,hi in bounds):
            if any(lo<start_ns or hi>=end_ns for lo,hi in bounds):
                raise ValueError('source file spans the selected day boundary; supply day-local Parquet')
            selected.append(path)
    return selected


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,default=HERE.parents[1]/'scratch/v3-1792707566/sage/obs')
    p.add_argument('--native-probe',type=Path,required=True)
    p.add_argument('--sql-probe',type=Path,required=True)
    p.add_argument('--memory',default='3GB')
    p.add_argument('--row-group-rows',type=int,default=262144)
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--match',default='')
    p.add_argument('--output',type=Path,default=HERE/'results/exp72_plugin_layout.json')
    a=p.parse_args()
    native,exporter=a.native_probe.resolve(),a.sql_probe.resolve()
    import pandas as pd
    start=pd.Timestamp('2025-03-01',tz='UTC')
    discovered=sorted(a.source.rglob('*.parquet'))
    files=day_files(discovered,start.value,(start+pd.Timedelta(days=1)).value)
    if not files or not native.is_file() or not exporter.is_file() or a.reps<1 or a.row_group_rows<2048:
        p.error('existing v3 files/probes and positive repetitions/row-group size required')
    report=dict(scope='local native Go DuckDB 2.0 streaming + Zstd IPC; no Iceberg/Pelican/HTTP request path',
        source=str(a.source),selected_day='2025-03-01',source_files_discovered=len(discovered),
        source_files_selected=len(files),memory_limit=a.memory,row_group_target=a.row_group_rows,
        native_sha256=hashlib.sha256(native.read_bytes()).hexdigest(),
        sql_exporter_sha256=hashlib.sha256(exporter.read_bytes()).hexdigest(),
        controls='both repacks Zstd level 3, root value shredded as DOUBLE, 2 CPU threads, async=8; fresh engine, OS cache warm; rotating layout order',
        grouping='plugin-specific local files guarantee unmixed plugin row groups; stored plugin column retained; no Hive path pruning',
        layouts={'original_time':info(files)},results=[],summaries=[],profiles=[],fingerprints={},phase='writing')
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    with tempfile.TemporaryDirectory(prefix='sage-plugin-layout-') as directory:
        root=Path(directory);sources={'original_time':files};original=scan(files)
        plugin_sql=f"SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'plugins'::VARCHAR AS name,to_json(list(DISTINCT plugin ORDER BY plugin))::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM {original}"
        plugins=json.loads(native_table(native,plugin_sql,a.memory,root/'plugins').column('value')[0].as_py())
        report['plugin_count']=len(plugins)
        for name in ('time_repacked','plugin_vsn_time'):
            path=root/name if name=='plugin_vsn_time' else root/(name+'.parquet')
            print('Rewriting',name,flush=True);began=time.perf_counter()
            copied_rows=0
            # One plugin per native writer process. A single partitioned COPY
            # across the full day exhausted 3 GB with 39 active partitions.
            partitions=list(enumerate(plugins)) if name=='plugin_vsn_time' else [(0,None)]
            for index,plugin_value in partitions:
                source=original;destination=path
                if name=='plugin_vsn_time':
                    predicate='plugin IS NULL' if plugin_value is None else 'plugin='+sql_quote(plugin_value)
                    source=f'(SELECT * FROM {original} WHERE {predicate})'
                    destination=path/f'part-{index:04d}'
                    destination.parent.mkdir(parents=True,exist_ok=True)
                table=native_table(native,write_sql(source,destination,name=='plugin_vsn_time',a.row_group_rows),a.memory,root/f'write-{name}-{index}')
                copied_rows+=int(table.column('value')[0].as_py())
                print(name,f'plugin pass {index+1}/{len(partitions)}',flush=True)
            sources[name]=sorted(path.rglob('*.parquet')) if path.is_dir() else [path]
            report['layouts'][name]=dict(**info(sources[name]),rewrite_wall_seconds=time.perf_counter()-began)
            if copied_rows!=report['layouts']['original_time']['rows']:raise AssertionError('COPY lost rows')
            if name=='plugin_vsn_time' and report['layouts'][name]['mixed_plugin_groups']:raise AssertionError('mixed plugin group')
            save();print(name,report['layouts'][name]['bytes'],'bytes',flush=True)
        # Whole-day multiset equality includes all values, tags and exact times,
        # independently of which benchmark query subsets are selected.
        report['phase']='full_day_fingerprints';save()
        for name,paths in sources.items():
            h='hash(time,name,value,meta,plugin,vsn,node,host,task,job,zone)'
            statement=f"SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'fingerprint'::VARCHAR AS name,to_json(struct_pack(rows:=count(*),hash_sum:=sum({h})::VARCHAR,hash_xor:=bit_xor({h})::VARCHAR))::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM {scan(paths)}"
            table=native_table(native,statement,a.memory,root/('fingerprint-'+name))
            report['fingerprints'][name]=json.loads(table.column('value')[0].as_py());save()
            if report['fingerprints'][name]!=report['fingerprints']['original_time']:raise AssertionError('full-day raw-row fingerprint changed')
            print(name,'full-day fingerprint matched',flush=True)
        statement=f"SELECT TIMESTAMP_NS '2025-03-01' AS timestamp,'plugin'::VARCHAR AS name,to_json(plugin)::VARCHAR AS value,'{{}}'::VARCHAR AS meta FROM {original} WHERE name='wxt.env.temp' AND vsn='W0A0' GROUP BY plugin ORDER BY count(*) DESC LIMIT 1"
        plugin=json.loads(native_table(native,statement,a.memory,root/'plugin').column('value')[0].as_py())
        report['selected_plugin']=plugin
        one={'name':'wxt.env.temp','vsn':'W0A0'}
        filters={'series':one,'plugin_vsn':{'plugin':plugin,'vsn':'W0A0'},'all_plugins_vsn':{'vsn':'W0A0'},'fleet':{}}
        definitions=[('series_minute','series',60),('series_hour','series',3600),('series_day','series',86400),
                     ('plugin_vsn_day','plugin_vsn',86400),('all_plugins_vsn_minute','all_plugins_vsn',60),
                     ('all_plugins_vsn_hour','all_plugins_vsn',3600),('fleet_minute','fleet',60),('fleet_hour','fleet',3600)]
        definitions=[d for d in definitions if a.match in d[0]]
        if not definitions:raise ValueError('no matching query cases')
        start=pd.Timestamp('2025-03-01',tz='UTC');cases=[]
        for name,key,seconds in definitions:
            end=start+pd.Timedelta(seconds=seconds)
            cases.append(dict(Name=name,Query=dict(start=start.isoformat(),end=end.isoformat(),filter=filters[key])))
        (root/'cases.json').write_text(json.dumps(cases))
        monitored([str(exporter),'-test.run=^TestColdProjectionProbe$','-test.count=1'],
                  dict(os.environ,COLD_PROJECTION_PROBE_DIR=str(root),COLD_PROJECTION_QUERIES=str(root/'cases.json')))
        report['phase']='query_timings';save()
        for case in cases:
            name=case['Name'];template=(root/(name+'-optimized.sql')).read_text();reference=None
            for rep in range(a.reps):
                names=list(sources);names=names[rep%len(names):]+names[:rep%len(names)]
                for layout in names:
                    sql=template.replace(MARKER,scan(sources[layout]))
                    run=run_probe(native,'2.0.0',sql,a.memory,8,False)
                    if reference is None:reference=signature(run)
                    if signature(run)!=reference:raise AssertionError('response content/schema/order changed')
                    report['results'].append(dict(case=name,layout=layout,repetition=rep+1,**run));save()
                    print(name,layout,f"rows={run['rows']} first={run['first_batch_seconds']:.3f}s total={run['total_seconds']:.3f}s",flush=True)
            for layout,paths in sources.items():
                runs=[r for r in report['results'] if r['case']==name and r['layout']==layout]
                query=case['Query']
                report['summaries'].append(dict(case=name,layout=layout,rows=runs[0]['rows'],
                    total_seconds=statistics.median(r['total_seconds'] for r in runs),
                    first_batch_seconds=statistics.median(r['first_batch_seconds'] for r in runs),
                    **eligible_groups(paths,query['filter'],pd.Timestamp(query['start']).value,pd.Timestamp(query['end']).value)))
                # Profile separately so diagnostics do not contaminate timers.
                if name in ('series_day','plugin_vsn_day','fleet_minute','fleet_hour'):
                    run=run_probe(native,'2.0.0',template.replace(MARKER,scan(paths)),a.memory,8,True)
                    if signature(run)!=reference:raise AssertionError('profiled response content differs')
                    report['profiles'].append(dict(case=name,layout=layout,**run))
            save()
        report['phase']='complete';save()
    print('Report:',a.output)


if __name__=='__main__':main()
