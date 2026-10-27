"""Benchmark SQL-only metadata JSON reuse without changing storage or HTTP.

Candidates reuse the current JSON expression and order-preserving MAP keys.
All discovery/group/join work is inside the native query timer. Native engine
initialization and validation are excluded. No windowing, remote writes or API
configuration changes. Local results still need same-image live confirmation.
The uniform prototype sums discovery/main query timers in separate fresh
processes; the API candidate uses its existing request database/connection.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp67_metadata_projection import native_table
from exp68_value_variant import semantic_fixture
from exp73_plugin_live_compare import full_warehouse_cases
from exp74_layout_scan_profiles import rotated_layouts
from exp83_projection_ablation import scan_from_audit,run,MARKER
from exp66_duckdb20_evaluation import validate_version
from exp65_cold_query_profile import load_timing
from exp73_plugin_live_compare import public_schema
import pyarrow.ipc as ipc

MODES=('baseline','correlated','dictionary','correlated_json','uniform')
META_MARKER='\'{"__SAGE_META_PLACEHOLDER__":true}\''
CASE_NAMES={'minute':'2025-03-01_series_minute','hour':'2025-03-01_series_hour',
            'day':'2025-03-01_series_day','march':'2025-03_series_month','april':'2025-04_series_month',
            'vsn_hour':'2025-03-01_all_plugins_vsn_hour','fleet_minute':'2025-03-01_fleet_minute'}
QUERIES=dict(full_warehouse_cases('unused'))


def export_sql(binary,root,cases):
    root.mkdir();path=root/'queries.json';path.write_text(json.dumps(cases))
    monitored([str(binary),'-test.run=^TestColdMetadataReuseProbe$','-test.count=1'],
              dict(os.environ,COLD_REUSE_PROBE_DIR=str(root),COLD_REUSE_QUERIES=str(path)))


def uniform_statement(baseline,template,metadata):
    if len(metadata)!=1:return baseline,'fallback'
    if template.count(META_MARKER)!=1 or not isinstance(json.loads(metadata[0]),dict):
        raise ValueError('invalid uniform metadata template/object')
    literal="'"+metadata[0].replace("'","''")+"'"
    return template.replace(META_MARKER,literal),'uniform'


def discover(native,sql,memory,profiled,archive):
    with tempfile.TemporaryDirectory(prefix='sage-exp84-discovery-') as directory:
        root=Path(directory);path=root/'query.sql';path.write_text(sql)
        _,peak=monitored([str(native),'-test.run=^TestColdDuckDBEvaluationProbe$','-test.count=1','-test.timeout=2h'],
            dict(os.environ,COLD_EVAL_DIR=str(root),COLD_EVAL_SQL=str(path),COLD_EVAL_MEMORY=memory,
                 COLD_EVAL_ASYNC_THREADS='8',COLD_EVAL_PROFILE='1' if profiled else '0'))
        report=json.loads((root/'run.json').read_text());validate_version('2.0.0',report)
        if not 0<=report['rows']<=2:raise ValueError('metadata discovery must return at most two tuples')
        with (root/'body.arrows').open('rb') as stream,ipc.open_stream(stream) as reader:
            public_schema(reader.schema);table=reader.read_all()
        metadata=table['meta'].to_pylist()
        if len(metadata)!=report['rows'] or any(not isinstance(value,str) or not isinstance(json.loads(value),dict) for value in metadata):
            raise ValueError('discovery did not return JSON objects')
        report.update(metadata=metadata,peak_process_rss_bytes=peak,sql_sha256=hashlib.sha256(sql.encode()).hexdigest())
        if profiled:
            timings=list(root.glob('*.timing.json'))
            if len(timings)!=1:raise ValueError('missing discovery timing')
            report['profile']=load_timing(timings[0])
            if not report['profile']['eligible']:raise ValueError('incomplete discovery profile')
            archive.mkdir(parents=True)
            for path in root.glob('*.json'):shutil.copyfile(path,archive/path.name)
            shutil.copyfile(root/'query.sql',archive/'query.sql')
            report['profile']['source']=str(archive/timings[0].name)
        return report


def measure(native,statements,discovery_sql,memory,profiled,mode,archive):
    if mode!='uniform' or not discovery_sql:
        return run(native,statements[mode],memory,profiled,'response',mode,False,archive)
    discovery=discover(native,discovery_sql,memory,profiled,archive/'discovery')
    statement,path=uniform_statement(statements['baseline'],statements['uniform'],discovery['metadata'])
    result=run(native,statement,memory,profiled,'response',mode,False,archive/'query')
    result.update(query_first_batch_seconds=result['first_batch_seconds'],query_total_seconds=result['total_seconds'],
                  discovery=discovery,selected_path=path,
                  timing_scope='sum of discovery and main native query timers in separate fresh engines; startup and validation excluded; not live API wall time')
    if result['rows']:result['first_batch_seconds']+=discovery['total_seconds']
    result['total_seconds']+=discovery['total_seconds']
    result['peak_process_rss_bytes']=max(result['peak_process_rss_bytes'],discovery['peak_process_rss_bytes'])
    return result


def semantic_source(path):
    semantic_fixture(path)
    table=pq.ParquetFile(path).read()
    # Equal mapping contents, different insertion order: exact JSON text must
    # survive dictionary grouping/correlation, including a promoted collision.
    extra=table.slice(0,3)
    for name,array in {
        'time':pa.array([1740787200002000194,1740787200002000195,1740787200002000196],type=pa.timestamp('ns',tz='UTC')),
        'meta':pa.array([[('plugin','raw'),('z','last'),('a','first')],
                         [('a','first'),('z','last'),('plugin','raw')],
                         [("q'key","it's \\ 中文\n\u2028")]],type=pa.map_(pa.string(),pa.string())),
        'plugin':pa.array(['promoted']*3),
    }.items():extra=extra.set_column(extra.schema.get_field_index(name),name,array)
    table=pa.concat_tables([table,extra]);pq.write_table(table,path)
    quote=lambda s:"'"+str(s).replace("'","''")+"'"
    return ("(SELECT * EXCLUDE(value_float,value_int,value_str),CASE "
            "WHEN value_float IS NOT NULL THEN value_float::DOUBLE::VARIANT "
            "WHEN value_int IS NOT NULL THEN value_int::BIGINT::VARIANT "
            "ELSE value_str::VARCHAR::VARIANT END AS value "
            "FROM read_parquet("+quote(path)+",hive_partitioning=false))"),len(table)


def validate_semantics(native,exporter,root,modes,memory):
    root.mkdir();source,count=semantic_source(root/'fixture.parquet')
    query=dict(start='2025-03-01T00:00:00Z',end='2025-03-02T00:00:00Z')
    cases=[dict(Name='fixture',Query=query),dict(Name='empty',Query=dict(query,filter={'name':'__absent__'})),
           dict(Name='head',Query=dict(query,head=1)),dict(Name='tail',Query=dict(query,tail=1)),
           dict(Name='order_pair',Query=dict(start='2025-03-01T00:00:00.002000194Z',end='2025-03-01T00:00:00.002000196Z'))]
    export_sql(exporter,root/'sql',cases);checks=[]
    for case in cases:
        reference=None
        for mode in modes:
            template=(root/'sql'/(case['Name']+'-'+mode+'.sql')).read_text()
            if template.count(MARKER)!=1:raise ValueError('semantic scan marker changed')
            statement=template.replace(MARKER,source)
            if mode=='uniform':
                discovery_sql=(root/'sql'/(case['Name']+'-discovery.sql')).read_text()
                if discovery_sql:
                    discovery=discover(native,discovery_sql.replace(MARKER,source),memory,False,root/'unused')
                    baseline=(root/'sql'/(case['Name']+'-baseline.sql')).read_text().replace(MARKER,source)
                    statement,path=uniform_statement(baseline,statement,discovery['metadata'])
                else:path='head_tail_fallback'
            table=native_table(native,statement,memory,root/(case['Name']+'-'+mode))
            if reference is None:reference=table
            if not table.equals(reference,check_metadata=True):raise ValueError(case['Name']+': exact Arrow/JSON output changed in '+mode)
            if case['Name']=='fixture':
                if len(table)!=count or table['meta'][-3].as_py()==table['meta'][-2].as_py():
                    raise ValueError('MAP insertion-order fixture is not distinguishing serialized order')
            if case['Name']=='empty' and len(table):raise ValueError('empty query returned rows')
            if case['Name']=='order_pair':
                if len(table)!=2 or table['meta'][0].as_py()==table['meta'][1].as_py():raise ValueError('ordered MAP key fixture lost distinct JSON')
                if mode=='uniform' and path!='fallback':raise ValueError('two MAP orders must not select a uniform literal')
            checks.append(dict(case=case['Name'],mode=mode,rows=len(table),exact_table_equal=True,
                               **(dict(selected_path=path) if mode=='uniform' else {})))
    if 'uniform' in modes:
        # Exercise the literal fast path, not merely the heterogeneous fallback:
        # NULL/empty MAPs, Unicode/quotes, collisions, NULL values and both MAP orders.
        original=pq.ParquetFile(root/'fixture.parquet').read()
        uniform_cases=[]
        for index in (0,1,5,10,12,27,1018,1024,1025,1026):
            stamp=original['time'][index].as_py()
            start=stamp.isoformat();end=(stamp+pd.Timedelta(1,unit='ns')).isoformat()
            uniform_cases.append(dict(Name='uniform_'+str(index),Query=dict(start=start,end=end)))
        export_sql(exporter,root/'uniform_sql',uniform_cases)
        for case in uniform_cases:
            stem=case['Name'];sqlroot=root/'uniform_sql'
            baseline=(sqlroot/(stem+'-baseline.sql')).read_text().replace(MARKER,source)
            template=(sqlroot/(stem+'-uniform.sql')).read_text().replace(MARKER,source)
            discovery=discover(native,(sqlroot/(stem+'-discovery.sql')).read_text().replace(MARKER,source),memory,False,root/'unused')
            statement,path=uniform_statement(baseline,template,discovery['metadata'])
            if path!='uniform':raise ValueError('uniform semantic fixture did not select fast path')
            control=native_table(native,baseline,memory,root/(stem+'-baseline'))
            actual=native_table(native,statement,memory,root/(stem+'-uniform'))
            if len(actual)!=1 or not actual.equals(control,check_metadata=True):raise ValueError('uniform literal changed exact output')
            checks.append(dict(case=stem,mode='uniform',rows=1,selected_path=path,exact_table_equal=True))
    return checks


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--native-probe',type=Path,required=True)
    p.add_argument('--sql-probe',type=Path,required=True)
    p.add_argument('--candidate-audit',type=Path,required=True)
    p.add_argument('--mode',action='append',choices=MODES[1:])
    p.add_argument('--case',action='append',choices=tuple(CASE_NAMES))
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--memory',default='3GB')
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();native=a.native_probe.resolve();exporter=a.sql_probe.resolve();artifacts=a.output.with_suffix('')
    if a.output.exists() or artifacts.exists() or 'node-data' in a.output.resolve().parts:p.error('fresh isolated output/artifact paths required')
    if a.reps<1 or not native.is_file() or not exporter.is_file():p.error('existing probes and positive repetitions required')
    audit=json.loads(a.candidate_audit.read_text());scan=scan_from_audit(audit)
    modes=['baseline']+list(dict.fromkeys(a.mode or ['uniform']));names=list(dict.fromkeys(a.case or CASE_NAMES))
    historical=json.loads((HERE/'results/full-layout-1792962354/http-arrow.json').read_text())
    counts={name:next(r['rows'] for r in historical['results'] if r['case']==CASE_NAMES[name]) for name in names}
    report=dict(status='running',scope=__doc__,candidate_audit=audit,memory_limit=a.memory,
                native_sha256=hashlib.sha256(native.read_bytes()).hexdigest(),sql_probe_sha256=hashlib.sha256(exporter.read_bytes()).hexdigest(),
                cache_protocol='fresh native engine per run, sequential rotated modes; OS cache uncontrolled; profiles separate',
                validation='exact semantic fixture equality; full response signatures/schema/count/ns ordering every run',
                selected_modes=modes,selected_cases=names,
                semantic_checks=[],results=[],profiles=[],summaries=[])
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    try:
        with tempfile.TemporaryDirectory(prefix='sage-exp84-') as directory:
            root=Path(directory)
            report['semantic_checks']=validate_semantics(native,exporter,root/'semantics',modes,a.memory);save()
            print('Exact semantic fixtures passed',flush=True)
            export_sql(exporter,root/'sql',[dict(Name=n,Query=QUERIES[CASE_NAMES[n]]) for n in names])
            for name in names:
                statements={};reference=None
                for mode in modes:
                    template=(root/'sql'/(name+'-'+mode+'.sql')).read_text()
                    if template.count(MARKER)!=1:raise ValueError('production scan marker changed')
                    statements[mode]=template.replace(MARKER,scan)
                discovery_sql=(root/'sql'/(name+'-discovery.sql')).read_text().replace(MARKER,scan) if 'uniform' in modes else ''
                for rep in range(a.reps):
                    for mode in rotated_layouts(modes,rep,a.reps):
                        result=measure(native,statements,discovery_sql,a.memory,False,mode,artifacts/name/mode)
                        signature=result['signatures']['full']
                        if reference is None:reference=signature
                        if result['rows']!=counts[name] or signature!=reference:raise ValueError(name+': public response changed in '+mode)
                        report['results'].append(dict(case=name,mode=mode,repetition=rep+1,**result));save()
                        print(name,mode,rep+1,f"first={result['first_batch_seconds']:.3f}s total={result['total_seconds']:.3f}s",flush=True)
                for mode in modes:
                    result=measure(native,statements,discovery_sql,a.memory,True,mode,artifacts/name/mode)
                    if result['signatures']['full']!=reference:raise ValueError('profiled output differs')
                    report['profiles'].append(dict(case=name,mode=mode,**result))
                    runs=[r for r in report['results'] if r['case']==name and r['mode']==mode]
                    summary=dict(case=name,mode=mode,
                        **{key:statistics.median(r[key] for r in runs) for key in ('first_batch_seconds','total_seconds','peak_process_rss_bytes','ipc_bytes')},
                        operator_work=result['profile']['duckdb']['operator_work_seconds'],engine_metrics=result['profile']['duckdb']['metrics'])
                    if mode=='uniform':
                        summary.update(discovery_seconds=statistics.median(r['discovery']['total_seconds'] for r in runs),
                                       main_query_seconds=statistics.median(r['query_total_seconds'] for r in runs),
                                       discovery_operator_work=result['discovery']['profile']['duckdb']['operator_work_seconds'],
                                       discovery_engine_metrics=result['discovery']['profile']['duckdb']['metrics'])
                    report['summaries'].append(summary)
                    save()
        report['status']='complete';save()
    except Exception as exc:
        report['status']='failed';report['error']=str(exc);save();raise


if __name__=='__main__':main()
