"""Compare actual published day-local files on native DuckDB 2.0, read-only.

Local scan/geometry diagnostic, excluding Iceberg planning, HTTP and Pelican.
Export current API SQL with the Go probe; validate every response signature.
Primary timings are unprofiled; separate profiles expose operator/scan costs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp66_duckdb20_evaluation import run_probe,signature
from exp67_metadata_projection import MARKER
from exp72_plugin_layout import scan
from exp73_plugin_live_compare import cases

DAY_LOW=1740787200000000000
DAY_HIGH=1740873600000000000
DEFAULT_CASES=('all_plugins_vsn_minute','fleet_minute','fleet_hour','series_day')
CASE_NAMES=(*DEFAULT_CASES,'plugin_hour','plugin_vsn_day','series_minute','series_hour','all_plugins_vsn_hour')


def query_cases(plugin):
    selected=dict(cases(plugin))
    selected['plugin_hour']=dict(start='2025-03-01T00:00:00+00:00',end='2025-03-01T01:00:00+00:00',filter={'plugin':plugin})
    return selected


def rotated_layouts(labels,repetition,repetitions):
    # With nine layouts and three repetitions, offsets 0/3/6 give each
    # candidate early, middle and late positions instead of shifting by one.
    offset=(repetition*len(labels)//repetitions) if repetitions<len(labels) else repetition%len(labels)
    return labels[offset:]+labels[:offset]


def time_bounds(group):
    column=next(c for c in (group.column(i) for i in range(group.num_columns)) if c.path_in_schema=='time')
    stats=column.statistics
    if not stats or not stats.has_min_max:raise ValueError('missing time statistics')
    # Use physical ns, avoiding datetime/pandas conversion or unit inference.
    return int(stats.min_raw),int(stats.max_raw)


def inventory(directory):
    files=[];rows=size=groups=0
    for path in sorted(Path(directory).rglob('*.parquet')):
        parquet=pq.ParquetFile(path)
        field=parquet.schema_arrow.field('time')
        if not pa.types.is_timestamp(field.type) or field.type.unit!='ns':raise ValueError('requires nanosecond time')
        bounds=[time_bounds(parquet.metadata.row_group(i)) for i in range(parquet.metadata.num_row_groups)]
        if any(hi>=DAY_LOW and lo<DAY_HIGH for lo,hi in bounds):
            if any(lo<DAY_LOW or hi>=DAY_HIGH for lo,hi in bounds):raise ValueError('file crosses March 1 boundary')
            files.append(path);rows+=parquet.metadata.num_rows;size+=path.stat().st_size;groups+=len(bounds)
    if not files:raise ValueError('no complete March 1 files')
    return files,dict(files=len(files),rows=rows,bytes=size,row_groups=groups)


def geometry(files,query):
    from exp70_v3_live_baseline import timestamp_ns
    low,high=timestamp_ns(query['start']),timestamp_ns(query['end'])
    kept=rows=size=0
    for path in files:
        parquet=pq.ParquetFile(path)
        for i in range(parquet.metadata.num_row_groups):
            group=parquet.metadata.row_group(i);lo,hi=time_bounds(group)
            if hi<low or lo>=high:continue
            stats={group.column(j).path_in_schema:group.column(j).statistics for j in range(group.num_columns)}
            if any(s and (s.null_count==group.num_rows or (s.has_min_max and not s.min<=value<=s.max))
                   for key,value in query['filter'].items() for s in [stats.get(key)]):continue
            kept+=1;rows+=group.num_rows
            size+=sum(group.column(j).total_compressed_size for j in range(group.num_columns))
    return dict(groups_not_excluded_by_footer=kept,rows_in_those_groups=rows,
                all_column_compressed_bytes_in_those_groups=size,
                note='footer eligibility upper bound; not actual engine reads or HTTP bytes')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dataset',action='append',required=True,help='repeat LABEL=local/table/data/directory')
    p.add_argument('--native-probe',type=Path,required=True)
    p.add_argument('--sql-probe',type=Path,required=True)
    p.add_argument('--case',action='append',choices=CASE_NAMES)
    p.add_argument('--plugin',help='plugin filter; default is the recorded exp73 selected plugin')
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--memory',default='3GB')
    p.add_argument('--output',type=Path,default=HERE/'results/exp74_layout_scan_profiles.json')
    a=p.parse_args();sources={};inventories={}
    for arg in a.dataset:
        label,sep,directory=arg.partition('=')
        if not sep or not label or label in sources:p.error('unique LABEL=directory required')
        sources[label],inventories[label]=inventory(directory)
    if len(sources)<2 or a.reps<1:p.error('at least two datasets and positive repetitions required')
    if len({v['rows'] for v in inventories.values()})!=1:p.error('March 1 row counts differ')
    plugin=a.plugin
    if plugin is None and any(name in ('plugin_hour','plugin_vsn_day') for name in (a.case or DEFAULT_CASES)):
        plugin=json.loads((HERE/'results/exp73_plugin_live_compare.json').read_text())['selected_plugin']
    selected=[(name,q) for name,q in query_cases(plugin if plugin is not None else 'unused').items() if name in (a.case or DEFAULT_CASES)]
    report=dict(scope=__doc__,status='running',memory=a.memory,selected_day='2025-03-01',
                inventories=inventories,selected_plugin=plugin,queries=dict(selected),results=[],profiles=[],summaries=[],
                native_sha256=hashlib.sha256(a.native_probe.read_bytes()).hexdigest(),
                sql_sha256=hashlib.sha256(a.sql_probe.read_bytes()).hexdigest(),
                cache_protocol='fresh engine per run; spread rotations over layout order when repetitions < layouts; OS caches uncontrolled; profiles separate from primary timings')
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    with tempfile.TemporaryDirectory(prefix='sage-layout-profiles-') as directory:
        root=Path(directory);queries=root/'queries.json'
        queries.write_text(json.dumps([dict(Name=n,Query=q) for n,q in selected]))
        monitored([str(a.sql_probe.resolve()),'-test.run=^TestColdProjectionProbe$','-test.count=1'],
                  dict(os.environ,COLD_PROJECTION_PROBE_DIR=str(root),COLD_PROJECTION_QUERIES=str(queries)))
        labels=list(sources)
        for name,query in selected:
            template=(root/(name+'-optimized.sql')).read_text();reference=None
            if template.count(MARKER)!=1:raise ValueError('SQL exporter no longer matches the v3 scan marker')
            for rep in range(a.reps):
                for label in rotated_layouts(labels,rep,a.reps):
                    result=run_probe(a.native_probe.resolve(),'2.0.0',template.replace(MARKER,scan(sources[label])),a.memory,8,False)
                    if reference is None:reference=signature(result)
                    if signature(result)!=reference:raise ValueError(name+': response schema/content/order changed')
                    report['results'].append(dict(case=name,layout=label,repetition=rep+1,**result));save()
                    print(name,label,f"{result['total_seconds']:.3f}s",flush=True)
            for label,files in sources.items():
                result=run_probe(a.native_probe.resolve(),'2.0.0',template.replace(MARKER,scan(files)),a.memory,8,True)
                if signature(result)!=reference:raise ValueError(name+': profiled response differs')
                report['profiles'].append(dict(case=name,layout=label,**result))
                runs=[r for r in report['results'] if r['case']==name and r['layout']==label]
                report['summaries'].append(dict(case=name,layout=label,rows=reference['rows'],
                    total_seconds=statistics.median(r['total_seconds'] for r in runs),
                    first_batch_seconds=statistics.median(r['first_batch_seconds'] for r in runs),
                    **geometry(files,query)))
                save()
    report['status']='complete';save();print('Report:',a.output)


if __name__=='__main__':main()
