"""Diagnostic value/metadata projection ablations on the adopted local files.

No public API change. Response ablations also change scan pruning, sort payload
and IPC sizes. Checksum ablations hash all raw fields in every mode and emit
one aggregate row without sorting: JSON formation plus hashing, not pure JSON
CPU. These local runs exclude Iceberg planning, Pelican and client DataFrames.
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

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp59_arrow_ipc_compression import monitored
from exp65_cold_query_profile import load_timing
from exp66_duckdb20_evaluation import validate_version
from exp73_plugin_live_compare import public_schema,validate_candidate_audit,full_warehouse_cases
from exp74_layout_scan_profiles import rotated_layouts

MODES=('full','without_meta','without_value','without_both')
NAMES={'day':'2025-03-01_series_day','march':'2025-03_series_month','april':'2025-04_series_month'}
QUERIES=dict(full_warehouse_cases('unused'))
MARKER="iceberg_scan('__SAGE_METADATA__')"


def scan_from_audit(audit):
    validate_candidate_audit(audit,full_warehouse=True)
    if audit.get('batch_order')!='time' or audit.get('row_group_row_limit')!=262144 or audit['rows']!=8_457_186_954 or audit['snapshots']!=61:
        raise ValueError('requires the complete adopted chronological/262144 warehouse')
    files=[Path(item['path']).resolve() for item in audit['file_time_ranges']]
    if not files or len(set(files))!=len(files):raise ValueError('distinct audited files required')
    if any('node-data' in path.parts or not path.is_file() for path in files):raise ValueError('isolated existing local files required')
    if sum(path.stat().st_size for path in files)!=audit['bytes']:raise ValueError('local byte inventory differs from audit')
    quote=lambda path:"'"+str(path).replace("'","''")+"'"
    return 'read_parquet(['+','.join(quote(path) for path in files)+'],hive_partitioning=false)'


def response_digests(path,modes):
    out={mode:dict(rows=0,row_hash_sum=0,row_hash_xor=0) for mode in modes};last=None
    with path.open('rb') as stream,ipc.open_stream(stream) as reader:
        public_schema(reader.schema)
        for batch in reader:
            times=batch.column(0).cast(pa.int64()).to_numpy()
            if len(times) and (np.any(times[1:]<times[:-1]) or (last is not None and times[0]<last)):
                raise ValueError('timestamps out of order')
            if len(times):last=times[-1]
            for mode in modes:
                masked=batch
                for index,literal,omit in ((2,'null',mode in ('without_value','without_both')),(3,'{}',mode in ('without_meta','without_both'))):
                    if omit:masked=masked.set_column(index,batch.schema.field(index),pa.array([literal]*len(batch),type=pa.string()))
                hashes=pd.util.hash_pandas_object(masked.to_pandas(),index=False).to_numpy()
                result=out[mode];result['rows']+=len(batch)
                result['row_hash_sum']=(result['row_hash_sum']+int(hashes.sum(dtype=np.uint64)))%(1<<64)
                result['row_hash_xor']^=int(np.bitwise_xor.reduce(hashes,initial=np.uint64(0)))
    for result in out.values():
        for key in ('row_hash_sum','row_hash_xor'):result[key]=str(result[key])
    return out


def validate_cost(payload,reference,mode,rows):
    keys=('rows','source_hash_sum','source_hash_xor')
    if payload.get('rows')!=rows or any(payload.get(k)!=reference.get(k) for k in keys):
        raise ValueError('checksum diagnostic changed source rows/fields')
    for key,omitted in [('value_hash_sum',mode in ('without_value','without_both')),('meta_hash_sum',mode in ('without_meta','without_both'))]:
        if key not in payload or (not omitted and payload[key]!=reference[key]):
            raise ValueError('retained serialized-field checksum differs: '+key)


def run(binary,sql,memory,profiled,family,mode,make_masks,archive):
    with tempfile.TemporaryDirectory(prefix='sage-exp83-run-') as directory:
        root=Path(directory);statement=root/'query.sql';statement.write_text(sql)
        _,peak=monitored([str(binary),'-test.run=^TestColdDuckDBEvaluationProbe$','-test.count=1','-test.timeout=2h'],
            dict(os.environ,COLD_EVAL_DIR=str(root),COLD_EVAL_SQL=str(statement),COLD_EVAL_MEMORY=memory,
                 COLD_EVAL_ASYNC_THREADS='8',COLD_EVAL_PROFILE='1' if profiled else '0'))
        report=json.loads((root/'run.json').read_text());validate_version('2.0.0',report)
        report.update(peak_process_rss_bytes=peak,ipc_bytes=(root/'body.arrows').stat().st_size,
                      sql_sha256=hashlib.sha256(sql.encode()).hexdigest())
        if family=='response':
            report['signatures']=response_digests(root/'body.arrows',MODES if make_masks else ('full',))
        else:
            if report['rows']!=1:raise ValueError('checksum diagnostic must return one row')
            with (root/'body.arrows').open('rb') as stream,ipc.open_stream(stream) as reader:
                public_schema(reader.schema);table=reader.read_all()
            if len(table)!=1 or report['rows']!=1:raise ValueError('checksum diagnostic must return one row')
            report['cost_checksums']=json.loads(table['value'][0].as_py())
        if profiled:
            timings=list(root.glob('*.timing.json'))
            if len(timings)!=1:raise ValueError('missing paired native timing')
            timing=load_timing(timings[0])
            if not timing['eligible']:raise ValueError('incomplete native profile')
            timing['source']=str(archive/timings[0].name)
            report['profile']=timing
            archive.mkdir(parents=True)
            for path in root.glob('*.json'):shutil.copyfile(path,archive/path.name)
            shutil.copyfile(statement,archive/'query.sql')
        return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--native-probe',type=Path,required=True)
    p.add_argument('--sql-probe',type=Path,required=True)
    p.add_argument('--candidate-audit',type=Path,required=True)
    p.add_argument('--case',action='append',choices=tuple(NAMES))
    p.add_argument('--family',choices=('response','checksum','both','cardinality'),default='both')
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--memory',default='3GB')
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();native=a.native_probe.resolve();exporter=a.sql_probe.resolve()
    artifacts=a.output.with_suffix('')
    if a.reps<1 or not native.is_file() or not exporter.is_file():p.error('existing probes and positive repetitions required')
    if a.output.exists() or artifacts.exists() or 'node-data' in a.output.resolve().parts:p.error('fresh isolated output/artifact paths required')
    audit=json.loads(a.candidate_audit.read_text());scan=scan_from_audit(audit)
    names=list(dict.fromkeys(a.case or NAMES));families=['response','checksum'] if a.family=='both' else ([] if a.family=='cardinality' else [a.family])
    historical=json.loads((HERE/'results/full-layout-1792962354/http-arrow.json').read_text())
    counts={name:next(r['rows'] for r in historical['results'] if r['case']==NAMES[name]) for name in names}
    report=dict(status='running',scope=__doc__,memory_limit=a.memory,candidate_audit=audit,
                native_sha256=hashlib.sha256(native.read_bytes()).hexdigest(),sql_probe_sha256=hashlib.sha256(exporter.read_bytes()).hexdigest(),
                primary_timings='unprofiled, fresh native engine per run; native initialization and checksum validation excluded',
                cache_protocol='sequential rotated modes; OS cache uncontrolled; profiles separate from timing repetitions',
                results=[],profiles=[],summaries=[],metadata_cardinality=[])
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    try:
        with tempfile.TemporaryDirectory(prefix='sage-exp83-sql-') as directory:
            root=Path(directory);queries=root/'queries.json'
            queries.write_text(json.dumps([dict(Name=name,Query=QUERIES[NAMES[name]]) for name in names]))
            monitored([str(exporter),'-test.run=^TestColdProjectionProbe$','-test.count=1'],
                      dict(os.environ,COLD_PROJECTION_PROBE_DIR=str(root),COLD_PROJECTION_QUERIES=str(queries)))
            for name in names:
                if a.family=='cardinality':
                    template=(root/(name+'-cardinality.sql')).read_text()
                    if template.count(MARKER)!=1:raise ValueError('cardinality scan marker changed')
                    result=run(native,template.replace(MARKER,scan),a.memory,False,'checksum','cardinality',False,artifacts/name)
                    payload=result['cost_checksums']
                    if payload.get('rows')!=counts[name] or not 0<payload.get('metadata_sets',0)<=counts[name]:
                        raise ValueError('invalid metadata cardinality/source count')
                    report['metadata_cardinality'].append(dict(case=name,**result));save()
                    print(name,'metadata_sets',payload['metadata_sets'],'rows',payload['rows'],flush=True)
                for family in families:
                    expected=None;cost_reference=None
                    sqls={}
                    for mode in MODES:
                        suffix=('optimized' if mode=='full' else mode) if family=='response' else 'cost_'+mode
                        template=(root/(name+'-'+suffix+'.sql')).read_text()
                        if template.count(MARKER)!=1:raise ValueError('SQL exporter scan marker changed')
                        sqls[mode]=template.replace(MARKER,scan)
                    for rep in range(a.reps):
                        for mode in rotated_layouts(list(MODES),rep,a.reps):
                            result=run(native,sqls[mode],a.memory,False,family,mode,expected is None,
                                       artifacts/name/family/mode)
                            if family=='response':
                                if expected is None:
                                    if mode!='full':raise ValueError('full response must run first')
                                    expected=result['signatures']
                                if result['rows']!=counts[name] or result['signatures']['full']!=expected[mode]:
                                    raise ValueError(name+': retained fields differ in '+mode)
                            else:
                                if cost_reference is None:cost_reference=result['cost_checksums']
                                validate_cost(result['cost_checksums'],cost_reference,mode,counts[name])
                            report['results'].append(dict(case=name,family=family,mode=mode,repetition=rep+1,**result));save()
                            print(name,family,mode,rep+1,f"first={result['first_batch_seconds']:.3f}s total={result['total_seconds']:.3f}s",flush=True)
                    for mode in MODES:
                        result=run(native,sqls[mode],a.memory,True,family,mode,False,artifacts/name/family/mode)
                        if family=='response' and result['signatures']['full']!=expected[mode]:raise ValueError('profiled retained fields differ')
                        if family=='checksum':validate_cost(result['cost_checksums'],cost_reference,mode,counts[name])
                        report['profiles'].append(dict(case=name,family=family,mode=mode,**result))
                        runs=[r for r in report['results'] if r['case']==name and r['family']==family and r['mode']==mode]
                        report['summaries'].append(dict(case=name,family=family,mode=mode,
                            **{key:statistics.median(r[key] for r in runs) for key in ('first_batch_seconds','total_seconds','peak_process_rss_bytes','ipc_bytes')},
                            operator_work=result['profile']['duckdb']['operator_work_seconds'],engine_metrics=result['profile']['duckdb']['metrics']))
                        save()
        report['status']='complete';save()
    except Exception as exc:
        report['status']='failed';report['error']=str(exc);save();raise


if __name__=='__main__':main()
