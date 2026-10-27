"""Live time-first versus plugin/vsn/time v3 trial, without server mutations.

HTTP bodies spool to isolated temporary files; semantic validation happens
outside the receive timer and uses bounded batches/lines. Arrow and NDJSON
retain nanosecond timestamps and the public schema. Compare layouts within a
format; this is not the exp71 Sage-client dataframe timer. No Docker operations,
catalog commits, storage PUTs or request-based profiling/compression overrides.
"""
import argparse
import hashlib
import json
from functools import lru_cache
from pathlib import Path
import statistics
import sys
import tempfile
import time
from urllib.request import Request,urlopen

import pyarrow as pa
import pyarrow.ipc as ipc

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp70_v3_live_baseline import timestamp_ns


class SpoolingReader:
    def __init__(self,response,output):
        self.response=response;self.output=output;self.n=0
    def read(self,n=-1):
        raw=self.response.read(n);self.output.write(raw);self.n+=len(raw);return raw
    def readable(self):return True
    @property
    def closed(self):return False


def public_schema(schema):
    expected=pa.schema([pa.field('timestamp',pa.timestamp('ns')),pa.field('name',pa.string()),
                        pa.field('value',pa.string()),pa.field('meta',pa.string())])
    if not schema.equals(expected,check_metadata=False):
        raise ValueError('API did not return the public four-field Arrow schema: '+str(schema))


def receive(endpoint,query,fmt,path,timeout,opener=urlopen):
    accept='application/vnd.apache.arrow.stream' if fmt=='arrow' else 'application/x-ndjson'
    request=Request(endpoint,json.dumps(query).encode(),{'Content-Type':'application/json',
                    'Accept':accept,'Accept-Encoding':'identity'})
    begin=time.perf_counter();cpu=time.process_time();first=None;rows=0;size=0
    with opener(request,timeout=timeout) as response,path.open('wb') as output:
        if response.headers.get('Content-Encoding','identity')!='identity':raise ValueError('unexpected outer compression')
        codec=response.headers.get('X-Arrow-Compression')
        if fmt=='arrow':
            if codec!='zstd':raise ValueError('trial requires the baseline Zstd IPC codec')
            if response.headers.get('Content-Type','').split(';')[0]!='application/vnd.apache.arrow.stream':raise ValueError('not an Arrow response')
            stream=SpoolingReader(response,output)
            with ipc.open_stream(stream) as reader:
                public_schema(reader.schema)
                for batch in reader:
                    if first is None:first=time.perf_counter()-begin
                    rows+=batch.num_rows
            while stream.read(1024*1024):pass
            size=stream.n
        else:
            # readline records the true first complete line, rather than the
            # old exp70 metric which waited for a 1 MiB buffered read.
            line=response.readline()
            if line:
                if not line.endswith(b'\n'):raise ValueError('incomplete first NDJSON record')
                first=time.perf_counter()-begin
                output.write(line);size+=len(line);rows=1
            last=line[-1:] if line else b''
            while True:
                raw=response.read(1024*1024)
                if not raw:break
                rows+=raw.count(b'\n');size+=len(raw);last=raw[-1:];output.write(raw)
            if last and last!=b'\n':raise ValueError('incomplete NDJSON response')
    return dict(rows=rows,bytes=size,total_seconds=time.perf_counter()-begin,
                first_result_seconds=first,client_cpu_seconds=time.process_time()-cpu,codec=codec)


@lru_cache(maxsize=4096)
def canonical_json(text):
    return json.dumps(json.loads(text),sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


class RowDigest:
    def __init__(self):
        self.rows=0;self.summed=0;self.xor=0;self.last=None;self.ordered=True
    def add(self,stamp,name,value,meta,strict_time_order=True):
        if self.last is not None and stamp<self.last:
            if strict_time_order:raise ValueError('timestamp ordering changed')
            self.ordered=False
        self.last=stamp
        # Prefix each component via JSON string escaping, so boundaries are
        # unambiguous. Keep scalar numeric representations and int precision.
        canonical=json.dumps([stamp,name,value,meta],ensure_ascii=False,separators=(',',':')).encode()
        h=int.from_bytes(hashlib.blake2b(canonical,digest_size=16).digest(),'little')
        self.rows+=1;self.summed=(self.summed+h)%(1<<128);self.xor^=h
    def result(self):
        return dict(rows=self.rows,row_hash_sum=str(self.summed),row_hash_xor=str(self.xor),timestamps_ordered=self.ordered)


def validate_body(path,fmt,strict_time_order=True):
    digest=RowDigest()
    if fmt=='arrow':
        with path.open('rb') as stream,ipc.open_stream(stream) as reader:
            public_schema(reader.schema)
            for batch in reader:
                columns=[batch.column(0).cast(pa.int64()).to_pylist()]+[batch.column(i).to_pylist() for i in range(1,4)]
                for stamp,name,value,meta in zip(*columns):
                    if stamp is None or meta is None:raise ValueError('null timestamp or metadata')
                    digest.add(stamp,name,'null' if value is None else canonical_json(value),canonical_json(meta),strict_time_order)
    else:
        with path.open('rb') as stream:
            for line in stream:
                row=json.loads(line)
                if set(row)!=set(('timestamp','name','value','meta')):raise ValueError('public NDJSON fields changed')
                value=json.dumps(row['value'],sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
                meta=json.dumps(row['meta'],sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
                digest.add(timestamp_ns(row['timestamp']),row['name'],value,meta,strict_time_order)
    return digest.result()


def cases(plugin):
    definitions=[('series_minute',60,{'name':'wxt.env.temp','vsn':'W0A0'}),
                 ('series_hour',3600,{'name':'wxt.env.temp','vsn':'W0A0'}),
                 ('series_day',86400,{'name':'wxt.env.temp','vsn':'W0A0'}),
                 ('plugin_vsn_day',86400,{'plugin':plugin,'vsn':'W0A0'}),
                 ('all_plugins_vsn_minute',60,{'vsn':'W0A0'}),
                 ('all_plugins_vsn_hour',3600,{'vsn':'W0A0'}),('fleet_minute',60,{}),('fleet_hour',3600,{})]
    from datetime import datetime,timedelta,timezone
    start=datetime(2025,3,1,tzinfo=timezone.utc)
    return [(name,dict(start=start.isoformat(),end=(start+timedelta(seconds=seconds)).isoformat(),filter=filters))
            for name,seconds,filters in definitions]


def full_warehouse_cases(plugin):
    """Sample both months and their boundary without returning the whole fleet month."""
    from datetime import datetime,timedelta,timezone
    selected=[]
    for day in ('2025-03-01','2025-03-31','2025-04-01','2025-04-30'):
        start=datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        for name,query in cases(plugin):
            duration=datetime.fromisoformat(query['end'])-datetime.fromisoformat(query['start'])
            selected.append((day+'_'+name,dict(query,start=start.isoformat(),end=(start+duration).isoformat())))
        selected.append((day+'_plugin_hour',dict(start=start.isoformat(),end=(start+timedelta(hours=1)).isoformat(),filter={'plugin':plugin})))
    for month,end in (('2025-03-01','2025-04-01'),('2025-04-01','2025-05-01')):
        selected.append((month[:7]+'_series_month',dict(start=month+'T00:00:00+00:00',end=end+'T00:00:00+00:00',filter={'name':'wxt.env.temp','vsn':'W0A0'})))
    selected.append(('month_boundary_fleet_hour',dict(start='2025-03-31T23:30:00+00:00',end='2025-04-01T00:30:00+00:00',filter={})))
    return selected


def validate_candidate_audit(audit,full_warehouse=False):
    if audit.get('layout')!='plugin-vsn-time' or audit.get('mixed_plugin_groups')!=0 or not audit.get('ordering_checked') or audit.get('value_field_id')!=11 or audit.get('value_type')!='variant':
        raise ValueError('candidate must pass the committed v3 plugin layout audit')
    if audit.get('file_row_limit')!=10_000_000:
        raise ValueError('candidate must use fixed 10-million-row file batches')
    if audit.get('row_group_row_limit') is not None:
        if not audit.get('bounded_plugin_groups') or not audit.get('plugin_pure_groups') or not 100000<=audit['row_group_row_limit']<=1000000:
            raise ValueError('bounded candidate must pass plugin-purity and row-group-cap checks')
        if not 0<audit.get('max_row_group_rows',0)<=audit['row_group_row_limit']:
            raise ValueError('bounded candidate must report an observed maximum within its cap')
    elif not audit.get('one_group_per_plugin'):
        raise ValueError('uncapped control must pass its recorded grouping audit')
    if audit.get('batch_order')=='time' and not audit.get('time_batch_ranges_checked'):
        raise ValueError('time-ordered batches require checked non-overlapping file ranges')
    high=1746057600000000000 if full_warehouse else 1740873600000000000
    if audit.get('min_timestamp_ns') is None or audit.get('max_timestamp_ns') is None or not (
        1740787200000000000<=audit['min_timestamp_ns']<=audit['max_timestamp_ns']<high):
        if full_warehouse:raise ValueError('full trial requires March/April-only coverage')
        raise ValueError('this trial requires a March-1-only candidate')


def full_warehouse_coverage(baseline,candidate):
    """Require the retained policy and matching per-day inventories, not just extents."""
    matched_coverage(baseline,candidate)
    if candidate.get('batch_order')!='time' or candidate.get('row_group_row_limit')!=262144 or not candidate.get('bounded_plugin_groups'):
        raise ValueError('full trial requires chronological batches and the retained 262144-row cap')
    if baseline['rows']!=8_457_186_954 or baseline['snapshots']!=61:
        raise ValueError('full trial requires the recorded 61-day March/April warehouse')
    day_ns=86_400_000_000_000
    def inventory(audit):
        totals={}
        for item in audit.get('file_time_ranges',[]):
            low,high=item['min_timestamp_ns'],item['max_timestamp_ns']
            if low is None or high is None or low>high or low//day_ns!=high//day_ns or item['rows']<=0:
                raise ValueError('full coverage inventory requires positive single-day files')
            day=low//day_ns;totals[day]=totals.get(day,0)+item['rows']
        if set(totals)!=set(range(20148,20209)) or sum(totals.values())!=audit['rows']:
            raise ValueError('full coverage inventory must contain all 61 days and all rows')
        return totals
    left,right=inventory(baseline),inventory(candidate)
    if left!=right:raise ValueError('baseline/candidate daily row counts differ')
    return dict(days=61,rows=baseline['rows'],daily_rows={str(k):v for k,v in sorted(left.items())})


def matched_coverage(baseline,candidate):
    if baseline.get('layout')!='time' or baseline.get('value_type')!='variant' or baseline.get('value_field_id')!=11:
        raise ValueError('baseline must be a time-first VARIANT ID-11 warehouse')
    keys=('rows','min_timestamp_ns','max_timestamp_ns','snapshots')
    if any(baseline.get(k) is None or baseline.get(k)!=candidate.get(k) for k in keys):
        raise ValueError('baseline/candidate must match row count, timestamp extent, and snapshot count')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline',default='http://127.0.0.1:10000/api/v1/query')
    p.add_argument('--candidate',default='http://127.0.0.1:10001/api/v1/query')
    p.add_argument('--plugin')
    p.add_argument('--candidate-audit',type=Path,required=True)
    p.add_argument('--baseline-audit',type=Path)
    p.add_argument('--require-matched-coverage',action='store_true')
    p.add_argument('--full-warehouse',action='store_true',help='strict March/April coverage gate and 39-query suite; requires --baseline-audit')
    p.add_argument('--case',action='append',choices=[name for name,_ in cases('unused')+full_warehouse_cases('unused')])
    p.add_argument('--format',action='append',choices=('arrow','ndjson'))
    p.add_argument('--api-image',default='',help='record the exact shared Docker image ID used for both APIs')
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--timeout',type=int,default=1800)
    p.add_argument('--match',default='')
    p.add_argument('--validate',choices=('first','all'),default='first')
    p.add_argument('--output',type=Path,default=HERE/'results/exp73_plugin_live_compare.json')
    a=p.parse_args()
    if a.reps<1 or a.timeout<1 or a.baseline==a.candidate:p.error('distinct endpoints and positive repetitions/timeout required')
    audit=json.loads(a.candidate_audit.read_text())
    try:validate_candidate_audit(audit,a.full_warehouse)
    except ValueError as exc:p.error(str(exc))
    baseline_audit=json.loads(a.baseline_audit.read_text()) if a.baseline_audit else None
    coverage=None
    if a.require_matched_coverage or a.full_warehouse:
        if baseline_audit is None:p.error('--require-matched-coverage requires --baseline-audit')
        try:
            matched_coverage(baseline_audit,audit)
            if a.full_warehouse:coverage=full_warehouse_coverage(baseline_audit,audit)
        except ValueError as exc:p.error(str(exc))
    plugin=a.plugin
    if plugin is None:plugin=json.loads((HERE/'results/exp72_plugin_layout.json').read_text())['selected_plugin']
    selected=[c for c in (full_warehouse_cases(plugin) if a.full_warehouse else cases(plugin)) if a.match in c[0] and (not a.case or c[0] in a.case)]
    if not selected:p.error('no matching cases')
    endpoints={'time':a.baseline,'plugin_vsn_time':a.candidate}
    report=dict(scope='live HTTP receive + Arrow IPC iteration + file spool; excludes canonical validation and dataframe decoding',
                status='running',endpoints=endpoints,declared_api_image=a.api_image,candidate_audit=audit,baseline_audit=baseline_audit,
                matched_coverage_required=a.require_matched_coverage or a.full_warehouse,full_warehouse_coverage=coverage,
                selected_cases=[name for name,_ in selected],candidate_batch_order=audit.get('batch_order','archive'),selected_plugin=plugin,
                timestamp_precision='nanoseconds, no flooring',validation=a.validate,
                cache_protocol='one excluded warmup per endpoint/format/case; rotate measured endpoint/format order; sequential requests',
                results=[],summaries=[])
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    with tempfile.TemporaryDirectory(prefix='sage-plugin-live-') as directory:
        body=Path(directory)/'body'
        jobs=[(layout,fmt) for fmt in dict.fromkeys(a.format or ['arrow','ndjson']) for layout in ('time','plugin_vsn_time')]
        for name,query in selected:
            for layout,fmt in jobs:receive(endpoints[layout],query,fmt,body,a.timeout)
            reference=None;checked=set()
            for rep in range(a.reps):
                order=jobs[rep%len(jobs):]+jobs[:rep%len(jobs)]
                for layout,fmt in order:
                    run=receive(endpoints[layout],query,fmt,body,a.timeout)
                    if run['rows']==0:raise ValueError('empty trial response: check warehouse coverage')
                    if a.validate=='all' or (layout,fmt) not in checked:
                        digest=validate_body(body,fmt)
                        if digest['rows']!=run['rows']:raise ValueError('body count differs from decoded rows')
                        if reference is None:reference=digest
                        if digest!=reference:raise ValueError(name+': time/plugin/format values or metadata differ')
                        checked.add((layout,fmt));run['validation']=digest
                    report['results'].append(dict(case=name,query=query,layout=layout,format=fmt,repetition=rep+1,**run));save()
                    print(name,layout,fmt,f"rows={run['rows']} first={run['first_result_seconds']:.3f}s total={run['total_seconds']:.3f}s",flush=True)
            for layout,fmt in jobs:
                runs=[r for r in report['results'] if r['case']==name and r['layout']==layout and r['format']==fmt]
                if len({r['rows'] for r in runs})!=1:raise ValueError('row count changed across repetitions')
                report['summaries'].append(dict(case=name,layout=layout,format=fmt,rows=runs[0]['rows'],
                    total_seconds=statistics.median(r['total_seconds'] for r in runs),
                    first_result_seconds=statistics.median(r['first_result_seconds'] for r in runs),
                    bytes=statistics.median(r['bytes'] for r in runs),
                    client_cpu_seconds=statistics.median(r['client_cpu_seconds'] for r in runs),
                    content_checked_against_both_layouts_and_formats=True))
            save()
    report['status']='complete';save();print('Report:',a.output)


if __name__=='__main__':main()
