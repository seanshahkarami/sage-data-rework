"""Audit historical v2 versus v3 timing scope; optionally measure v3 identically.

No v2 server, storage rewrite or compatibility reader. --measure-v3 uses the
historical Sage-client timer against the current API. Checksums are compared
only after explicitly flooring v3 timestamps to v2's microsecond precision.
"""
import argparse
import json
from pathlib import Path
import statistics
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import exp60_arrow_compression_live as live
from exp59_arrow_ipc_compression import monitored

CASES=[c for c in live.CASES if c[0] in ('cold 1 min, one name','cold 1 hour, one name')]
DAY=('cold 1 day, one name','2025-03-01T00:00:00Z','2025-03-02T00:00:00Z',live.ONE,False,'1800')
FULL_CASES=[c for c in live.CASES if c[0].startswith('cold ')]


def normalized_signature(frame):
    frame=frame.copy()
    frame['timestamp']=frame['timestamp'].dt.floor('us')
    hashes=pd.util.hash_pandas_object(frame,index=False).to_numpy()
    return dict(rows=len(frame),columns=list(frame),dtypes={k:str(v) for k,v in frame.dtypes.items()},
                row_hash_sum=str(int(hashes.sum(dtype=np.uint64))),
                row_hash_xor=str(int(np.bitwise_xor.reduce(hashes,initial=np.uint64(0)))))


def measured(endpoint,case):
    raw,_=monitored([sys.executable,'-I',str(Path(__file__).resolve()),'--worker',
                     '--endpoint',endpoint,'--case-json',json.dumps(case)])
    return json.loads(raw)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint',default='http://127.0.0.1:10000/api/v1/query')
    p.add_argument('--historical',type=Path,default=HERE/'results/exp67_after_live.json')
    p.add_argument('--v3-baseline',type=Path,default=HERE/'results/exp70_v3_live_baseline.json')
    p.add_argument('--measure-v3',action='store_true')
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--include-month',action='store_true',help='only after March/April coverage is published; repeats all historical cold cases and March')
    p.add_argument('--worker',action='store_true')
    p.add_argument('--case-json')
    p.add_argument('--output',type=Path,default=HERE/'results/exp71_v2_v3_cost.json')
    a=p.parse_args()
    if a.worker:
        original=live.client._load_arrow
        frames=[]
        def capture(stream):
            frame=original(stream);frames.append(frame);return frame
        with patch.object(live.client,'_load_arrow',capture):
            run=live.once(a.endpoint,json.loads(a.case_json))
        run['v2_precision_signature']=normalized_signature(frames[0])
        print(json.dumps(run));return
    if a.reps<1:p.error('positive repetitions required')
    old=json.loads(a.historical.read_text());v3=json.loads(a.v3_baseline.read_text())
    report=dict(historical=str(a.historical),v3_response_baseline=str(a.v3_baseline),
        status='historical scopes differ; VARIANT end-to-end overhead is not established',
        reasons=['v2 exp67 includes Sage-client dataframe decoding; v3 exp70 stops at response collection',
                 'v2 warehouse covers March/April, v3 warehouse March 1 only',
                 'timestamps, file layout, row-group sizes and cache history changed',
                 'no matching historical live one-day measurement or latest NDJSON baseline'],
        historical_context=[],current_client_results=[],comparable_timer_summaries=[])
    for case,name in zip(CASES,('minute','hour')):
        prior=next(s for s in old['summaries'] if s['query']==case[0])
        current=next(s for s in v3['summaries'] if s['query']==name and s['format']=='arrow')
        report['historical_context'].append(dict(query=name,rows=prior['rows'],
            v2_client_total_seconds=prior['total_seconds'],v2_first_batch_seconds=prior['first_batch_seconds'],
            v3_response_total_seconds=current['total_seconds'],v3_first_batch_seconds=current['first_batch_seconds'],
            total_timers_comparable=False))
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True)
        a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    if a.measure_v3:
        cases=FULL_CASES+[DAY] if a.include_month else CASES+[DAY]
        for case in cases:
            if not case[4]:measured(a.endpoint,case) # exp64 warms short cases, not month cases
            runs=[]
            prior=[r for r in old['results'] if r['query']==case[0]]
            for rep in range(a.reps):
                run=measured(a.endpoint,case)
                if run['mode']!='zstd':raise AssertionError('requires historical Zstd codec')
                if prior:
                    for previous in prior:
                        if run['v2_precision_signature']!={k:previous[k] for k in run['v2_precision_signature']}:
                            raise AssertionError('values/meta/schema differ beyond timestamp precision')
                runs.append(run);report['current_client_results'].append(dict(repetition=rep+1,**run));save()
            total=statistics.median(r['total_seconds'] for r in runs)
            summary=dict(query=case[0],rows=runs[0]['rows'],v3_client_total_seconds=total,
                         v3_first_batch_seconds=statistics.median(r['first_batch_seconds'] for r in runs),
                         timer_matches_historical=True,historical_available=bool(prior))
            if prior:
                baseline=statistics.median(r['total_seconds'] for r in prior)
                summary.update(v2_client_total_seconds=baseline,observed_migration_change_percent=100*(total/baseline-1),
                               values_meta_match_after_explicit_timestamp_floor=True)
            report['comparable_timer_summaries'].append(summary);save()
        report['status']='same-timer historical migration comparison; still not isolated VARIANT overhead'
        save()
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
