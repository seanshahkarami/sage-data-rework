"""Summarize the opt-in, profiled retained-input raw-candidate experiment.

These are native completion/CPU diagnostics, not HTTP latency or staging tests.
"""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import statistics

from exp65_cold_query_profile import load_timing

CASES=tuple(f'series_{period}_{direction}{n}' for period in ('day','hour') for direction in ('head','tail') for n in (1,1000))


def medians(items):
    keys=set.intersection(*(set(x) for x in items))
    return {k:statistics.median(x[k] for x in items) for k in sorted(keys) if all(type(x[k]) in (int,float) for x in items)}


def child(root,relative):
    p=Path(relative)
    if p.is_absolute() or '..' in p.parts or 'node-data' in p.parts:raise ValueError('invalid relative artifact path')
    path=(root/p).resolve()
    if root not in path.parents or 'node-data' in path.parts:raise ValueError('artifact must be inside isolated run')
    return path


def summarize(report_path):
    report_path=report_path.resolve();root=report_path.parent
    if 'node-data' in root.parts:raise ValueError('isolated report required')
    report=json.loads(report_path.read_text())
    if report['status']!='complete' or report['profiled'] is not True:raise ValueError('completed profiled experiment required; inspect preserved failures first')
    reps=report['measured_repetitions']
    if type(reps) is not int or not 1<=reps<=10:raise ValueError('invalid repetition count')
    groups=defaultdict(list);trials=[]
    for trial in report['trials']:
        if trial['case'] not in CASES or trial['plan'] not in ('direct','raw_candidates'):raise ValueError('unexpected case/plan')
        if trial['status']!='complete' or trial['parity'] is not True or trial.get('error'):raise ValueError('failed or unmatched trial')
        if type(trial['repetition']) is not int or type(trial['warmup']) is not bool or trial['warmup']!=(trial['repetition']==0):raise ValueError('invalid warmup marker')
        directory=child(root,trial['directory']);timing=trial['timing']
        if Path(timing).name!=timing:raise ValueError('invalid timing filename')
        sidecar=child(root,str((directory/timing).relative_to(root)))
        data=json.loads(sidecar.read_text());native=data['duckdb_file']
        if Path(native).name!=native:raise ValueError('invalid native filename')
        child(root,str((directory/native).relative_to(root)))
        profile=load_timing(sidecar)
        if not profile['eligible'] or not profile.get('engine_version','').startswith('v2.0.') or profile['rows']!=trial['rows']:raise ValueError('ineligible profile/row count')
        body=child(root,str((directory/'rows.json').relative_to(root)));rows=json.loads(body.read_text())
        if len(rows)!=trial['rows'] or not rows:raise ValueError('missing response rows')
        entry=dict(trial,profile=profile,body_sha256=hashlib.sha256(body.read_bytes()).hexdigest())
        trials.append(entry);groups[trial['case'],trial['plan']].append(entry)
    summaries=[]
    for case in CASES:
        for plan in ('direct','raw_candidates'):
            runs=groups[case,plan]
            if sorted(r['repetition'] for r in runs)!=list(range(reps+1)):raise ValueError('missing/duplicate repetitions')
            measured=[r for r in runs if not r['warmup']]
            summaries.append(dict(case=case,plan=plan,rows=measured[0]['rows'],
                trial_seconds=statistics.median(r['seconds'] for r in measured),
                phases=medians([r['profile']['phases'] for r in measured]),
                engine_metrics=medians([r['profile']['duckdb']['metrics'] for r in measured]),
                operator_work_seconds=medians([r['profile']['duckdb']['operator_work_seconds'] for r in measured])))
    comparisons=[]
    for case in CASES:
        a=next(s for s in summaries if s['case']==case and s['plan']=='direct')
        b=next(s for s in summaries if s['case']==case and s['plan']=='raw_candidates')
        before=a['phases']['total_backend_lifetime_seconds'];after=b['phases']['total_backend_lifetime_seconds']
        comparisons.append(dict(case=case,direct_native_seconds=before,candidate_native_seconds=after,
            direct_over_candidate_ratio=before/after if after else None))
    return dict(status='complete',scope=__doc__,source=str(report_path),source_sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(),
        notes=report['notes'],trials=trials,summaries=summaries,comparisons=comparisons)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--report',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or 'node-data' in output.parts:raise ValueError('new isolated summary output required')
    result=summarize(a.report);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    for item in result['comparisons']:print(item['case'],f"native direct={item['direct_native_seconds']:.3f}s candidate={item['candidate_native_seconds']:.3f}s ratio={item['direct_over_candidate_ratio']:.2f}")

if __name__=='__main__':main()
