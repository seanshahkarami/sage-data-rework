"""Pair Arrow requests with fresh profiles from two dedicated sidecar APIs.

Read-only HTTP queries and local log reads. Does not configure/restart APIs.
Both servers must already have COLD_PROFILE_DIR enabled with separate mounted
profile directories. Timings are diagnostic (profiling on), not primary exp73.
"""
import argparse
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp65_cold_query_profile import load_timing,NOTES
from exp73_plugin_live_compare import receive,validate_body,cases,validate_candidate_audit,matched_coverage

DEFAULT_CASES=('all_plugins_vsn_minute','fleet_minute','fleet_hour')


def profile_paths(directory):return set(directory.rglob('*.timing.json'))


def fresh_profile(directory,before,timeout=10):
    deadline=time.monotonic()+timeout
    while True:
        found=profile_paths(directory)-before
        if len(found)>1:raise ValueError('multiple new profiles: use an isolated API with no concurrent requests')
        if found:
            try:
                result=load_timing(next(iter(found)))
                if result.get('eligible'):return result
            except (OSError,ValueError,KeyError):pass
        if time.monotonic()>=deadline:raise ValueError('no fresh complete Arrow timing/native profile pair')
        time.sleep(.05)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline',required=True)
    p.add_argument('--candidate',required=True)
    p.add_argument('--baseline-profiles',type=Path,required=True)
    p.add_argument('--candidate-profiles',type=Path,required=True)
    p.add_argument('--baseline-audit',type=Path,required=True)
    p.add_argument('--candidate-audit',type=Path,required=True)
    p.add_argument('--api-image',required=True)
    p.add_argument('--case',action='append',choices=(*DEFAULT_CASES,'series_day'))
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--timeout',type=int,default=1800)
    p.add_argument('--output',type=Path,default=HERE/'results/exp75_paired_live_profiles.json')
    a=p.parse_args()
    if a.reps<1 or a.baseline==a.candidate:p.error('positive repetitions and distinct endpoints required')
    if a.baseline_profiles.resolve()==a.candidate_profiles.resolve():p.error('use separate profile mounts')
    if not all(d.is_dir() for d in (a.baseline_profiles,a.candidate_profiles)):p.error('profile mounts must already exist')
    baseline=json.loads(a.baseline_audit.read_text());candidate=json.loads(a.candidate_audit.read_text())
    try:validate_candidate_audit(candidate);matched_coverage(baseline,candidate)
    except ValueError as exc:p.error(str(exc))
    endpoints=dict(time=a.baseline,plugin=a.candidate);directories=dict(time=a.baseline_profiles,plugin=a.candidate_profiles)
    report=dict(scope=__doc__,status='running',api_image=a.api_image,baseline_audit=baseline,candidate_audit=candidate,
                notes=NOTES,results=[],summaries=[],validation='full body for first measured response per layout/case; counts/schema every response')
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    with tempfile.TemporaryDirectory(prefix='sage-paired-profile-') as directory:
        body=Path(directory)/'body'
        for name,query in cases('unused'):
            if name not in (a.case or DEFAULT_CASES):continue
            reference=None;checked=set()
            for rep in range(a.reps+1):
                labels=['time','plugin'] if rep%2==0 else ['plugin','time']
                for layout in labels:
                    before=profile_paths(directories[layout])
                    result=receive(endpoints[layout],query,'arrow',body,a.timeout)
                    profile=fresh_profile(directories[layout],before)
                    if result['rows']==0 or result['rows']!=profile.get('rows'):raise ValueError('HTTP/native row counts differ or are empty')
                    if rep and layout not in checked:
                        digest=validate_body(body,'arrow')
                        if reference is None:reference=digest
                        if digest!=reference:raise ValueError(name+': baseline/candidate content differs')
                        checked.add(layout);result['validation']=digest
                    report['results'].append(dict(case=name,layout=layout,repetition=rep,
                                                  warmup=rep==0,response=result,profile=profile));save()
                    print(name,layout,'warmup' if rep==0 else rep,f"{result['total_seconds']:.3f}s",flush=True)
            for layout in endpoints:
                runs=[r for r in report['results'] if r['case']==name and r['layout']==layout and not r['warmup']]
                if len({r['response']['rows'] for r in runs})!=1:raise ValueError('row counts changed between repetitions')
                def med(section):
                    mappings=[section(r) for r in runs]
                    keys=set.intersection(*(set(m) for m in mappings))
                    return {k:statistics.median(m[k] for m in mappings) for k in sorted(keys)
                            if all(isinstance(m[k],(int,float)) for m in mappings)}
                report['summaries'].append(dict(case=name,layout=layout,
                    response=med(lambda r:r['response']),phases=med(lambda r:r['profile']['phases']),
                    operator_work=med(lambda r:r['profile']['duckdb']['operator_work_seconds']),
                    engine_metrics=med(lambda r:r['profile']['duckdb']['metrics']),
                    scoped_metrics=med(lambda r:r['profile']['duckdb']['scoped_numeric_metrics'])))
            save()
    report['status']='complete';save();print('Report:',a.output)


if __name__=='__main__':main()
