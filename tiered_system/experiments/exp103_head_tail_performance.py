"""Read-only historical head/tail HTTP benchmark and separate Arrow profiles.

Dedicated cold-only/split APIs use one image and the retained historical input.
No seeding, publication, deletion, deployment or container configuration.
"""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
from urllib.parse import urlsplit

from exp95_aggregation_performance import (WAREHOUSE, SETTINGS, cases as aggregate_cases,
    runtime, validate_runtime, stable, receive, records, medians)
from exp75_paired_live_profiles import profile_paths, fresh_profile


def cases():
    original = aggregate_cases()
    workloads = dict(series_day=original['series_day_mean'], series_hour=original['series_hour_mean'],
        fleet_minute=original['fleet_minute_count'], temperature_fleet_hour=original['temperature_fleet_hour_mean'])
    result = {}
    for name, source in workloads.items():
        for direction in ('head', 'tail'):
            for limit in (1, 1000):
                q = {k: v for k, v in source.items() if k not in ('experimental_func', 'experimental_window')}
                result[f'{name}_{direction}{limit}'] = dict(q, **{direction: limit})
    return result


def identity(row):
    return row['time'], row['name'], json.dumps(row['meta'], sort_keys=True)


def validate_rows(q, rows):
    from exp70_v3_live_baseline import timestamp_ns
    start, end = timestamp_ns(q['start']), timestamp_ns(q['end'])
    if not rows: raise ValueError('empty historical response; verify retained input coverage')
    seen, counts = set(), Counter()
    for row in rows:
        key = identity(row)
        if key in seen: raise ValueError('duplicate observation identity')
        seen.add(key)
        if not start <= row['time'] < end: raise ValueError('observation outside exclusive query bounds')
        if type(row['meta']) is not dict or any(type(k) is not str or type(v) is not str for k, v in row['meta'].items()):
            raise ValueError('invalid metadata')
        for k, v in q['filter'].items():
            if (row['name'] if k == 'name' else row['meta'].get(k)) != v: raise ValueError('filter mismatch')
        if type(row['value']) is float and not math.isfinite(row['value']): raise ValueError('nonfinite value')
        counts[row['name'], key[2]] += 1
    if any(count > q.get('head', q.get('tail')) for count in counts.values()): raise ValueError('limit exceeded per series')
    if any(rows[i]['time'] > rows[i+1]['time'] for i in range(len(rows)-1)): raise ValueError('output is not time ordered')


def compare_rows(expected, actual):
    if len(expected) != len(actual): raise ValueError('response row count differs')
    for a, b in zip(sorted(expected, key=identity), sorted(actual, key=identity)):
        if identity(a) != identity(b): raise ValueError('response identity differs')
        x, y = a['value'], b['value']
        if type(x) is not type(y): raise ValueError('value type differs (including integer/float narrowing)')
        if type(x) is float:
            if not math.isclose(x, y, rel_tol=1e-12, abs_tol=1e-10): raise ValueError('float value differs')
        elif x != y: raise ValueError('exact scalar differs')


def check_reference(states, reference, profiled):
    validate_runtime(states, WAREHOUSE, profiled)
    if reference.get('status') != 'complete' or reference.get('profiled') or reference.get('placement') != 'split':
        raise ValueError('completed retained split control reference required')
    expected = reference['runtime_before']['candidate']['settings']
    for state in states.values():
        for key in SETTINGS:
            if key not in ('COLD_BORDER', 'COLD_PROFILE_DIR') and state['settings'][key] != expected[key]:
                raise ValueError('retained historical input/execution settings differ: ' + key)


def check_control(control, before, selected):
    if control.get('experiment') != 'exp103' or control.get('status') != 'complete' or control.get('profiled') or control.get('selected_cases') != selected:
        raise ValueError('matching completed unprofiled control required')
    if control['queries'] != {name: cases()[name] for name in selected}: raise ValueError('control queries differ')
    for role, new in before.items():
        old = control['runtime_before'][role]
        if any(old[k] != new[k] for k in ('image', 'memory_limit', 'nano_cpus')): raise ValueError('control image/resources differ')
        if {k:v for k,v in old['settings'].items() if k!='COLD_PROFILE_DIR'} != {k:v for k,v in new['settings'].items() if k!='COLD_PROFILE_DIR'}:
            raise ValueError('control execution settings differ')


def run(a):
    root = a.run_dir.resolve()
    paths = [root, a.reference.resolve()] + [p.resolve() for p in (a.cold_profiles, a.split_profiles, a.control) if p]
    if root.exists() or any('node-data' in p.parts for p in paths): raise ValueError('fresh isolated output/reference/profile paths required')
    endpoints = dict(cold=a.cold, split=a.split)
    if len(set(endpoints.values())) != 2: raise ValueError('distinct dedicated endpoints required')
    for endpoint in endpoints.values():
        u = urlsplit(endpoint)
        if u.scheme!='http' or u.hostname not in ('localhost','127.0.0.1','::1') or u.username or u.password or u.query or u.fragment:
            raise ValueError('dedicated loopback HTTP endpoints required')
    if a.reps<1 or a.timeout<1: raise ValueError('positive repetitions/timeout required')
    if bool(a.cold_profiles)!=bool(a.split_profiles): raise ValueError('both profile mounts required')
    profiled = bool(a.cold_profiles)
    if profiled != bool(a.control): raise ValueError('profiles require --control; control belongs only to the diagnostic phase')
    dirs = dict(cold=a.cold_profiles, split=a.split_profiles)
    if profiled and (any(not p.is_dir() for p in dirs.values()) or dirs['cold'].resolve()==dirs['split'].resolve()):
        raise ValueError('separate existing profile mounts required')
    selected = list(dict.fromkeys(a.case or cases()))
    queries = {name:cases()[name] for name in selected}
    containers = dict(cold=a.cold_container, split=a.split_container)
    before = {role:runtime(c) for role,c in containers.items()}
    check_reference(before,json.loads(a.reference.read_text()),profiled)
    control = json.loads(a.control.read_text()) if a.control else None
    if control: check_control(control,before,selected)
    root.mkdir(parents=True,mode=0o700)
    report = dict(experiment='exp103',status='running',profiled=profiled,selected_cases=selected,queries=queries,
        runtime_before=before,reference=str(a.reference),reference_sha256=hashlib.sha256(a.reference.read_bytes()).hexdigest(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),control=str(a.control) if a.control else None,
        results=[],summaries=[],notes=[
            'Cold-only reference and split placement use identical historical observations and the same current image.',
            'One excluded warmup per case/placement/format, sequential requests, rotating placement/format order; no cold-cache claim.',
            'Full receive latency includes parsing/spooling, excludes semantic validation and container inspection; not dataframe latency.',
            'Every response checked against first cold reference; profiled responses also checked against hashed unprofiled control bodies.',
            'Arrow profiles are separate diagnostics. Stage timings combine source iteration, decoding, validation and Parquet writing.',
            'Runtime snapshots detect OOM/restarts, not continuous memory/scratch peaks. No throughput or Go CPU/allocation profile claim.'])
    def save():
        p=root/'report.pending';p.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n');p.replace(root/'report.json')
    save()
    try:
        for name,q in queries.items():
            expected=None
            for rep in range(a.reps+1):
                work=[(role,fmt) for role in ('cold','split') for fmt in (('arrow',) if profiled else ('arrow','ndjson'))]
                offset=rep%len(work)
                for role,fmt in work[offset:]+work[:offset]:
                    body=root/f'{name}-{role}-{rep}.{fmt}'
                    previous=profile_paths(dirs[role]) if profiled else None
                    response=receive(endpoints[role],q,fmt,body,a.timeout)
                    profile=fresh_profile(dirs[role],previous) if profiled else None
                    rows=records(body,fmt);validate_rows(q,rows)
                    if response['rows']!=len(rows): raise ValueError('wire/decoded row count differs')
                    if expected is None:
                        if role!='cold': raise ValueError('cold reference must run first')
                        expected=rows
                    compare_rows(expected,rows)
                    if profile:
                        if profile['rows']!=len(rows) or not profile.get('engine_version','').startswith('v2.0.'): raise ValueError('profile rows/engine differ')
                        if role=='split' and not all(k in profile['phases'] for k in ('aggregate_hot_rows','aggregate_input_bytes','aggregate_parquet_bytes','aggregate_hot_read_stage_seconds','aggregate_staging_complete_since_backend_start_seconds')):
                            raise ValueError('split staging profile fields missing')
                    if control:
                        prior=next(r for r in control['results'] if r['case']==name and r['role']=='cold' and r['format']=='arrow')
                        if Path(prior['body']).name!=prior['body']: raise ValueError('invalid control body path')
                        path=a.control.parent/prior['body']
                        if hashlib.sha256(path.read_bytes()).hexdigest()!=prior['body_sha256']: raise ValueError('control body changed')
                        compare_rows(records(path,'arrow'),rows)
                    report['results'].append(dict(case=name,role=role,format=fmt,repetition=rep,warmup=rep==0,
                        response=response,profile=profile,parity_passed=True,body=body.name,body_sha256=hashlib.sha256(body.read_bytes()).hexdigest()))
                    save();print(name,role,fmt,rep,round(response['total_seconds'],3),flush=True)
                after={role:runtime(c) for role,c in containers.items()};stable(before,after)
        report['runtime_after']=after
        for name in selected:
            for role in ('cold','split'):
                for fmt in (('arrow',) if profiled else ('arrow','ndjson')):
                    runs=[r for r in report['results'] if r['case']==name and r['role']==role and r['format']==fmt and not r['warmup']]
                    item=dict(case=name,role=role,format=fmt,response=medians([r['response'] for r in runs]))
                    if profiled:item.update(phases=medians([r['profile']['phases'] for r in runs]),engine_metrics=medians([r['profile']['duckdb']['metrics'] for r in runs]))
                    report['summaries'].append(item)
        report['status']='complete';save()
    except Exception as exc:
        report.update(status='failed',error=str(exc));save();raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for role in ('cold','split'):
        p.add_argument('--'+role,required=True);p.add_argument('--'+role+'-container',required=True);p.add_argument('--'+role+'-profiles',type=Path)
    p.add_argument('--reference',required=True,type=Path);p.add_argument('--control',type=Path)
    p.add_argument('--run-dir',required=True,type=Path);p.add_argument('--case',action='append',choices=tuple(cases()))
    p.add_argument('--reps',type=int,default=3);p.add_argument('--timeout',type=int,default=1800)
    run(p.parse_args())

if __name__=='__main__':main()
