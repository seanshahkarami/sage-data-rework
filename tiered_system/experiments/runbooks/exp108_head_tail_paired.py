"""Paired retained/candidate split HTTP measurements, followed by separate profiles.

Build both images from exp108 snapshots; the main API checkout is unchanged.
No seeding, publication, deletion, deployment or container configuration.
"""
import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from exp95_aggregation_performance import (WAREHOUSE, SETTINGS,
    runtime, stable, receive, records, medians)
from exp75_paired_live_profiles import profile_paths, fresh_profile

from exp103_head_tail_performance import cases, validate_rows, compare_rows
from exp108_prepare_head_tail_candidate import digest


def check_snapshot(path):
    manifest = json.loads(path.read_text())
    if manifest.get('status') != 'prepared': raise ValueError('prepared source manifest required')
    for role in ('baseline', 'candidate'):
        root = path.parent / role
        if any(p.is_symlink() for p in root.rglob('*')): raise ValueError('snapshot symlinks rejected')
        actual = {str(p.relative_to(root)):digest(p) for p in root.rglob('*') if p.is_file()}
        if actual != manifest['files'][role]: raise ValueError('source snapshot changed: ' + role)
    return manifest


def check_reference(states, reference, profiled):
    if reference.get('experiment') != 'exp103' or reference.get('status') != 'complete' or reference.get('profiled'):
        raise ValueError('completed exp106 historical control required')
    if reference.get('queries') != cases(): raise ValueError('historical query matrix differs')
    expected = reference['runtime_before']['split']['settings']
    for role, state in states.items():
        env = state['settings']
        if not state['running'] or state['oom_killed']: raise ValueError('unhealthy API: ' + role)
        if env['COLD_FRONTIER'] != 'static' or env['COLD_BORDER'] != '2025-03-01T12:00:00Z' or env['COLD_WAREHOUSE'].rstrip('/') != WAREHOUSE:
            raise ValueError('use split static routing on the adopted warehouse')
        if bool(env['COLD_PROFILE_DIR']) != profiled: raise ValueError('profile phase differs')
        for key in SETTINGS:
            if key != 'COLD_PROFILE_DIR' and env[key] != expected[key]: raise ValueError('retained settings differ: ' + key)
    a, b = states['baseline'], states['candidate']
    if a['id'] == b['id'] or a['image'] == b['image']: raise ValueError('distinct APIs and baseline/candidate images required')
    for key in ('memory_limit', 'nano_cpus'):
        if a[key] != b[key]: raise ValueError('resource limits differ')


def reference_rows(reference_path, reference, name):
    prior = next(r for r in reference['results'] if r['case'] == name and r['role'] == 'cold' and r['format'] == 'ndjson' and not r['warmup'])
    if not prior.get('parity_passed') or Path(prior['body']).name != prior['body']: raise ValueError('invalid reference body')
    path = reference_path.parent / prior['body']
    if digest(path) != prior['body_sha256']: raise ValueError('reference body changed')
    rows = records(path, 'ndjson'); validate_rows(cases()[name], rows)
    return rows


def check_control(control, before, selected):
    if control.get('experiment') != 'exp108' or control.get('status') != 'complete' or control.get('profiled') or control.get('selected_cases') != selected:
        raise ValueError('matching completed unprofiled control required')
    if control['queries'] != {name: cases()[name] for name in selected}: raise ValueError('control queries differ')
    for role, new in before.items():
        old = control['runtime_before'][role]
        if any(old[k] != new[k] for k in ('image', 'memory_limit', 'nano_cpus')): raise ValueError('control image/resources differ')
        if {k:v for k,v in old['settings'].items() if k!='COLD_PROFILE_DIR'} != {k:v for k,v in new['settings'].items() if k!='COLD_PROFILE_DIR'}:
            raise ValueError('control execution settings differ')


def run(a):
    root = a.run_dir.resolve()
    paths = [root, a.reference.resolve(), a.source_manifest.resolve()] + [p.resolve() for p in (a.baseline_profiles, a.candidate_profiles, a.control) if p]
    if root.exists() or any('node-data' in p.parts for p in paths): raise ValueError('fresh isolated output/reference/profile paths required')
    endpoints = dict(baseline=a.baseline, candidate=a.candidate)
    if len(set(endpoints.values())) != 2: raise ValueError('distinct dedicated endpoints required')
    for endpoint in endpoints.values():
        u = urlsplit(endpoint)
        if u.scheme!='http' or u.hostname not in ('localhost','127.0.0.1','::1') or u.username or u.password or u.query or u.fragment:
            raise ValueError('dedicated loopback HTTP endpoints required')
    if a.reps<1 or a.timeout<1: raise ValueError('positive repetitions/timeout required')
    if bool(a.baseline_profiles)!=bool(a.candidate_profiles): raise ValueError('both profile mounts required')
    profiled = bool(a.baseline_profiles)
    if profiled != bool(a.control): raise ValueError('profiles require --control; control belongs only to the diagnostic phase')
    dirs = dict(baseline=a.baseline_profiles, candidate=a.candidate_profiles)
    if profiled and (any(not p.is_dir() for p in dirs.values()) or dirs['baseline'].resolve()==dirs['candidate'].resolve()):
        raise ValueError('separate existing profile mounts required')
    selected = list(dict.fromkeys(a.case or cases()))
    queries = {name:cases()[name] for name in selected}
    containers = dict(baseline=a.baseline_container, candidate=a.candidate_container)
    before = {role:runtime(c) for role,c in containers.items()}
    reference=json.loads(a.reference.read_text())
    check_reference(before,reference,profiled)
    snapshot=check_snapshot(a.source_manifest)
    if before['baseline']['image']!=a.baseline_image or before['candidate']['image']!=a.candidate_image:
        raise ValueError('container image does not match recorded build image ID')
    control = json.loads(a.control.read_text()) if a.control else None
    if control:
        check_control(control,before,selected)
        if control.get('source_manifest_sha256') != digest(a.source_manifest): raise ValueError('control source differs')
    root.mkdir(parents=True,mode=0o700)
    report = dict(experiment='exp108',status='running',profiled=profiled,selected_cases=selected,queries=queries,
        runtime_before=before,source_manifest=str(a.source_manifest),source_manifest_sha256=digest(a.source_manifest),
        source_changes=snapshot['changed'],reference=str(a.reference),reference_sha256=hashlib.sha256(a.reference.read_bytes()).hexdigest(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),control=str(a.control) if a.control else None,
        results=[],summaries=[],notes=[
            'Two split placements; only the isolated candidate source changes standalone selection.',
            'One excluded warmup; sequential requests with rotating plan/format order; no cold-cache claim.',
            'Full receive includes parsing/spooling, excludes validation/inspection; not Sage-client dataframe latency.',
            'Every body compared to hashed retained exp106 cold NDJSON; profiled phase requires matching control.',
            'Arrow diagnostics separate from unprofiled primary medians. Timings overlap; buffer peaks are not process RSS.',
            'No seeding, publication, deletion or deployment. Runtime snapshots detect restarts/OOM, not continuous peaks.'])
    def save():
        p=root/'report.pending';p.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n');p.replace(root/'report.json')
    save()
    try:
        for name,q in queries.items():
            expected=reference_rows(a.reference,reference,name)
            for rep in range(a.reps+1):
                work=[(role,fmt) for role in ('baseline','candidate') for fmt in (('arrow',) if profiled else ('arrow','ndjson'))]
                offset=rep%len(work)
                for role,fmt in work[offset:]+work[:offset]:
                    body=root/f'{name}-{role}-{rep}.{fmt}'
                    previous=profile_paths(dirs[role]) if profiled else None
                    response=receive(endpoints[role],q,fmt,body,a.timeout)
                    profile=fresh_profile(dirs[role],previous) if profiled else None
                    rows=records(body,fmt);validate_rows(q,rows)
                    if response['rows']!=len(rows): raise ValueError('wire/decoded row count differs')
                    compare_rows(expected,rows)
                    if profile:
                        if profile['rows']!=len(rows) or not profile.get('engine_version','').startswith('v2.0.'): raise ValueError('profile rows/engine differ')
                        if not all(k in profile['phases'] for k in ('aggregate_hot_rows','aggregate_input_bytes','aggregate_parquet_bytes','aggregate_hot_read_stage_seconds','aggregate_staging_complete_since_backend_start_seconds')):
                            raise ValueError('split staging profile fields missing')
                    if control:
                        prior=next(r for r in control['results'] if r['case']==name and r['role']=='baseline' and r['format']=='arrow')
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
            for role in ('baseline','candidate'):
                for fmt in (('arrow',) if profiled else ('arrow','ndjson')):
                    runs=[r for r in report['results'] if r['case']==name and r['role']==role and r['format']==fmt and not r['warmup']]
                    item=dict(case=name,role=role,format=fmt,response=medians([r['response'] for r in runs]))
                    if profiled:item.update(phases=medians([r['profile']['phases'] for r in runs]),engine_metrics=medians([r['profile']['duckdb']['metrics'] for r in runs]))
                    report['summaries'].append(item)
        report['comparisons']=[]
        for name in selected:
            for fmt in (('arrow',) if profiled else ('arrow','ndjson')):
                values={role:next(s['response']['total_seconds'] for s in report['summaries'] if s['case']==name and s['format']==fmt and s['role']==role) for role in ('baseline','candidate')}
                report['comparisons'].append(dict(case=name,format=fmt,**values,speedup=values['baseline']/values['candidate']))
        check_snapshot(a.source_manifest)
        report['status']='complete';save()
    except Exception as exc:
        report.update(status='failed',error=str(exc));save();raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for role in ('baseline','candidate'):
        p.add_argument('--'+role,required=True);p.add_argument('--'+role+'-container',required=True);p.add_argument('--'+role+'-profiles',type=Path)
    p.add_argument('--source-manifest',required=True,type=Path)
    p.add_argument('--baseline-image',required=True);p.add_argument('--candidate-image',required=True)
    p.add_argument('--reference',required=True,type=Path);p.add_argument('--control',type=Path)
    p.add_argument('--run-dir',required=True,type=Path);p.add_argument('--case',action='append',choices=tuple(cases()))
    p.add_argument('--reps',type=int,default=3);p.add_argument('--timeout',type=int,default=1800)
    run(p.parse_args())

if __name__=='__main__':main()
