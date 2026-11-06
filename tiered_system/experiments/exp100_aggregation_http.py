"""Paired HTTP integration benchmark on reused API instances.

Read-only queries and container inspection. No deployment, seeding, publication
or deletion. Includes repeated/overlapping ranges and intervening short queries.
"""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import statistics
from urllib.parse import urlsplit

from exp95_aggregation_performance import WAREHOUSE,BORDER,SETTINGS,cases,runtime,stable,receive,records,validate_rows,compare_rows
from exp75_paired_live_profiles import fresh_profile,profile_paths


def sequence(placement,workload):
    original=cases();steps=[]
    def add(name,query):steps.append(dict(id=name,query=query))
    if placement=='cold' and workload=='mixed':
        month=original['series_month_mean']
        add('month_start',month);add('month_repeat',dict(month))
        add('month_overlap',dict(month,start='2025-03-08T00:00:00Z',end='2025-03-29T00:00:00Z'))
    for name,q in original.items():
        if name!='series_month_mean':add(name,q)
    add('day_head_mean',dict(original['series_day_mean'],head=1000))
    add('day_tail_mean',dict(original['series_day_mean'],tail=1000))
    if placement=='cold' and workload=='mixed':add('month_return',original['series_month_mean'])
    return steps


def validate_endpoints(endpoints):
    if len(set(endpoints.values()))!=2:raise ValueError('distinct dedicated endpoints required')
    for endpoint in endpoints.values():
        u=urlsplit(endpoint)
        if u.scheme!='http' or u.hostname not in ('localhost','127.0.0.1','::1') or u.username or u.password or u.query or u.fragment:
            raise ValueError('dedicated loopback HTTP endpoints required')


def check_runtime(states,placement,reference,profiled,reference_candidate=False):
    if reference_candidate:
        if reference.get('placement')!=placement:raise ValueError('reference placement differs')
        expected=reference['runtime_before']['candidate']
    else:
        expected=reference['runtime_before'][placement]
    border='2025-05-01T00:00:00Z' if placement=='cold' else BORDER
    for layout,state in states.items():
        env=state['settings']
        if not state['running'] or state['oom_killed']:raise ValueError('unhealthy API '+layout)
        if env['COLD_FRONTIER']!='static' or env['COLD_WAREHOUSE'].rstrip('/')!=WAREHOUSE or env['COLD_BORDER']!=border:
            raise ValueError('warehouse/frontier mismatch '+layout)
        if bool(env['COLD_PROFILE_DIR'])!=profiled:raise ValueError('profile phase mismatch')
        if env['ARROW_COMPRESSION'] not in ('','zstd'):raise ValueError('Zstd required')
        if env['INFLUXDB_BUCKET']!=expected['settings']['INFLUXDB_BUCKET']:raise ValueError('reuse the verified historical hot bucket')
        for key in SETTINGS:
            if key not in ('COLD_PROFILE_DIR',) and env[key]!=expected['settings'][key]:raise ValueError('execution setting differs: '+key)
    if states['baseline']['image']!=expected['image']:raise ValueError('baseline must use retained reference image')
    if any(states['baseline'][k]!=states['candidate'][k] for k in ('memory_limit','nano_cpus')):raise ValueError('paired resource limits differ')
    if states['candidate']['image']==states['baseline']['image'] or states['candidate']['id']==states['baseline']['id']:
        raise ValueError('candidate must be a separate newly built image/container')


def medians(items):
    keys=set.intersection(*(set(i) for i in items))
    return {k:statistics.median(i[k] for i in items) for k in sorted(keys) if all(type(i[k]) in (int,float) for i in items)}


def run(a):
    root=a.run_dir.resolve()
    if root.exists() or any('node-data' in p.resolve().parts for p in (root,a.reference)):raise ValueError('fresh isolated output and reference required')
    if a.reps<1 or a.timeout<1:raise ValueError('positive repetitions/timeout required')
    endpoints=dict(baseline=a.baseline,candidate=a.candidate);validate_endpoints(endpoints)
    containers=dict(baseline=a.baseline_container,candidate=a.candidate_container)
    directories=dict(baseline=a.baseline_profiles,candidate=a.candidate_profiles)
    profiled=bool(a.baseline_profiles)
    if profiled!=bool(a.candidate_profiles):raise ValueError('both profile mounts required')
    if profiled and (any(not p.is_dir() or 'node-data' in p.resolve().parts for p in directories.values()) or directories['baseline'].resolve()==directories['candidate'].resolve()):
        raise ValueError('separate existing isolated profile mounts required')
    if profiled!=bool(a.control):raise ValueError('diagnostics require completed unprofiled --control')
    formats=list(dict.fromkeys(a.format or (['arrow'] if profiled else ['arrow','ndjson'])))
    if profiled and formats!=['arrow']:raise ValueError('profiles are Arrow-only')
    reference=json.loads(a.reference.read_text())
    if reference['status']!='complete' or reference['profiled']:raise ValueError('completed unprofiled reference required')
    steps=sequence(a.placement,a.workload)
    reference_candidate=getattr(a,'reference_candidate',False)
    before={layout:runtime(c) for layout,c in containers.items()};check_runtime(before,a.placement,reference,profiled,reference_candidate)
    control=json.loads(a.control.read_text()) if a.control else None
    if control:
        if control['status']!='complete' or control['profiled'] or control['placement']!=a.placement or control['steps']!=steps or 'arrow' not in control['formats']:
            raise ValueError('matching unprofiled placement/workload/control required')
        for layout in before:
            old,new=control['runtime_before'][layout],before[layout]
            if any(old[k]!=new[k] for k in ('image','memory_limit','nano_cpus')):raise ValueError('control image/resources changed')
            if {k:v for k,v in old['settings'].items() if k!='COLD_PROFILE_DIR'}!={k:v for k,v in new['settings'].items() if k!='COLD_PROFILE_DIR'}:
                raise ValueError('control execution settings changed')
    root.mkdir(parents=True,mode=0o700)
    report=dict(status='running',scope=__doc__,placement=a.placement,workload=a.workload,profiled=profiled,formats=formats,steps=steps,
        runtime_before=before,reference=str(a.reference),reference_candidate=reference_candidate,reference_sha256=hashlib.sha256(a.reference.read_bytes()).hexdigest(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),control=str(a.control) if a.control else None,results=[],summaries=[],
        notes=['Reused API engines throughout; one excluded whole-sequence warmup; backend order alternates per repetition. No cold-cache claim.',
               'A whole sequence runs on one backend before switching. Arrow/NDJSON use their existing distinct native engines, serialized by the backend mutex.',
               'Wire parse/spooling included; semantic validation and read-only runtime checks excluded from receive timer.',
               'All bodies checked against immutable old-image baseline, including overlaps and limits. External caches/other host workloads remain uncontrolled.'])
    def save():
        p=root/'report.pending';p.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n');p.replace(root/'report.json')
    save();expected={}
    try:
        for fmt in formats:
            for rep in range(a.reps+1):
                for layout in (('baseline','candidate') if rep%2==0 else ('candidate','baseline')):
                    for step in steps:
                        q=step['query'];key=json.dumps(q,sort_keys=True)
                        body=root/f"{fmt}-{rep}-{layout}-{step['id']}.{fmt}"
                        paths=profile_paths(directories[layout]) if profiled else None
                        response=receive(endpoints[layout],q,fmt,body,a.timeout)
                        profile=fresh_profile(directories[layout],paths) if profiled else None
                        rows=records(body,fmt);validate_rows(q,rows)
                        if response['rows']!=len(rows):raise ValueError('wire/decoded row count mismatch')
                        if key not in expected:
                            if layout!='baseline':raise ValueError('old baseline must establish reference first')
                            expected[key]=rows
                        diff,_=compare_rows(expected[key],rows)
                        if diff:raise ValueError(step['id']+': response parity failed '+str(diff[:3]))
                        if profile and (profile['rows']!=len(rows) or not profile.get('engine_version','').startswith('v2.0.')):raise ValueError('profile rows/engine differ')
                        if control:
                            prior=next(r for r in control['results'] if r['step']==step['id'] and r['layout']=='baseline' and r['format']=='arrow')
                            path=a.control.parent/prior['body']
                            if Path(prior['body']).name!=prior['body'] or hashlib.sha256(path.read_bytes()).hexdigest()!=prior['body_sha256']:raise ValueError('control body changed')
                            diff,_=compare_rows(records(path,'arrow'),rows)
                            if diff:raise ValueError('diagnostic output differs from control')
                        report['results'].append(dict(step=step['id'],query=q,layout=layout,format=fmt,repetition=rep,warmup=rep==0,
                            response=response,profile=profile,body=body.name,body_sha256=hashlib.sha256(body.read_bytes()).hexdigest(),parity_passed=True))
                        save();print(a.placement,fmt,rep,layout,step['id'],round(response['total_seconds'],3),flush=True)
                    after={label:runtime(c) for label,c in containers.items()};stable(before,after)
        report['runtime_after']=after
        groups=defaultdict(list)
        for r in report['results']:
            if not r['warmup']:groups[r['step'],r['layout'],r['format']].append(r)
        for (step,layout,fmt),runs in groups.items():
            summary=dict(step=step,layout=layout,format=fmt,response=medians([r['response'] for r in runs]))
            if profiled:
                summary.update(phases=medians([r['profile']['phases'] for r in runs]),engine_metrics=medians([r['profile']['duckdb']['metrics'] for r in runs]))
            report['summaries'].append(summary)
        report['status']='complete';save()
    except Exception as exc:report.update(status='failed',error=str(exc));save();raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline',required=True);p.add_argument('--candidate',required=True)
    p.add_argument('--baseline-container',required=True);p.add_argument('--candidate-container',required=True)
    p.add_argument('--baseline-profiles',type=Path);p.add_argument('--candidate-profiles',type=Path);p.add_argument('--control',type=Path)
    p.add_argument('--reference-candidate',action='store_true',help='Use the candidate image from a completed same-placement paired HTTP report as the next baseline')
    p.add_argument('--reference',required=True,type=Path);p.add_argument('--run-dir',required=True,type=Path)
    p.add_argument('--placement',choices=('cold','split'),required=True);p.add_argument('--workload',choices=('mixed','short'),default='mixed')
    p.add_argument('--format',action='append',choices=('arrow','ndjson'));p.add_argument('--reps',type=int,default=3);p.add_argument('--timeout',type=int,default=1800)
    run(p.parse_args())

if __name__=='__main__':main()
