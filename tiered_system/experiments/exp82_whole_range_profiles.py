"""Read-only whole-range HTTP measurements and request-paired native profiles.

Run on the VM beside one dedicated API container. The script inspects but never
configures/restarts containers. Profile timings are diagnostic, not the primary
unprofiled latency measurement. cgroup peak memory includes warmups/file cache
and is a container-lifetime high-water mark, not per-query RSS.
"""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from exp65_cold_query_profile import NOTES
from exp73_plugin_live_compare import receive,validate_body,full_warehouse_cases
from exp75_paired_live_profiles import fresh_profile,profile_paths

WAREHOUSE='https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse'
SETTINGS=dict(COLD_WAREHOUSE=WAREHOUSE,COLD_WATERMARK='2025-05-01T00:00:00Z',
              COLD_ASYNC_THREADS='8',COLD_READ_AHEAD_DEPTH='-1',ARROW_COMPRESSION='zstd')
CASES={name:query for name,query in full_warehouse_cases('unused') if name in
       ('2025-03_series_month','2025-04_series_month','2025-03-01_series_day','2025-03-01_series_hour')}


def command(args):
    result=subprocess.run(args,capture_output=True,text=True,timeout=30)
    if result.returncode:raise ValueError('read-only container inspection failed: '+result.stderr.strip())
    return result.stdout


def runtime(container,profiled,uniform=False):
    raw=json.loads(command(['docker','inspect',container]))[0]
    env=dict(item.split('=',1) for item in raw['Config']['Env'] if '=' in item)
    validate_settings(env,profiled,uniform)
    running=raw['State']['Status']=='running'
    peak=int(command(['docker','exec',container,'cat','/sys/fs/cgroup/memory.peak']).strip()) if running else None
    events=dict((name,int(value)) for name,value in (line.split() for line in
        command(['docker','exec',container,'cat','/sys/fs/cgroup/memory.events']).splitlines())) if running else {}
    settings={k:env.get(k,'') for k in (*SETTINGS,'COLD_PROFILE_DIR')}
    settings['COLD_UNIFORM_METADATA']=env.get('COLD_UNIFORM_METADATA','false')
    return dict(container_id=raw['Id'],image=raw['Image'],settings=settings,
                memory_limit_bytes=raw['HostConfig']['Memory'],nano_cpus=raw['HostConfig'].get('NanoCpus'),
                status=raw['State']['Status'],restarts=raw['RestartCount'],oom_killed=raw['State']['OOMKilled'],
                cgroup_peak_memory_bytes=peak,cgroup_memory_events=events)


def validate_settings(env,profiled,uniform=False):
    if any(env.get(k)!=v for k,v in SETTINGS.items()):raise ValueError('API does not use the adopted warehouse and recorded execution settings')
    if bool(env.get('COLD_PROFILE_DIR'))!=profiled:raise ValueError('API profiling setting does not match this phase')
    setting=env.get('COLD_UNIFORM_METADATA','false').lower()
    if setting not in (('true','1','t') if uniform else ('false','0','f')):
        raise ValueError('API uniform-metadata setting does not match this phase')


def healthy(before,after):
    if before['container_id']!=after['container_id'] or before['image']!=after['image'] or before['settings']!=after['settings']:
        raise ValueError('container/configuration changed during measurement')
    if after['status']!='running' or after['oom_killed'] or after['restarts']!=before['restarts']:
        raise ValueError('container restarted or was OOM-killed during measurement')
    for key in ('oom','oom_kill','oom_group_kill'):
        if after['cgroup_memory_events'].get(key,0)>before['cgroup_memory_events'].get(key,0):
            raise ValueError('container memory event increased: '+key)


def reference_signatures(report,selected):
    if report.get('status')!='complete':raise ValueError('reference sweep is incomplete')
    signatures={}
    for name in selected:
        records=[r for r in report['results'] if r['case']==name and r['layout']=='plugin_vsn_time' and r['format']=='arrow']
        validated=[r['validation'] for r in records if 'validation' in r]
        if not validated or any(item!=validated[0] for item in validated):raise ValueError('missing or inconsistent reference content: '+name)
        if any(r['query']!=CASES[name] or r['rows']!=validated[0]['rows'] for r in records):raise ValueError('reference query/count changed: '+name)
        signatures[name]=validated[0]
    return signatures


def median_mapping(mappings):
    keys=set.intersection(*(set(m) for m in mappings))
    return {key:statistics.median(m[key] for m in mappings) for key in sorted(keys)
            if all(isinstance(m[key],(int,float)) and not isinstance(m[key],bool) for m in mappings)}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint',required=True)
    p.add_argument('--container',required=True,help='dedicated API container name/ID, for read-only configuration/cgroup checks')
    p.add_argument('--profiles',type=Path,help='mounted profile directory; omit for unprofiled control')
    p.add_argument('--control',type=Path,help='completed exp82 unprofiled report; required with --profiles')
    p.add_argument('--uniform-metadata',action='store_true',help='verify the operator-controlled uniform metadata candidate is enabled')
    p.add_argument('--reference',type=Path,default=HERE/'results/full-layout-1792962354/http-arrow.json')
    p.add_argument('--case',action='append',choices=tuple(CASES))
    p.add_argument('--reps',type=int,default=3)
    p.add_argument('--timeout',type=int,default=1800)
    p.add_argument('--output',type=Path,required=True,help='fresh result filename')
    a=p.parse_args()
    if a.output.exists() or 'node-data' in a.output.resolve().parts:p.error('fresh isolated output required')
    if a.reps<1 or a.timeout<1:p.error('positive repetitions/timeout required')
    if a.profiles and not a.control:p.error('--profiles requires a completed unprofiled --control')
    if a.control and not a.profiles and not a.uniform_metadata:p.error('unprofiled --control is for the uniform metadata candidate')
    if a.profiles and (not a.profiles.is_dir() or 'node-data' in a.profiles.resolve().parts):p.error('existing isolated profile mount required')
    names=list(dict.fromkeys(a.case or ['2025-03_series_month','2025-04_series_month']))
    expected=reference_signatures(json.loads(a.reference.read_text()),names)
    before=runtime(a.container,bool(a.profiles),a.uniform_metadata)
    if before['status']!='running' or before['oom_killed']:p.error('dedicated API must be running without an OOM-killed state')
    control=json.loads(a.control.read_text()) if a.control else None
    if control:
        if control.get('status')!='complete' or control.get('profiled') or control.get('selected_cases')!=names:
            p.error('use a completed unprofiled control with the same selected cases')
        prior=control['runtime_before']
        if prior['image']!=before['image'] or any(prior[k]!=before[k] for k in ('memory_limit_bytes','nano_cpus')):
            p.error('control image or resource limits differ')
        if any(prior['settings'].get(k)!=before['settings'].get(k) for k in SETTINGS):p.error('control execution settings differ')
        if a.profiles and bool(control.get('uniform_metadata',False))!=a.uniform_metadata:
            p.error('profiled phase must reference an unprofiled control with the same metadata setting')
    report=dict(status='running',scope=__doc__,profiled=bool(a.profiles),uniform_metadata=a.uniform_metadata,selected_cases=names,
                endpoint=a.endpoint,reference=str(a.reference),runtime_before=before,notes=NOTES,
                control_source=str(a.control) if a.control else None,
                cache_protocol='one excluded warmup per case; sequential requests, case order rotates; OS/Pelican caches uncontrolled',
                validation='full body first measured response per case; schema/count every response; signatures checked against completed full-layout sweep',
                results=[],summaries=[])
    def save():
        a.output.parent.mkdir(parents=True,exist_ok=True)
        pending=a.output.with_suffix('.pending');pending.write_text(json.dumps(report,indent=2)+'\n');pending.replace(a.output)
    save()
    try:
        with tempfile.TemporaryDirectory(prefix='sage-exp82-') as directory:
            body=Path(directory)/'body';checked=set()
            for rep in range(a.reps+1):
                offset=rep%len(names)
                for name in names[offset:]+names[:offset]:
                    paths=profile_paths(a.profiles) if a.profiles else None
                    response=receive(a.endpoint,CASES[name],'arrow',body,a.timeout)
                    if response['rows']!=expected[name]['rows']:raise ValueError(name+': response count differs from reference')
                    profile=fresh_profile(a.profiles,paths) if a.profiles else None
                    if profile:
                        if profile['rows']!=response['rows'] or not profile.get('engine_version','').startswith('v2.0.'):
                            raise ValueError('native profile row count or engine version mismatch')
                        if a.uniform_metadata and name.endswith('series_month'):
                            phases=profile['phases']
                            if phases.get('metadata_discovery_rows')!=1 or phases.get('metadata_reuse_applied')!=1:
                                raise ValueError('expected uniform metadata fast path was not applied')
                    if rep and name not in checked:
                        digest=validate_body(body,'arrow')
                        if digest!=expected[name]:raise ValueError(name+': content differs from adopted warehouse reference')
                        response['validation']=digest;checked.add(name)
                    report['results'].append(dict(case=name,query=CASES[name],repetition=rep,warmup=rep==0,response=response,profile=profile));save()
                    print(name,'warmup' if rep==0 else rep,f"first={response['first_result_seconds']:.3f}s total={response['total_seconds']:.3f}s",flush=True)
        report['runtime_after']=runtime(a.container,bool(a.profiles),a.uniform_metadata);healthy(before,report['runtime_after'])
        for name in names:
            runs=[r for r in report['results'] if r['case']==name and not r['warmup']]
            summary=dict(case=name,response=median_mapping([r['response'] for r in runs]))
            if a.profiles:
                summary.update(phases=median_mapping([r['profile']['phases'] for r in runs]),
                    operator_work=median_mapping([r['profile']['duckdb']['operator_work_seconds'] for r in runs]),
                    engine_metrics=median_mapping([r['profile']['duckdb']['metrics'] for r in runs]),
                    scoped_metrics=median_mapping([r['profile']['duckdb']['scoped_numeric_metrics'] for r in runs]))
            if control:summary['unprofiled_control_response']=next(s['response'] for s in control['summaries'] if s['case']==name)
            report['summaries'].append(summary)
        report['status']='complete';save()
    except Exception as exc:
        report['status']='failed';report['error']=str(exc)
        try:report['runtime_after']=runtime(a.container,bool(a.profiles),a.uniform_metadata)
        except Exception as telemetry_error:report['telemetry_error']=str(telemetry_error)
        save();raise


if __name__=='__main__':main()
