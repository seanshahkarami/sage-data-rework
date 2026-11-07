"""Experimental Flux-native integer SQL vs immutable exp87 Flux observations.

No HTTP/Influx/warehouse writes. Also exports small candidate HTTP probe inputs.
This is research code; no production backend path is enabled.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent))
import exp86_aggregation_semantics as base


def replay_cases(reference, fixture):
    output=[]
    for entry in reference['results']:
        probe=entry['probe']
        if probe.get('family')!='arithmetic' or probe.get('source')!='storage-native':continue
        if entry['result']['status']!='success':raise ValueError('successful native storage oracle required')
        if not re.fullmatch(r'[A-Za-z0-9_-]+',probe['id']):raise ValueError('unsafe captured probe id')
        name='exp87.'+probe['arithmetic_case']
        rows=[r for r in fixture if r['name']==name]
        if len(rows)!=2:raise ValueError('expected two captured arithmetic records')
        flux_path=reference['_source_dir']/(probe['id']+'.flux')
        flux=flux_path.read_text()
        match=re.search(r'range\(start: ([^,]+), stop: ([^)]+)\)',flux)
        if not match:raise ValueError('captured explicit range required')
        case=dict(id=probe['id'],name=name,function=probe['function'],window=probe['window'],
                  start=base.ns(match[1].strip()),end=base.ns(match[2].strip()),border=base.BASE+6*base.MINUTE)
        output.append((case,entry['result'],flux_path))
    if len(output)!=45:raise ValueError('expected all 45 arithmetic function/window cases')
    return output


def run(args):
    source=Path(args.source_report).resolve();root=Path(args.run_dir).resolve()
    if 'node-data' in source.parts or 'node-data' in root.parts or root.exists():raise ValueError('isolated source and fresh run directory required')
    raw=source.read_bytes();reference=json.loads(raw)
    if reference.get('experiment')!='exp87' or not reference.get('completed') or not reference.get('live_flux_executed'):
        raise ValueError('completed live exp87 report required')
    fixture_path=source.with_name('variant-fixture.json');fixture_raw=fixture_path.read_bytes();fixture=json.loads(fixture_raw)
    reference['_source_dir']=source.parent
    cases=replay_cases(reference,fixture)
    root.mkdir(parents=True,mode=0o700)
    duck=base.Duck(args.duckdb,root);version=duck.version()
    report=dict(experiment='exp88',integer_policy='flux64',duckdb_version=version,
        source_report_sha256=hashlib.sha256(raw).hexdigest(),fixture_sha256=hashlib.sha256(fixture_raw).hexdigest(),
        compiler_sha256=hashlib.sha256(Path(base.__file__).read_bytes()).hexdigest(),
        scope='Native integer candidate vs recorded Flux; local VARIANT and primitive Parquet staging, no live Flux/API/Iceberg/Pelican',
        results=[],completed=False)
    save=lambda:(root/'report.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    save()
    arithmetic=[r for r in fixture if r['name'].startswith('exp87.') and r['meta']['sensor']=='arithmetic']
    for case,oracle,flux_path in cases:
        result=dict(case=case,flux=oracle,flux_sha256=hashlib.sha256(flux_path.read_bytes()).hexdigest(),candidates={})
        for placement in ('cold_only','split'):
            setup,source_relation=base.placement_sql(arithmetic,case,root,placement)
            def execute():
                rows,seconds=duck.run(setup+base.aggregate_sql(case,source_relation,integer_policy='flux64'),case['id']+'-'+placement)
                return dict(rows=base.duck_records(rows),seconds=seconds)
            observed=base.attempt(execute)
            difference=base.compare(oracle['rows'],observed['rows']) if observed['status']=='success' else observed['error']
            observed['parity']='FAIL' if difference else 'PASS'
            if difference:observed['difference']=difference
            result['candidates'][placement]=observed
        report['results'].append(result);save()
    report['summary']={state:sum(x['parity']==state for c in report['results'] for x in c['candidates'].values()) for state in ('PASS','FAIL')}
    report['completed']=True;save()
    # The next fixture stage uses these same inputs/expectations, never a new oracle.
    (root/'fixture.json').write_text(json.dumps(arithmetic,indent=2)+'\n')
    print(json.dumps(report['summary'],indent=2))
    return 1 if report['summary']['FAIL'] else 0


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb',default='duckdb');parser.add_argument('--source-report',required=True);parser.add_argument('--run-dir',required=True)
    raise SystemExit(run(parser.parse_args()))


if __name__=='__main__':main()
