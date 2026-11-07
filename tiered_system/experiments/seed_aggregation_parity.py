"""Seed a NEW isolated loopback Influx bucket from the actual exp91 inputs.

Retains the bucket and all evidence. No production endpoint, Pelican or deletion.
Verifies native raw kinds/values before enabling the live three-tier API test.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parent/'runbooks'))
import exp86_aggregation_semantics as base


def run(args):
    fixture=Path(args.fixture_root).resolve();root=Path(args.run_dir).resolve()
    url=urlsplit(args.influx_url)
    if (url.scheme not in ('http','https') or url.username is not None or url.password is not None
            or url.query or url.fragment or url.hostname not in ('localhost','127.0.0.1','::1')):
        raise ValueError('only a dedicated loopback Influx fixture is allowed')
    if root.exists() or any('node-data' in p.parts for p in (fixture,root)):
        raise ValueError('new isolated outputs and fixture required')
    manifest_bytes=(fixture/'http-manifest.json').read_bytes();manifest=json.loads(manifest_bytes)
    prepared=json.loads((fixture/'report.json').read_bytes())
    if not prepared.get('completed') or prepared['summary']['passed']!=prepared['summary']['total']:
        raise ValueError('passing local Iceberg preparation required')
    names={case['request']['filter']['name'] for case in manifest['cases']}
    rows=[r for r in json.loads((fixture/'fixture.json').read_bytes()) if r['name'] in names]
    if not rows:raise ValueError('fixture has no queried inputs')
    # Unqueried mixed-kind encoding controls cannot be written into a single
    # Influx shard under one measurement. They remain cold raw-storage checks.
    token=os.environ.get('INFLUX_TOKEN')
    if not token:raise ValueError('INFLUX_TOKEN for the dedicated fixture required')
    root.mkdir(parents=True,mode=0o700)
    bucket='agg-exp86-'+uuid.uuid4().hex
    report=dict(experiment='aggregation-live-parity-seed',completed=False,url=args.influx_url,
                org=args.org,bucket=bucket,manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                source_rows=len(rows),retained_bucket=True,scope='New local synthetic bucket; no production writes/deletion')
    save=lambda:(root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    save()
    client=base.Influx(args.influx_url,token,args.org,bucket)
    try:
        report['influx_health']=json.loads(client.request('/health'))
        report['bucket_id']=client.setup(rows);save()
        start=min(r['time'] for r in rows);end=max(r['time'] for r in rows)+1
        flux='from(bucket: '+json.dumps(bucket)+') |> range(start: '+base.stamp(start)+', stop: '+base.stamp(end)+')'
        (root/'preflight.flux').write_text(flux+'\n')
        body=client.query(flux);(root/'preflight.csv').write_text(body)
        observed=base.parse_flux(body,True);difference=base.compare(rows,observed)
        report['raw_difference']=difference
        if difference:raise ValueError('raw native fixture round trip differs: '+difference)
        report.update(completed=True,raw_rows_verified=len(observed));save()
        print('Verified native rows:',len(observed));print('Seed report:',root/'report.json')
        return 0
    except Exception as error:
        report['error']=str(error).replace(token,'<redacted>');save();raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture-root',required=True);parser.add_argument('--run-dir',required=True)
    parser.add_argument('--influx-url',default='http://127.0.0.1:18096');parser.add_argument('--org',default='waggle')
    try:return run(parser.parse_args())
    except Exception as error:
        message=str(error)
        if os.environ.get('INFLUX_TOKEN'):message=message.replace(os.environ['INFLUX_TOKEN'],'<redacted>')
        print('Parity seed stopped: '+message,file=sys.stderr);return 1


if __name__=='__main__':raise SystemExit(main())
