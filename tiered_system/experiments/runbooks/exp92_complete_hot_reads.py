"""Read-only mixed-kind completeness experiment on the retained exp87 fixture.

Uses actual shard-group metadata, never guessed time slices. No writes, buckets,
Influx deletion, engine mounts, production API, or Pelican operations. Metadata
backup contains secrets: persist ONLY the selected bucket's shard manifest.
"""
import argparse
from email.parser import BytesParser
from email.policy import default as email_default
import hashlib
import json
import os
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exp86_aggregation_semantics as base
import exp87_aggregation_type_probes as probe

ZERO = '0001-01-01T00:00:00Z'


def selected_manifest(metadata, bucket, bucket_id, org):
    """Discard KV/SQL and all unrelated buckets before any output is written."""
    matches = [b for b in metadata['buckets'] if b.get('bucketName') == bucket
               and b.get('bucketID') == bucket_id and b.get('organizationName') == org]
    if len(matches) != 1:
        raise ValueError('exactly one matching fixture bucket manifest required')
    b = matches[0]
    return dict(bucketName=bucket, bucketID=bucket_id, organizationName=org,
                retentionPolicies=[dict(name=p['name'], shardGroups=[
                    {k: g[k] for k in ('id', 'startTime', 'endTime', 'deletedAt', 'truncatedAt') if k in g}
                    for g in p['shardGroups']]) for p in b['retentionPolicies']])


def shard_ranges(manifest, start, stop):
    """Clip actual live groups; reject overlapping ranges that could hide kinds."""
    if start >= stop:
        raise ValueError('nonempty half-open range required')
    intervals = []
    for policy in manifest['retentionPolicies']:
        for group in policy['shardGroups']:
            if group.get('deletedAt') not in (None, '', ZERO):
                continue
            low, high = base.ns(group['startTime']), base.ns(group['endTime'])
            if group.get('truncatedAt') not in (None, '', ZERO):
                high = min(high, base.ns(group['truncatedAt']))
            if low >= high:
                raise ValueError('invalid shard-group bounds')
            a, b = max(low, start), min(high, stop)
            if a < b:
                intervals.append(dict(start=a, stop=b, group_id=group['id']))
    intervals.sort(key=lambda x: (x['start'], x['stop']))
    for left, right in zip(intervals, intervals[1:]):
        if left['stop'] > right['start']:
            raise ValueError('overlapping shard groups: complete type-safe reads not established')
    return intervals


def bucket_manifests_from_backup(content_type, body):
    """Keep the buckets JSON part. The SQL/KV parts stay out of every saved file."""
    if not isinstance(body, (bytes, bytearray)):
        raise ValueError('metadata backup must be parsed from bytes')
    media = content_type.split(';', 1)[0].strip().lower()
    if media == 'application/json':
        document = json.loads(body)
    elif media == 'multipart/mixed':
        message = BytesParser(policy=email_default).parsebytes(
            b'Content-Type: ' + content_type.encode() + b'\r\nMIME-Version: 1.0\r\n\r\n' + bytes(body))
        found = [part.get_payload(decode=True) for part in message.iter_attachments()
                 if part.get_param('name', header='content-disposition') == 'buckets']
        if len(found) != 1 or found[0] is None:
            raise ValueError('metadata backup must contain exactly one buckets part')
        document = json.loads(found[0])
    else:
        raise ValueError('metadata backup content type is not usable')
    if isinstance(document, list):
        return {'buckets': document}
    if isinstance(document, dict) and isinstance(document.get('buckets'), list):
        return {'buckets': document['buckets']}
    raise ValueError('metadata backup buckets part has an unexpected shape')


class FixtureReader(probe.ReadOnlyInflux):
    def request(self, path, data=None, content='application/json', accept='application/json'):
        if path == '/api/v2/backup/metadata' and data is None:
            return base.Influx.request(self, path, accept=accept)
        return super().request(path, data, content, accept)

    def backup_buckets(self):
        captured = {}
        body = base.Influx.request(self, '/api/v2/backup/metadata',
                                    accept='multipart/mixed, application/json', capture=captured)
        try:
            return bucket_manifests_from_backup(captured.get('content_type', ''), body)
        finally:
            del body


def checked_reference(path):
    reference = json.loads(path.read_bytes())
    bucket = reference.get('variant_bucket', '')
    if (reference.get('experiment') != 'exp87' or not reference.get('completed')
            or not reference.get('live_flux_executed')
            or not re.fullmatch(r'agg-exp87-[a-f0-9]{32}', bucket)):
        raise ValueError('completed live exp87 report with its isolated chronology bucket required')
    rows = json.loads(path.with_name('variant-fixture.json').read_bytes())
    mixed = [r for r in rows if r['meta'].get('sensor') == 'mixed']
    if len(mixed) != 6 or len({r['name'] for r in mixed}) != 3:
        raise ValueError('expected three two-point mixed series')
    return reference, mixed


def check_aggregate_contract(rows, duck, report, save):
    # Count all recovered rows. Arithmetic must reject mixed kinds instead of
    # silently discarding or converting one kind. This does not choose a new
    # mathematical mixed-numeric policy or reproduce Influx's omission.
    for name in sorted({r['name'] for r in rows}):
        case = dict(name=name, start=probe.START, end=probe.STOP, border=probe.SPLIT, window=None)
        for function in base.FUNCTIONS:
            case['function'] = function
            label = name + '-complete-' + function
            sql = base.source_sql(rows) + base.aggregate_sql(case, 'full_raw', integer_policy='flux64')
            try:
                observed, _ = duck.run(sql, label)
                passed = function == 'count' and len(observed) == 1 and json.loads(observed[0]['value']) == 2
                entry = dict(name=name, method='complete-input-' + function, passed=passed, rows=observed)
            except RuntimeError as error:
                passed = function != 'count' and 'unsupported or mixed aggregate input' in str(error)
                entry = dict(name=name, method='complete-input-' + function, passed=passed, error=str(error))
            report['results'].append(entry)
            save()
            if not passed:
                raise AssertionError('complete mixed input count/rejection contract failed')


def run(args):
    source = Path(args.source_report).resolve()
    root = Path(args.run_dir).resolve()
    manifest_path = Path(args.shard_manifest).resolve() if args.shard_manifest else None
    if root.exists() or any('node-data' in p.parts for p in (root, source, *([manifest_path] if manifest_path else []))):
        raise ValueError('new isolated output directory and inputs required')
    reference, expected = checked_reference(source)
    token = os.environ.get('INFLUX_TOKEN')
    if not token and not args.local_only:
        raise ValueError('INFLUX_TOKEN required for fixture reads')
    root.mkdir(parents=True, mode=0o700)
    duck = base.Duck(args.duckdb, root)
    version = duck.version()
    bucket = reference['variant_bucket']
    reader = FixtureReader(args.influx_url, token, args.org, bucket)
    report = dict(experiment='exp92', completed=False, duckdb_version=version,
                  scope='Read-only exp87 fixture; actual shard ranges; no production changes',
                  source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                  script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  bucket=bucket, results=[])
    save = lambda: (root / 'report.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    save()
    if args.local_only:
        report['scope'] = 'Synthetic complete-input SQL contract only; no live extraction or metadata validation'
        report['live_flux_executed'] = False
        check_aggregate_contract(expected, duck, report, save)
        report.update(completed=True, synthetic_rows=len(expected),
                      summary=dict(gates=len(report['results']), passed=sum(r['passed'] for r in report['results'])))
        save()
        print(json.dumps(report['summary'], indent=2))
        return 0
    report['live_flux_executed'] = True
    report['influx_health'] = json.loads(reader.request('/health'))
    if manifest_path:
        manifest = json.loads(manifest_path.read_bytes())
    else:
        # Never write the raw backup or its KV/SQL fields to disk. A distinct
        # operator credential can retrieve metadata without widening query access.
        metadata_reader = FixtureReader(args.influx_url, os.environ.get('INFLUX_METADATA_TOKEN', token), args.org, bucket)
        metadata = metadata_reader.backup_buckets()
        manifest = selected_manifest(metadata, bucket, reference['variant_bucket_id'], args.org)
        del metadata
    if (manifest.get('bucketName'), manifest.get('bucketID'), manifest.get('organizationName')) != (bucket, reference['variant_bucket_id'], args.org):
        raise ValueError('shard manifest identity differs from fixture')
    # Even a supplied file must pass the allowlist, so no unrelated fields persist.
    manifest = selected_manifest({'buckets': [manifest]}, bucket, reference['variant_bucket_id'], args.org)
    (root / 'shard-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    intervals = shard_ranges(manifest, probe.START, probe.STOP)
    if not intervals:
        raise ValueError('fixture shard metadata is empty')
    report['ranges'] = intervals
    save()
    all_rows = []
    for name in sorted({r['name'] for r in expected}):
        wanted = [r for r in expected if r['name'] == name]
        whole = probe.range_filter(bucket, name, probe.START, probe.STOP)
        control = probe.execute_probe(reader, dict(id=name + '-whole-control', flux=whole, parse_time=True), root)
        report['results'].append(dict(name=name, method='whole-control', result=control,
                                     difference=base.compare(wanted, control['rows']) if control['status']=='success' else 'query error'))
        # Diagnostic only: determine whether a type predicate changes storage
        # visibility. It is never assumed to recover omitted rows.
        typed = []
        errors = []
        for kind in ('int', 'float'):
            flux = 'import "types"\n' + whole + ' |> filter(fn: (r) => types.isType(v: r._value, type: "' + kind + '"))'
            result = probe.execute_probe(reader, dict(id=name + '-type-' + kind, flux=flux, parse_time=True), root)
            if result['status'] == 'success': typed.extend(result['rows'])
            else: errors.append(result)
        report['results'].append(dict(name=name, method='type-predicate-control', errors=errors,
                                     difference=base.compare(wanted, typed), rows=typed))
        complete = []
        for index, interval in enumerate(intervals):
            query = dict(id=name + '-shard-' + str(index), parse_time=True,
                         flux=probe.range_filter(bucket, name, interval['start'], interval['stop']))
            result = probe.execute_probe(reader, query, root)
            if result['status'] != 'success':
                raise RuntimeError('shard-scoped raw read failed; see saved evidence')
            complete.extend(result['rows'])
        difference = base.compare(wanted, complete)
        report['results'].append(dict(name=name, method='actual-shard-ranges', rows=complete,
                                     passed=difference is None, difference=difference))
        save()
        if difference:
            raise AssertionError('shard-scoped extraction incomplete: ' + difference)
        all_rows.extend(complete)
    report['raw_rows_verified'] = len(all_rows)
    check_aggregate_contract(all_rows, duck, report, save)
    gates = [r for r in report['results'] if 'passed' in r]
    report.update(completed=True, summary=dict(gates=len(gates), passed=sum(r['passed'] for r in gates)))
    save()
    print(json.dumps(report['summary'], indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-report', required=True)
    parser.add_argument('--shard-manifest', help='optional filtered manifest for this exact fixture bucket')
    parser.add_argument('--local-only', action='store_true', help='test synthetic complete-input SQL only; no Flux or metadata claims')
    parser.add_argument('--influx-url', default='http://127.0.0.1:18096')
    parser.add_argument('--org', default='waggle')
    parser.add_argument('--duckdb', default='duckdb')
    parser.add_argument('--run-dir', required=True)
    args = parser.parse_args()
    try:
        return run(args)
    except Exception as error:
        message = str(error)
        for key in ('INFLUX_TOKEN', 'INFLUX_METADATA_TOKEN'):
            if os.environ.get(key): message = message.replace(os.environ[key], '<redacted>')
        print('exp92 failed: ' + message, file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
