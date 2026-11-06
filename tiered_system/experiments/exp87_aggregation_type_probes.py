"""Read-only old-fixture queries + new tiny chronology fixture + pure Flux arrays.

No production API, Iceberg, Pelican, /node-data, or delete operations. Run with
stdlib Python and a native DuckDB 2.0.x CLI. Fixture data and all evidence remain.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import URLError
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import exp86_aggregation_semantics as base

START = base.ns('2025-01-01T00:00:00Z')
SPLIT = base.ns('2025-02-01T00:00:00Z')
STOP = base.ns('2025-03-02T00:00:00Z')
GROUP = '["_measurement", "_field", "_start", "_stop", "plugin", "vsn", "sensor"]'


class ReadOnlyInflux(base.Influx):
    def request(self, path, data=None, content='application/json', accept='application/json'):
        allowed = (data is None and (path == '/health' or path.startswith('/api/v2/buckets/')))
        allowed |= path.startswith('/api/v2/query?') and isinstance(data, dict)
        if not allowed: raise ValueError('source client is read-only')
        return super().request(path, data, content, accept)


def arithmetic_cases():
    return [dict(id='unsigned-large', kind='uint', values=[18446744073709551612, 18446744073709551613]),
            dict(id='unsigned-small', kind='uint', values=[2, 6]),
            dict(id='signed-overflow', kind='int', values=[9223372036854775807, 1]),
            dict(id='signed-underflow', kind='int', values=[-9223372036854775808, -1]),
            dict(id='signed-small', kind='int', values=[-4, 10])]


def variants():
    rows = []
    meta = {'plugin': 'exp87', 'vsn': 'EXP87', 'sensor': 'mixed'}
    january, march = START + 194, base.ns('2025-03-01T00:00:00Z') + 195
    for name, points in [
        ('int-then-float', [(january, 'int', 2), (march, 'float', 3.5)]),
        ('float-then-int', [(january, 'float', 2.5), (march, 'int', 3)]),
        # Same temporal types as the first series, opposite insertion order.
        ('late-written-first', [(march, 'float', 3.5), (january, 'int', 2)])]:
        for stamp, kind, value in points:
            rows.append(dict(time=stamp, name='exp87.' + name, meta=meta, kind=kind, value=value))
    for case in arithmetic_cases():
        for minute, value in zip((1, 8), case['values']):
            rows.append(dict(time=base.BASE + minute * base.MINUTE + 194, name='exp87.' + case['id'],
                meta={'plugin': 'exp87', 'vsn': 'EXP87', 'sensor': 'arithmetic'}, kind=case['kind'], value=value))
    return rows


def range_filter(bucket, measurement, start, stop):
    return ('from(bucket: ' + json.dumps(bucket) + ') |> range(start: ' + base.stamp(start) +
            ', stop: ' + base.stamp(stop) + ') |> filter(fn: (r) => r._field == "value" and r._measurement == ' +
            json.dumps(measurement) + ')')


def array_source(case, floats=False):
    rows = []
    cast = 'float' if floats else 'uint' if case['kind'] == 'uint' else 'int'
    for minute, value in zip((1, 8), case['values']):
        rows.append('{_time: ' + base.stamp(base.BASE + minute * base.MINUTE + 194) +
            ', _measurement: ' + json.dumps('exp87.' + case['id']) + ', _field: "value", plugin: "exp87", vsn: "EXP87", sensor: "arithmetic", _value: ' + cast + '(v: ' + json.dumps(str(value)) + ')}')
    return ('import "array"\narray.from(rows: [' + ','.join(rows) + ']) |> range(start: ' +
        base.stamp(base.BASE) + ', stop: ' + base.stamp(base.BASE + 12 * base.MINUTE) +
        ') |> group(columns: ' + GROUP + ')')


def mixed_array_source(first_kind, raw=False):
    integer = '{_time: 2025-01-01T00:00:00.000000194Z, _measurement: "exp87.array-mixed", _field: "value", plugin: "exp87", vsn: "EXP87", sensor: "mixed", _value: int(v: "2")}'
    floating = '{_time: 2025-03-01T00:00:00.000000195Z, _measurement: "exp87.array-mixed", _field: "value", plugin: "exp87", vsn: "EXP87", sensor: "mixed", _value: float(v: "3.5")}'
    a, b = (integer, floating) if first_kind == 'int' else (floating, integer)
    value = ('import "array"\na = array.from(rows: [' + a + '])\nb = array.from(rows: [' + b + '])\n'
        'union(tables: [a,b]) |> range(start: ' + base.stamp(START) + ', stop: ' + base.stamp(STOP) + ')')
    # Raw union deliberately leaves separate schemas intact. Grouping tests
    # whether Flux rejects/reconciles those kinds outside the storage cursor.
    return value if raw else value + ' |> group(columns: ' + GROUP + ')'


def probes(source_bucket, variant_bucket):
    queries = []
    def add(label, flux_source, fn=None, window=None, **info):
        flux = flux_source
        if fn: flux += (' |> aggregateWindow(every: ' + window + ', fn: ' + fn + ')') if window else ' |> ' + fn + '()'
        queries.append(dict(id=label, flux=flux, parse_time=fn is None or window is not None,
                            function=fn, window=window, **info))
    series = [('existing-mixed', source_bucket, 'exp86.mixed')]
    if variant_bucket:
        series += [(name, variant_bucket, 'exp87.' + name) for name in
                   ('int-then-float', 'float-then-int', 'late-written-first')]
    for label, bucket, measurement in series:
        for part, start, stop in [('whole', START, STOP), ('january', START, SPLIT), ('march', SPLIT, STOP)]:
            source = range_filter(bucket, measurement, start, stop)
            add(label + '-' + part + '-raw', source, family='mixed', source='storage', part=part)
            for fn in ('count', 'mean', 'sum'):
                add(label + '-' + part + '-' + fn, source, fn, family='mixed', source='storage', part=part)
        whole = range_filter(bucket, measurement, START, STOP)
        for suffix, transform in [('to-float', ' |> toFloat()'),
                                  ('map-float', ' |> map(fn: (r) => ({r with _value: float(v: r._value)}))')]:
            for fn in (None, 'count', 'mean', 'sum'):
                add(label + '-' + suffix + '-' + (fn or 'raw'), whole + transform, fn,
                    family='mixed', source='storage-converted', transformation=suffix)
        # 1y keeps both observations in the same logical aggregate window.
        for fn in ('count', 'mean', 'sum'):
            add(label + '-window-' + fn, whole, fn, '1y', family='mixed', source='storage')
    for order in ('int', 'float'):
        add('array-mixed-' + order + '-raw', mixed_array_source(order, True), family='mixed', source='array-union')
        for fn in ('count', 'mean', 'sum'):
            add('array-mixed-' + order + '-' + fn, mixed_array_source(order), fn, family='mixed', source='array-union')
            add('array-mixed-' + order + '-converted-' + fn, mixed_array_source(order, True) + ' |> toFloat() |> group(columns: ' + GROUP + ')', fn,
                family='mixed', source='array-union-converted')
    for case in arithmetic_cases():
        for source_kind in ('array-native', 'array-float', 'storage-native', 'storage-map-native', 'storage-float'):
            if source_kind.startswith('storage'):
                if not variant_bucket: continue
                source = range_filter(variant_bucket, 'exp87.' + case['id'], base.BASE, base.BASE + 12 * base.MINUTE)
                if source_kind == 'storage-float': source += ' |> toFloat()'
                if source_kind == 'storage-map-native': source += ' |> map(fn: (r) => ({r with _value: r._value}))'
            else: source = array_source(case, source_kind == 'array-float')
            for fn in ('count', 'sum', 'mean'):
                for window in (None, '5m', '15m'):
                    add(case['id'] + '-' + source_kind + '-' + fn + '-' + (window or 'whole'),
                        source, fn, window, family='arithmetic', source=source_kind, arithmetic_case=case['id'])
    return queries


def duck_checks(duck):
    reports = []
    modulus, sign = 1 << 64, 1 << 63
    for case in arithmetic_cases():
        values = case['values']
        native = 'UBIGINT' if case['kind'] == 'uint' else 'BIGINT'
        literal = ','.join('(' + base.quote(value) + '::' + native + ')' for value in values)
        unsigned = f'((wide_sum % {modulus}) + {modulus}) % {modulus}'
        signed = f'((((wide_sum + {sign}) % {modulus}) + {modulus}) % {modulus}) - {sign}'
        wrapped = unsigned if case['kind'] == 'uint' else signed
        sql = ('WITH input(value) AS (VALUES ' + literal + '), totals AS (SELECT sum(value::HUGEINT) AS wide_sum,count(*) AS n,avg(value::DOUBLE) AS mathematical_mean FROM input) '
            'SELECT wide_sum::VARCHAR AS exact_sum,n,mathematical_mean,' +
            '(' + wrapped + ')::VARCHAR AS wrapped_sum,(' + wrapped + ')::DOUBLE/n AS wrapped_accumulator_mean FROM totals;')
        output, seconds = duck.run(sql, case['id'] + '-arithmetic')
        wide = sum(values)
        expected_wrap = wide % modulus if case['kind'] == 'uint' else (wide + sign) % modulus - sign
        observed = output[0]
        if int(observed['exact_sum']) != wide or int(observed['wrapped_sum']) != expected_wrap or observed['n'] != len(values):
            raise AssertionError('DuckDB/Python exact arithmetic differs: ' + case['id'])
        reports.append(dict(case=case, observed=observed, seconds=seconds,
                            python_exact_sum=str(wide), python_wrapped_sum=str(expected_wrap), passed=True))
    return reports


def execute_probe(client, query, root):
    (root / (query['id'] + '.flux')).write_text(query['flux'] + '\n')
    started = time.monotonic()
    try:
        text = client.query(query['flux'])
        (root / (query['id'] + '.csv')).write_text(text)
        records = base.parse_flux(text, query['parse_time'])
        return dict(status='success', rows=records, seconds=time.monotonic() - started)
    except Exception as error:
        message = str(error)
        if isinstance(error, (URLError, TimeoutError, OSError)) or re.search(r'Influx HTTP (401|403|404|429|502|503|504):', message):
            raise  # Infrastructure/auth failures are not arithmetic observations.
        if not isinstance(error, (RuntimeError, ValueError)) or message.startswith('malformed annotated CSV'):
            raise
        # Influx.query already redacts its token. Do not persist headers/env.
        return dict(status='error', error_type=type(error).__name__, error=message[-4000:],
                    error_category=base.error_category(error), seconds=time.monotonic() - started)


def run(args):
    root = Path(args.run_dir).resolve()
    if root.exists() or 'node-data' in root.parts: raise ValueError('new isolated run directory required')
    source_path = Path(args.source_report).resolve() if args.source_report else None
    if source_path and 'node-data' in source_path.parts: raise ValueError('/node-data is outside experiment scope')
    reference = None
    if source_path:
        reference = json.loads(source_path.read_text())
        if reference.get('experiment') != 'exp86' or not reference.get('completed') or not reference.get('raw_fixture_verified'):
            raise ValueError('completed original Flux exp86 report required')
        if not re.fullmatch(r'agg-exp86-[a-f0-9]{32}', reference.get('bucket', '')): raise ValueError('isolated source bucket required')
    if not args.local_only and (not reference or not os.getenv('INFLUX_TOKEN')):
        raise ValueError('live mode requires --source-report and INFLUX_TOKEN')
    root.mkdir(parents=True, mode=0o700)
    duck = base.Duck(args.duckdb, root)
    version = duck.version()  # Target version enforced before any new bucket.
    report = dict(experiment='exp87', duckdb_version=version, completed=False,
        scope='Synthetic arithmetic/storage probes only; no API/Iceberg/Pelican/deletion',
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), results=[])
    if source_path:
        report.update(source_report_sha256=hashlib.sha256(source_path.read_bytes()).hexdigest(), source_bucket=reference['bucket'])
    save = lambda: (root / 'report.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    save()
    try:
        report['duckdb_checks'] = duck_checks(duck)
        if args.local_only:
            report['scope'] = 'DuckDB/Python arithmetic only; no Flux/network/variant bucket operations'
            report['live_flux_executed'] = False
        else:
            token = os.environ['INFLUX_TOKEN']
            reader = ReadOnlyInflux(args.influx_url, token, args.org, reference['bucket'])
            report['influx_health'] = json.loads(reader.request('/health'))
            client = reader
            variant_bucket = None
            if not args.read_only:
                variant_bucket = 'agg-exp87-' + uuid.uuid4().hex
                client = base.Influx(args.influx_url, token, args.org, variant_bucket)
                data = variants()
                report.update(variant_bucket=variant_bucket, expected_variant_rows=len(data))
                save()  # Record identity before external mutation.
                (root / 'variant-fixture.json').write_text(json.dumps(data, indent=2) + '\n')
                report['variant_bucket_id'] = client.setup(data)
            queries = probes(reference['bucket'], variant_bucket)
            for query in queries:
                # Both clients query only; the original fixture client cannot write.
                result = execute_probe(reader, query, root)
                report['results'].append(dict(probe={k:v for k,v in query.items() if k!='flux'}, result=result))
                save()
                print(query['id'], result['status'], flush=True)
            report['live_flux_executed'] = True
            report['summary'] = {status:sum(r['result']['status']==status for r in report['results']) for status in ('success','error')}
            report['summary']['probes'] = len(queries)
        report['completed'] = True
        save()
        print('Report:',root / 'report.json')
        return 0  # Captured errors are observations, not automatically parity failures.
    except BaseException:
        save()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', default='duckdb')
    parser.add_argument('--influx-url', default=os.getenv('INFLUX_URL','http://127.0.0.1:18096'))
    parser.add_argument('--org', default='waggle')
    parser.add_argument('--source-report', help='completed exp86 Flux report identifying the existing isolated bucket')
    parser.add_argument('--run-dir', required=True)
    parser.add_argument('--local-only', action='store_true', help='DuckDB/Python arithmetic only; no HTTP access')
    parser.add_argument('--read-only', action='store_true', help='skip new chronology bucket; existing fixture reads and Flux arrays only')
    args = parser.parse_args()
    if args.local_only and args.read_only: parser.error('local-only and read-only are separate modes')
    raise SystemExit(run(args))


if __name__ == '__main__': main()
