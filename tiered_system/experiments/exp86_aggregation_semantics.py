"""Tiny Flux/DuckDB 2.0 aggregation oracle. No API or warehouse mutations.

Creates one unique Influx fixture bucket and a new local result directory. The
bucket is deliberately retained; no bucket/object/delete endpoint is used.
DuckDB inputs are local synthetic rows, split through a real Parquet hot staging
file. This tests semantics, not Iceberg/Pelican planning or API performance.
"""
import argparse
import csv
from datetime import datetime, timezone
import io
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import uuid

BASE = 1740787200000000000
MINUTE = 60_000_000_000
FUNCTIONS = ('mean', 'min', 'max', 'sum', 'count')
FIXED = {'ns': 1, 'us': 1000, 'ms': 1_000_000, 's': 1_000_000_000,
         'm': MINUTE, 'h': 60 * MINUTE, 'd': 1440 * MINUTE, 'w': 10080 * MINUTE}


DIAGNOSTICS = """SELECT
        time_bucket(INTERVAL '1 week',TIMESTAMP '2025-03-01')::VARCHAR AS default_week,
        time_bucket(INTERVAL '1 week',TIMESTAMP '2025-03-01',TIMESTAMP '1970-01-01')::VARCHAR AS epoch_week,
        try_cast('123'::VARCHAR::VARIANT AS DOUBLE) AS string_numeric_cast,
        try_cast(true::BOOLEAN::VARIANT AS DOUBLE) AS bool_numeric_cast,
        to_json(1::BIGINT::VARIANT)::VARCHAR AS native_variant_json;"""


def ns(text):
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(?:Z|\+00:00)', text)
    if not match:
        raise ValueError('UTC timestamp with at most nine fractional digits required')
    delta = datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + int((match[2] or '').ljust(9, '0'))


def stamp(value):
    return datetime.fromtimestamp(value // 1_000_000_000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') + f'.{value % 1_000_000_000:09d}Z'


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def fixture():
    rows = []
    def add(name, minute, value, kind='float', sensor='a', at=None):
        rows.append(dict(time=BASE + minute * MINUTE if at is None else at,
                         name='exp86.' + name, kind=kind, value=value,
                         meta={'plugin': 'exp86', 'vsn': 'EXP86', 'sensor': sensor}))
    for minute, value in [(1, 10.0), (2, 20.0), (4, 30.0), (8, 100.0)]:
        add('numeric', minute, value)
    add('numeric', 1, 7.0, sensor='b')
    add('numeric', 11, 9.0, sensor='b')
    add('numeric', 2, 5.0, sensor='cold_only')
    add('numeric', 9, 8.0, sensor='hot_only')
    for minute, value in [(1, '123'), (8, 'offline')]: add('text', minute, value, 'string')
    for minute, value in [(1, True), (8, False)]: add('flag', minute, value, 'bool')
    for minute, value in [(1, 9007199254740993), (8, 9007199254740995)]: add('signed', minute, value, 'int')
    for minute, value in [(1, 18446744073709551612), (8, 18446744073709551613)]: add('unsigned', minute, value, 'uint')
    for minute, value in [(1, 9223372036854775807), (8, 1)]: add('overflow', minute, value, 'int')
    for offset, value in [(194, 10.0), (195, 20.0), (199, 30.0)]: add('nano', 0, value, at=BASE + offset)
    for text, value in [('2024-12-31T23:59:59.999999999Z', 1.0),
                        ('2025-01-15T00:00:00Z', 2.0), ('2025-03-01T00:00:00Z', 3.0)]:
        add('calendar', 0, value, at=ns(text))
    for text, value in [('1969-12-31T23:55:00.000000194Z', 4.0), ('1970-01-01T00:01:00.000000195Z', 8.0)]:
        add('negative', 0, value, at=ns(text))
    # Mixed field kinds across shards: oracle captures rejection/coercion policy.
    add('mixed', 0, 2, 'int', at=ns('2025-01-01T00:00:00Z'))
    add('mixed', 0, 3.5, 'float', at=ns('2025-03-01T00:00:00Z'))
    return rows


def cases():
    out = []
    def add(label, name='numeric', fn='mean', window=None, start=BASE,
            end=BASE + 12 * MINUTE, border=BASE + 6 * MINUTE, **extra):
        out.append(dict(id=label, name='exp86.' + name, function=fn,
                        window=window, start=start, end=end, border=border, **extra))
    for fn in FUNCTIONS:
        add(fn + '-whole', fn=fn)
        add(fn + '-5m', fn=fn, window='5m')
        add(fn + '-5m-clipped-ns', fn=fn, window='5m', end=BASE + 12 * MINUTE + 194)
    for name in ('text', 'flag', 'signed', 'unsigned'):
        for fn in FUNCTIONS: add(name + '-' + fn, name=name, fn=fn)
    for fn in FUNCTIONS:
        add('nano-' + fn, name='nano', fn=fn, window='2ns', start=BASE + 193,
            end=BASE + 201, border=BASE + 196)
    add('weekly-origin', window='1w')
    add('seven-minute-origin', window='7m')
    add('fixed-compound', window='1h30m')
    add('pre-epoch', name='negative', window='7m', start=ns('1969-12-31T23:50:00Z'),
        end=ns('1970-01-01T00:10:00.000000194Z'), border=0)
    for window in ('1mo', '1y', '1mo1d'):
        add('calendar-' + window, name='calendar', window=window,
            start=ns('2024-12-20T00:00:00Z'), end=ns('2025-03-15T00:00:00.000000194Z'),
            border=ns('2025-02-01T00:00:00Z'), oracle_only=window == '1mo1d')
    for limit in ('head', 'tail'):
        add(limit + '-before-mean', **{limit: 2})
        add(limit + '-before-window', window='5m', **{limit: 2})
        add(limit + '-zero', **{limit: 0})
    add('no-series', name='absent', fn='count', window='5m')
    add('signed-overflow', name='overflow', fn='sum')
    for fn in ('count', 'mean', 'sum'):
        add('mixed-' + fn, name='mixed', fn=fn, start=ns('2025-01-01T00:00:00Z'),
            end=ns('2025-03-02T00:00:00Z'), border=ns('2025-02-01T00:00:00Z'))
    for fn in ('count', 'mean'):
        add('all-null-' + fn, name='nulls', fn=fn, oracle_only=True, null_fixture=True)
    return out


def line(row):
    head = row['name'] + ',' + ','.join(k + '=' + v for k, v in sorted(row['meta'].items()))
    value = row['value']
    if row['kind'] == 'int': text = str(value) + 'i'
    elif row['kind'] == 'uint': text = str(value) + 'u'
    elif row['kind'] == 'bool': text = 'true' if value else 'false'
    elif row['kind'] == 'string': text = json.dumps(value)
    else: text = repr(value)
    return head + ' value=' + text + ' ' + str(row['time'])


class Influx:
    def __init__(self, url, token, org, bucket):
        parsed = urlsplit(url)
        if parsed.scheme not in ('http', 'https') or not parsed.netloc or parsed.username or parsed.query or parsed.fragment:
            raise ValueError('Influx URL must be an HTTP(S) origin without credentials/query')
        if not re.fullmatch(r'agg-exp(?:86|87)-[a-f0-9]{32}', bucket): raise ValueError('isolated bucket required')
        self.url, self.token, self.org, self.bucket = url.rstrip('/'), token, org, bucket

    def request(self, path, data=None, content='application/json', accept='application/json', capture=None):
        if path.startswith('/api/v2/delete'): raise ValueError('deletion disabled')
        body = None if data is None else (data if isinstance(data, bytes) else json.dumps(data).encode())
        req = Request(self.url + path, body, headers={'Authorization': 'Token ' + self.token,
                      'Content-Type': content, 'Accept': accept})
        try:
            with urlopen(req, timeout=120) as response:
                body = response.read(16 * 1024 * 1024 + 1)
                if len(body) > 16 * 1024 * 1024: raise ValueError('fixture response exceeds limit')
                if capture is not None:
                    capture['content_type'] = response.headers.get('Content-Type', '')
                return body
        except HTTPError as error:
            detail = error.read(8192).decode(errors='replace').replace(self.token, '<redacted>')
            raise RuntimeError(f'Influx HTTP {error.code}: {detail}') from error

    def setup(self, rows):
        orgs = json.loads(self.request('/api/v2/orgs?' + urlencode({'org': self.org})))['orgs']
        if len(orgs) != 1: raise ValueError('exactly one matching organization required')
        bucket = json.loads(self.request('/api/v2/buckets', {'orgID': orgs[0]['id'], 'name': self.bucket,
                                        'retentionRules': []}))
        self.request('/api/v2/write?' + urlencode({'org': self.org, 'bucket': self.bucket, 'precision': 'ns'}),
                     ('\n'.join(map(line, rows)) + '\n').encode(), 'text/plain')
        return bucket['id']

    def query(self, flux):
        # Influx omits #datatype unless the dialect asks for it. The parser needs those types.
        return self.request('/api/v2/query?' + urlencode({'org': self.org}),
                            {'query': flux, 'type': 'flux', 'dialect': {
                                'header': True, 'delimiter': ',', 'commentPrefix': '#',
                                'dateTimeFormat': 'RFC3339',
                                'annotations': ['datatype', 'group', 'default']}},
                            accept='application/csv').decode()


def flux_query(bucket, case):
    source = 'from(bucket: ' + json.dumps(bucket) + ')'
    imports = ''
    if case.get('null_fixture'):
        imports = 'import "array"\nimport "internal/debug"\n'
        source = ('array.from(rows: [{_time: ' + stamp(BASE + MINUTE) + ', _measurement: "exp86.nulls", _field: "value", sensor: "nulls", _value: debug.null(type: "float")},'
                  '{_time: ' + stamp(BASE + 2 * MINUTE) + ', _measurement: "exp86.nulls", _field: "value", sensor: "nulls", _value: debug.null(type: "float")}])')
    parts = [source,
             'range(start: ' + stamp(case['start']) + ', stop: ' + stamp(case['end']) + ')',
             'filter(fn: (r) => r._field == "value" and r._measurement == ' + json.dumps(case['name']) + ')']
    if case.get('null_fixture'): parts.append('group(columns: ["_measurement", "_field", "_start", "_stop", "sensor"])')
    if 'head' in case: parts.append('limit(n: ' + str(case['head']) + ')')
    if 'tail' in case: parts.append('tail(n: ' + str(case['tail']) + ')')
    if case['window']:
        parts.append('aggregateWindow(every: ' + case['window'] + ', fn: ' + case['function'] + ')')
    else: parts.append(case['function'] + '()')
    return imports + ' |> '.join(parts)


def parse_flux(text, windowed):
    types, names, records = [], [], []
    for fields in csv.reader(io.StringIO(text)):
        if not fields: continue
        if fields[0] == '#datatype': types = fields; continue
        if fields[0].startswith('#'): continue
        if 'error' in fields and 'reference' in fields: names = fields; continue
        if '_value' in fields: names = fields; continue
        if not names or len(fields) != len(names): raise ValueError('malformed annotated CSV')
        row = dict(zip(names, fields))
        if 'error' in row: raise ValueError(row['error'])
        datatype = types[names.index('_value')]
        raw = row['_value']
        value = (None if raw == '' else int(raw) if datatype in ('long', 'unsignedLong')
                 else float(raw) if datatype == 'double' else raw == 'true' if datatype == 'boolean' else raw)
        metadata = {k: v for k, v in row.items() if k and k not in ('result', 'table') and not k.startswith('_') and v != ''}
        records.append(dict(time=ns(row['_time' if windowed else '_start']), name=row['_measurement'],
                            meta=metadata, value=value,
                            kind={'long': 'int', 'unsignedLong': 'uint', 'double': 'float', 'boolean': 'bool', 'string': 'string'}[datatype]))
    return records


class Duck:
    def __init__(self, binary, root): self.binary, self.root = binary, root

    def run(self, sql, label):
        path = self.root / (label + '.sql')
        path.write_text(sql)
        started = time.monotonic()
        result = subprocess.run([self.binary, '-batch', '-bail', '-json'], input=sql, text=True,
                                capture_output=True, timeout=120)
        (self.root / (label + '.stdout')).write_text(result.stdout)
        (self.root / (label + '.stderr')).write_text(result.stderr)
        if result.returncode: raise RuntimeError(result.stderr.strip()[-3000:])
        return json.loads(result.stdout or '[]'), time.monotonic() - started

    def version(self):
        rows, _ = self.run('SELECT version() AS version;', 'version')
        version = rows[0]['version']
        if not re.fullmatch(r'v2\.0\.\d+', version): raise ValueError('requires DuckDB 2.0.x; found ' + version)
        return version


def kind_sql(value='value'):
    return 'CASE ' + ' '.join('WHEN variant_typeof(' + value + ') = variant_typeof(' + expression + '::VARIANT) THEN ' + quote(kind)
        for kind, expression in [('int', '1::BIGINT'), ('uint', '1::UBIGINT'), ('float', '1::DOUBLE'),
                                 ('bool', 'true'), ('bool', 'false'), ('string', "'x'::VARCHAR")]) + " ELSE 'other' END"


def source_sql(rows):
    values = []
    types = {'int': 'BIGINT', 'uint': 'UBIGINT', 'float': 'DOUBLE', 'bool': 'BOOLEAN', 'string': 'VARCHAR'}
    for row in rows:
        text = ('true' if row['value'] else 'false') if row['kind'] == 'bool' else str(row['value'])
        meta = 'MAP {' + ','.join(quote(k) + ':' + quote(v) for k, v in sorted(row['meta'].items())) + '}'
        values.append('(' + str(row['time']) + ',' + quote(row['name']) + ',' + meta + ',' +
                      quote(row['kind']) + ',' + quote(text) + ')')
    cast = 'CASE kind ' + ' '.join('WHEN ' + quote(k) + ' THEN CAST(text AS ' + t + ')::VARIANT' for k, t in types.items()) + ' END'
    return ('CREATE TEMP TABLE primitive AS SELECT * FROM (VALUES ' + ','.join(values) +
            ') AS t(time_ns,name,meta,kind,text);\nCREATE TEMP TABLE full_raw AS SELECT make_timestamp_ns(time_ns) AS t,name,meta,' +
            cast + ' AS value FROM primitive;\n')


def month_ns(index):
    year, month = divmod(index, 12)
    return ns(f'{year:04d}-{month + 1:02d}-01T00:00:00Z')


def windows(case):
    window = case['window']
    if window is None: return None, [(0, case['start'])]
    pieces = re.findall(r'(\d+)(ns|us|ms|mo|s|m|h|d|w|y)', window)
    if ''.join(a + b for a, b in pieces) != window: raise ValueError('unsupported experiment interval')
    fixed = sum(int(a) * FIXED[b] for a, b in pieces if b in FIXED)
    months = sum(int(a) * (12 if b == 'y' else 1) for a, b in pieces if b in ('mo', 'y'))
    if months and fixed: raise ValueError('mixed calendar/fixed interval is oracle-only')
    if fixed:
        first, last = case['start'] // fixed, (case['end'] - 1) // fixed
        if last - first > 10000: raise ValueError('fixture expansion too large')
        rows = [(i, min((i + 1) * fixed, case['end'])) for i in range(first, last + 1)]
        expression = f'(epoch_ns(t) // {fixed} - CASE WHEN epoch_ns(t) % {fixed} < 0 THEN 1 ELSE 0 END)'
        return expression, rows
    if months <= 0: raise ValueError('positive interval required')
    a = datetime.fromtimestamp(case['start'] // 1_000_000_000, timezone.utc)
    b = datetime.fromtimestamp((case['end'] - 1) // 1_000_000_000, timezone.utc)
    origin = 1970 * 12
    first, last = (a.year * 12 + a.month - 1 - origin) // months, (b.year * 12 + b.month - 1 - origin) // months
    rows = [(month_ns(origin + i * months), min(month_ns(origin + (i + 1) * months), case['end'])) for i in range(first, last + 1)]
    # DuckDB rounds an implicit TIMESTAMP_NS -> TIMESTAMP conversion. Floor
    # explicitly so the final nanosecond of a month cannot enter the next one.
    floored = 'make_timestamp(epoch_ns(t)//1000 - CASE WHEN epoch_ns(t)%1000<0 THEN 1 ELSE 0 END)'
    return f"epoch_ns(time_bucket(INTERVAL '{months} months',{floored},TIMESTAMP '1970-01-01'))", rows


def flux_integer_value(fn, kind, native):
    """Exact internal accumulation, native 64-bit reduction, then optional mean."""
    sign, modulus = 1 << 63, 1 << 64
    total = "sum(CASE WHEN kind=" + quote(kind) + " THEN value::" + native + "::HUGEINT END)"
    if kind == 'uint': wrapped = f'((({total}) % {modulus}) + {modulus}) % {modulus}'
    else: wrapped = f'((((({total}) + {sign}) % {modulus}) + {modulus}) % {modulus}) - {sign}'
    typed = 'CAST((' + wrapped + ') AS ' + native + ')'
    if fn == 'mean':
        typed += "::DOUBLE / NULLIF(count(CASE WHEN kind=" + quote(kind) + " THEN value END),0)"
    return '(' + typed + ')::VARIANT'


def aggregate_sql(case, source, *, integer_policy='strict'):
    if integer_policy not in ('strict','flux64'): raise ValueError('unknown experiment integer policy')
    fn = case['function']
    if fn not in FUNCTIONS: raise ValueError('unknown experiment function')
    filtered = 'SELECT *, ' + kind_sql() + ' AS kind FROM ' + source + ' WHERE name=' + quote(case['name']) + f" AND epoch_ns(t)>={case['start']} AND epoch_ns(t)<{case['end']}"
    selected = filtered
    if 'head' in case or 'tail' in case:
        limit = case.get('head', case.get('tail'))
        order = 'DESC' if 'tail' in case else 'ASC'
        selected = 'SELECT * EXCLUDE(rn) FROM (SELECT *, row_number() OVER(PARTITION BY name,meta ORDER BY t ' + order + ') AS rn FROM (' + selected + ')) WHERE rn<=' + str(limit)
    bucket, boundaries = windows(case)
    grouped = 'SELECT *, ' + (bucket or '0') + ' AS bucket FROM selected'
    numeric = "kind IN ('int','uint','float')"
    if fn == 'count': value = 'count(*)::BIGINT::VARIANT'
    elif fn == 'mean':
        value = 'avg(CASE WHEN ' + numeric + ' THEN value::DOUBLE END)::VARIANT'
        if integer_policy == 'flux64':
            value = 'CASE min(kind) ' + ' '.join('WHEN ' + quote(k) + ' THEN ' + flux_integer_value(fn,k,t) for k,t in [('int','BIGINT'),('uint','UBIGINT')]) + ' ELSE ' + value + ' END'
    else:
        op = fn
        branches = []
        for kind, native in [('int','BIGINT'),('uint','UBIGINT'),('float','DOUBLE')]:
            expression = 'CAST(' + op + '(CASE WHEN kind=' + quote(kind) + ' THEN value::' + native + ' END) AS ' + native + ')::VARIANT'
            if integer_policy == 'flux64' and fn == 'sum' and kind != 'float': expression = flux_integer_value(fn,kind,native)
            branches.append('WHEN ' + quote(kind) + ' THEN ' + expression)
        value = 'CASE min(kind) ' + ' '.join(branches) + ' END'
    if fn != 'count':
        value = "CASE WHEN count(*) FILTER(WHERE NOT (" + numeric + "))>0 OR count(DISTINCT kind)>1 THEN error('unsupported or mixed aggregate input') ELSE " + value + ' END'
    values = ','.join('(' + str(key) + ',' + str(stop) + ')' for key, stop in boundaries)
    join = ('SELECT s.name,s.meta,w.stop_ns,a.v FROM series s CROSS JOIN boundaries w LEFT JOIN aggregated a ON a.name=s.name AND a.meta=s.meta AND a.bucket=w.bucket')
    if not case['window'] and fn in ('min', 'max'): join += ' WHERE a.n IS NOT NULL'
    if fn == 'count': join = join.replace('w.stop_ns,a.v', 'w.stop_ns,coalesce(a.v,0::BIGINT::VARIANT) AS v')
    serialization = 'CASE ' + kind_sql('v') + ' ' + ' '.join('WHEN ' + quote(k) + ' THEN to_json(v::' + t + ')::VARCHAR' for k, t in [('int', 'BIGINT'), ('uint', 'UBIGINT'), ('float', 'DOUBLE')]) + ' ELSE NULL END'
    return ('WITH filtered AS (' + filtered + '), selected AS (' + selected + '), grouped AS (' + grouped + '), '
            'series AS (SELECT DISTINCT name,meta FROM filtered), '
            'boundaries(bucket,stop_ns) AS (VALUES ' + values + '), '
            'aggregated AS (SELECT name,meta,bucket,count(*) AS n,' + value + ' AS v FROM grouped GROUP BY name,meta,bucket), '
            'output AS (' + join + ') '
            'SELECT stop_ns AS time,name,to_json(meta)::VARCHAR AS meta,' + serialization + ' AS value,' + kind_sql('v') + ' AS kind '
            'FROM output ORDER BY time,name,meta;')


def duck_records(rows):
    return [dict(time=r['time'], name=r['name'], meta=json.loads(r['meta']),
                 value=None if r['value'] is None else json.loads(r['value']), kind=r['kind']) for r in rows]


def compare(expected, actual):
    key = lambda r: (r['time'], r['name'], json.dumps(r['meta'], sort_keys=True))
    expected, actual = sorted(expected, key=key), sorted(actual, key=key)
    if len(expected) != len(actual): return f'row count {len(expected)} != {len(actual)}'
    for a, b in zip(expected, actual):
        if key(a) != key(b): return 'timestamp/name/metadata differs: ' + repr((a, b))
        if a['value'] is None or b['value'] is None:
            if a['value'] != b['value']: return 'null/value differs: ' + repr((a, b))
            continue
        if a['kind'] != b['kind']: return 'value kind differs: ' + repr((a, b))
        if a['kind'] == 'float':
            if not math.isclose(a['value'], b['value'], rel_tol=1e-12, abs_tol=1e-10): return 'float differs: ' + repr((a, b))
        elif a['value'] != b['value']: return 'exact value differs: ' + repr((a, b))
    return None


def placement_sql(rows, case, root, placement):
    setup = "SET TimeZone='UTC'; SET threads=2; SET memory_limit='3GB';\n" + source_sql(rows)
    if placement == 'cold_only': return setup, 'full_raw'
    hot = root / (case['id'] + '-hot.parquet')
    setup += 'COPY (SELECT time_ns,name,map_from_entries(list_reverse(map_entries(meta))) AS meta,kind,text FROM primitive WHERE time_ns>=' + str(max(case['border'],case['start'])) + ' AND time_ns<' + str(case['end']) + ' AND name=' + quote(case['name']) + ') TO ' + quote(hot) + " (FORMAT PARQUET, COMPRESSION ZSTD);\n"
    expr = 'CASE kind ' + ' '.join('WHEN ' + quote(k) + ' THEN CAST(text AS ' + t + ')::VARIANT' for k, t in [('int', 'BIGINT'), ('uint', 'UBIGINT'), ('float', 'DOUBLE'), ('bool', 'BOOLEAN'), ('string', 'VARCHAR')]) + ' END'
    setup += 'CREATE TEMP VIEW combined AS SELECT * FROM full_raw WHERE epoch_ns(t)<' + str(case['border']) + ' UNION ALL SELECT make_timestamp_ns(time_ns),name,map_from_entries(list_sort(map_entries(meta))),' + expr + ' FROM read_parquet(' + quote(hot) + ');\n'
    return setup, 'combined'


def error_category(error):
    text = str(error).lower()
    if any(x in text for x in ('overflow', 'out of range', 'out-of-range')): return 'overflow'
    if any(x in text for x in ('unsupported or mixed aggregate input', 'unsupported input type',
                               'unsupported aggregate column type', 'schema collision', 'type conflict')): return 'input_type'
    return 'unclassified'  # Never count infrastructure/parser/binder failures as parity.


def attempt(fn):
    try: return {'status': 'success', **fn()}
    except Exception as error: return {'status': 'error', 'error_type': type(error).__name__, 'error': str(error)[-3000:], 'error_category': error_category(error)}


def run(args):
    root = Path(args.run_dir).resolve()
    if 'node-data' in root.parts or root.exists(): raise ValueError('new isolated run directory required, outside /node-data')
    root.mkdir(parents=True, mode=0o700)
    root.chmod(0o700)
    duck = Duck(args.duckdb, root)
    version = duck.version()  # Fail before making any Influx changes on wrong engine.
    bucket = 'agg-exp86-' + uuid.uuid4().hex
    client = Influx(args.influx_url, os.environ['INFLUX_TOKEN'], args.org, bucket)
    rows = fixture()
    report = dict(experiment='exp86', duckdb_version=version, bucket=bucket,
                  script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  scope='synthetic Flux vs local VARIANT/Parquet; no API/Iceberg/Pelican execution',
                  fixture_rows=len(rows), results=[])
    save = lambda: (root / 'report.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    save()
    try:
        report['diagnostics'] = duck.run(DIAGNOSTICS, 'diagnostics')[0]
        report['influx_health'] = json.loads(client.request('/health'))
        report['bucket_id'] = client.setup(rows)
        save()
        (root / 'fixture.json').write_text(json.dumps(rows, indent=2) + '\n')
        raw_flux = ('from(bucket: ' + json.dumps(bucket) + ') |> range(start: 1969-12-01T00:00:00Z, stop: 2025-04-01T00:00:00Z) '
                    '|> filter(fn: (r) => r._field == "value" and r._measurement != "exp86.mixed")')
        raw_text = client.query(raw_flux)
        (root / 'raw-source.csv').write_text(raw_text)
        raw = parse_flux(raw_text, True)
        expected = [dict(time=r['time'], name=r['name'], meta=r['meta'], value=r['value'], kind=r['kind']) for r in rows if r['name'] != 'exp86.mixed']
        difference = compare(expected, raw)
        if difference: raise RuntimeError('raw fixture preflight failed: ' + difference)
        report['raw_fixture_verified'] = True
        save()
        for case in cases():
            flux = flux_query(bucket, case)
            (root / (case['id'] + '.flux')).write_text(flux + '\n')
            def oracle():
                started = time.monotonic()
                text = client.query(flux)
                (root / (case['id'] + '.csv')).write_text(text)
                return dict(rows=parse_flux(text, bool(case['window'])), seconds=time.monotonic() - started)
            result = dict(case=case, flux=attempt(oracle), candidates={})
            if not case.get('oracle_only'):
                for placement in ('cold_only', 'split'):
                    setup, source = placement_sql(rows, case, root, placement)
                    def candidate(setup=setup, source=source, placement=placement):
                        output, elapsed = duck.run(setup + aggregate_sql(case, source), case['id'] + '-' + placement)
                        return dict(rows=duck_records(output), seconds=elapsed)
                    observed = attempt(candidate)
                    if observed['status'] != result['flux']['status']:
                        observed['parity'] = 'FAIL'
                        observed['difference'] = 'success/error differs from Flux'
                    elif observed['status'] == 'error':
                        category = observed['error_category']
                        observed['parity'] = 'BOTH_ERROR' if category != 'unclassified' and category == result['flux']['error_category'] else 'FAIL'
                        observed['difference'] = 'Error categories compared; exact API status/message compatibility not established'
                    else:
                        difference = compare(result['flux']['rows'], observed['rows'])
                        observed['parity'] = 'FAIL' if difference else 'PASS'
                        if difference: observed['difference'] = difference
                    result['candidates'][placement] = observed
            else: result['note'] = 'Oracle-only: null-input contract or compound calendar algorithm not implemented in candidate'
            report['results'].append(result)
            save()
            print(case['id'], {k: v['parity'] for k, v in result['candidates'].items()}, flush=True)
        report['summary'] = {status: sum(c.get('parity') == status for r in report['results'] for c in r['candidates'].values()) for status in ('PASS', 'FAIL', 'BOTH_ERROR')}
        report['completed'] = True
        save()
        return 1 if report['summary']['FAIL'] else 0
    except BaseException:
        report['completed'] = False
        save()
        raise


def replay_report(args):
    """Re-execute candidate SQL against an immutable recorded Flux oracle."""
    source_path = Path(args.replay_report).resolve()
    root = Path(args.run_dir).resolve()
    if 'node-data' in source_path.parts or 'node-data' in root.parts or root.exists():
        raise ValueError('isolated source and new result directory required')
    raw = source_path.read_bytes()
    reference = json.loads(raw)
    if reference.get('experiment') != 'exp86' or not reference.get('completed') or not reference.get('raw_fixture_verified'):
        raise ValueError('completed exp86 Flux report with verified raw fixture required')
    fixture_path = source_path.with_name('fixture.json')
    fixture_bytes = fixture_path.read_bytes()
    rows = json.loads(fixture_bytes)
    for row in rows:
        if type(row['time']) is not int: raise ValueError('integer fixture time required')
    for entry in reference['results']:
        case = entry['case']
        if not re.fullmatch(r'[A-Za-z0-9_-]+', case['id']): raise ValueError('invalid case id')
        for key in ('start', 'end', 'border', 'head', 'tail'):
            if key in case and type(case[key]) is not int: raise ValueError('integer case bound/limit required')
    root.mkdir(parents=True, mode=0o700)
    duck = Duck(args.duckdb, root)
    version = duck.version()
    report = dict(experiment='exp86-replay', duckdb_version=version,
        source_report=str(source_path), source_report_sha256=hashlib.sha256(raw).hexdigest(),
        fixture_sha256=hashlib.sha256(fixture_bytes).hexdigest(),
        script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope='New candidate SQL vs recorded Flux oracle; no live Influx/API/Iceberg/Pelican execution',
        results=[])
    save = lambda: (root / 'report.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    save()
    for entry in reference['results']:
        case, oracle = entry['case'], entry['flux']
        result = dict(case=case, flux=oracle, candidates={})
        if not case.get('oracle_only'):
            for placement in ('cold_only', 'split'):
                setup, source = placement_sql(rows, case, root, placement)
                def query():
                    output, elapsed = duck.run(setup + aggregate_sql(case, source), case['id'] + '-' + placement)
                    return dict(rows=duck_records(output), seconds=elapsed)
                observed = attempt(query)
                if observed['status'] != oracle['status']:
                    observed.update(parity='FAIL', difference='success/error differs from recorded Flux')
                elif observed['status'] == 'error':
                    category = observed['error_category']
                    matched = category != 'unclassified' and category == oracle['error_category']
                    observed.update(parity='BOTH_ERROR' if matched else 'FAIL',
                        difference='Error categories compared; exact API status/message compatibility not established')
                else:
                    difference = compare(oracle['rows'], observed['rows'])
                    observed['parity'] = 'FAIL' if difference else 'PASS'
                    if difference: observed['difference'] = difference
                result['candidates'][placement] = observed
        else: result['note'] = 'Oracle-only observation retained unchanged'
        report['results'].append(result)
        save()
    report['summary'] = {status: sum(c.get('parity') == status for r in report['results'] for c in r['candidates'].values()) for status in ('PASS', 'FAIL', 'BOTH_ERROR')}
    report['completed'] = True
    save()
    print(json.dumps(report['summary'], indent=2))
    return 1 if report['summary']['FAIL'] else 0


def local_smoke(args):
    """Candidate self-consistency only; explicitly permits a non-2.0 engine."""
    root = Path(args.run_dir).resolve()
    if 'node-data' in root.parts or root.exists(): raise ValueError('new isolated directory required')
    root.mkdir(parents=True, mode=0o700)
    duck = Duck(args.duckdb, root)
    version = duck.run('SELECT version() AS version;', 'version')[0][0]['version']
    report = dict(experiment='exp86-local-smoke', duckdb_version=version,
                  script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  scope='Candidate self-consistency only. NO Flux/API/Iceberg/Pelican validation. Not DuckDB 2.0 evidence unless version says 2.0.',
                  results=[], controls=[])
    failed = False
    for case in cases():
        if case.get('oracle_only'): continue
        observed = {}
        for placement in ('cold_only', 'split'):
            setup, source = placement_sql(fixture(), case, root, placement)
            def query(setup=setup, source=source, placement=placement):
                rows, elapsed = duck.run(setup + aggregate_sql(case, source), case['id'] + '-' + placement)
                return dict(rows=duck_records(rows), seconds=elapsed)
            observed[placement] = attempt(query)
        a, b = observed['cold_only'], observed['split']
        if a['status'] != b['status']: difference = 'success/error differs'
        elif a['status'] == 'error': difference = None if a['error_category'] == b['error_category'] and a['error_category'] != 'unclassified' else 'unclassified candidate failure'
        else: difference = compare(a['rows'], b['rows'])
        failed |= difference is not None
        report['results'].append(dict(case=case, candidates=observed, difference=difference))
    expected = {'mean-whole': 40.0, 'sum-whole': 160.0, 'count-whole': 4,
                'min-whole': 10.0, 'max-whole': 100.0, 'unsigned-max': 18446744073709551613,
                'text-count': 2, 'flag-count': 2}
    for label, value in expected.items():
        result = next(r for r in report['results'] if r['case']['id'] == label)['candidates']['split']
        rows = result.get('rows', [])
        a = next((r for r in rows if r['meta'].get('sensor') == 'a'), {})
        passed = a.get('value') == value
        failed |= not passed
        report['controls'].append(dict(case=label, expected=value, actual=a.get('value'), passed=passed))
    report['diagnostics'] = duck.run(DIAGNOSTICS, 'diagnostics')[0]
    report['summary'] = dict(cases=len(report['results']), manual_controls=len(report['controls']),
                             failures=sum(r['difference'] is not None for r in report['results']), passed=not failed)
    (root / 'report.json').write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    print(json.dumps(dict(version=version, summary=report['summary'], diagnostics=report['diagnostics']), indent=2))
    return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--replay-report', help='replay saved Flux oracle against new candidate SQL; no Influx access')
    parser.add_argument('--local-smoke', action='store_true', help='candidate-only self-consistency; no Flux and no engine-2.0 claim')
    parser.add_argument('--duckdb', default='duckdb', help='native DuckDB 2.0.x CLI')
    parser.add_argument('--influx-url', default=os.getenv('INFLUX_URL', 'http://localhost:18086'))
    parser.add_argument('--org', default='waggle')
    parser.add_argument('--run-dir', required=True, help='new private directory; never reused or cleaned implicitly')
    args = parser.parse_args()
    if args.local_smoke and args.replay_report: parser.error("choose local-smoke or replay-report, not both")
    if args.replay_report: raise SystemExit(replay_report(args))
    if args.local_smoke: raise SystemExit(local_smoke(args))
    if not os.getenv('INFLUX_TOKEN'): parser.error('INFLUX_TOKEN is required and must permit fixture bucket creation')
    raise SystemExit(run(args))


if __name__ == '__main__': main()
