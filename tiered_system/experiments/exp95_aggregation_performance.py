"""Read-only historical aggregation benchmark of dedicated cold-only/split APIs.

No seeding, deletion, container changes or storage writes. Both APIs must use
one image and the adopted immutable warehouse; the split Influx bucket must
already contain the hot slice of the same data. Full responses are retained.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.ipc as ipc

from exp70_v3_live_baseline import timestamp_ns
from exp73_plugin_live_compare import receive, public_schema
from exp75_paired_live_profiles import fresh_profile, profile_paths
from exp90_aggregation_client import compare_rows

WAREHOUSE = 'https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse'
BORDER = '2025-03-01T12:00:00Z'
SETTINGS = ('COLD_FRONTIER', 'COLD_BORDER', 'COLD_WAREHOUSE', 'COLD_ASYNC_THREADS',
            'COLD_READ_AHEAD_DEPTH', 'ARROW_COMPRESSION', 'COLD_PROFILE_DIR',
            'COLD_AGGREGATE_MAX_STAGING_BYTES', 'INFLUXDB_BUCKET')


def cases():
    series = {'name': 'wxt.env.temp', 'vsn': 'W0A0'}
    def query(start, end, filters, func, window=None):
        q = dict(start=start, end=end, filter=filters, experimental_func=func)
        if window: q['experimental_window'] = window
        return q
    result = {}
    for func in ('count', 'mean', 'sum', 'min', 'max'):
        result['series_day_' + func] = query('2025-03-01T00:00:00Z', '2025-03-02T00:00:00Z', series, func, '5m')
    result['series_hour_mean'] = query('2025-03-01T11:30:00Z', '2025-03-01T12:30:00Z', series, 'mean', '1m')
    result['fleet_minute_count'] = query('2025-03-01T11:59:30Z', '2025-03-01T12:00:30Z', {}, 'count')
    result['temperature_fleet_hour_mean'] = query('2025-03-01T11:30:00Z', '2025-03-01T12:30:00Z', {'name': 'wxt.env.temp'}, 'mean', '5m')
    result['series_month_mean'] = query('2025-03-01T00:00:00Z', '2025-04-01T00:00:00Z', series, 'mean', '1h')
    return result


def command(args):
    proc = subprocess.run(args, capture_output=True, text=True, timeout=30)
    if proc.returncode: raise ValueError('read-only container inspection failed: ' + proc.stderr.strip())
    return proc.stdout


def runtime(container):
    raw = json.loads(command(['docker', 'inspect', container]))[0]
    env = dict(x.split('=', 1) for x in raw['Config']['Env'] if '=' in x)
    # Never persist the whole environment: it contains credentials.
    return dict(id=raw['Id'], image=raw['Image'], running=raw['State']['Running'],
                restarts=raw['RestartCount'], oom_killed=raw['State']['OOMKilled'],
                memory_limit=raw['HostConfig']['Memory'], nano_cpus=raw['HostConfig'].get('NanoCpus'),
                settings={k: env.get(k, '') for k in SETTINGS},
                memory_events=dict((k, int(v)) for k, v in (line.split() for line in command(
                    ['docker', 'exec', container, 'cat', '/sys/fs/cgroup/memory.events']).splitlines())))


def validate_runtime(states, warehouse, profiled):
    for role, state in states.items():
        env = state['settings']
        if not state['running'] or state['oom_killed']: raise ValueError(role + ': unhealthy API')
        if env['COLD_FRONTIER'] != 'static' or env['COLD_WAREHOUSE'].rstrip('/') != warehouse.rstrip('/'):
            raise ValueError(role + ': use static routing and the selected warehouse')
        expected = BORDER if role == 'split' else '2025-05-01T00:00:00Z'
        if env['COLD_BORDER'] != expected: raise ValueError(role + ': unexpected cold border')
        if role == 'split' and not env['INFLUXDB_BUCKET']: raise ValueError('historical hot bucket must be configured')
        if env['ARROW_COMPRESSION'] not in ('', 'zstd'): raise ValueError('Zstd Arrow required')
        if bool(env['COLD_PROFILE_DIR']) != profiled: raise ValueError('profile setting does not match phase')
    a, b = states['cold'], states['split']
    if a['id'] == b['id'] or a['image'] != b['image']: raise ValueError('use distinct APIs built from the same image')
    for key in ('memory_limit', 'nano_cpus'):
        if a[key] != b[key]: raise ValueError('API resource limits differ')
    for key in ('COLD_ASYNC_THREADS', 'COLD_READ_AHEAD_DEPTH', 'ARROW_COMPRESSION', 'COLD_AGGREGATE_MAX_STAGING_BYTES'):
        if a['settings'][key] != b['settings'][key]: raise ValueError('API execution settings differ: ' + key)


def stable(before, after):
    for role in before:
        a, b = before[role], after[role]
        if any(a[k] != b[k] for k in ('id', 'image', 'restarts', 'settings', 'memory_limit', 'nano_cpus')):
            raise ValueError(role + ': runtime changed')
        if not b['running'] or b['oom_killed']: raise ValueError(role + ': API stopped or OOM-killed')
        if any(b['memory_events'].get(k, 0) > a['memory_events'].get(k, 0) for k in ('oom', 'oom_kill', 'oom_group_kill')):
            raise ValueError(role + ': OOM event increased')


def records(path, fmt):
    if fmt == 'ndjson':
        rows = []
        for line in path.read_text().splitlines():
            r = json.loads(line)
            if set(r) != {'timestamp', 'name', 'value', 'meta'}: raise ValueError('NDJSON schema changed')
            rows.append(dict(time=timestamp_ns(r['timestamp']), name=r['name'], value=r['value'], meta=r['meta']))
        return rows
    rows = []
    with path.open('rb') as stream, ipc.open_stream(stream) as reader:
        public_schema(reader.schema)
        for batch in reader:
            cols = [batch.column(0).cast(pa.int64()).to_pylist()] + [batch.column(i).to_pylist() for i in range(1, 4)]
            for stamp, name, value, meta in zip(*cols):
                rows.append(dict(time=stamp, name=name, value=None if value is None else json.loads(value), meta=json.loads(meta)))
    return rows


def validate_rows(query, rows):
    if not rows: raise ValueError('empty historical response; check fixture coverage')
    start, end = timestamp_ns(query['start']), timestamp_ns(query['end'])
    identities = set()
    for r in rows:
        key = (r['time'], r['name'], json.dumps(r['meta'], sort_keys=True))
        if key in identities: raise ValueError('duplicate aggregate identity')
        identities.add(key)
        if not start <= r['time'] <= end: raise ValueError('aggregate label outside query')
        if query['experimental_func'] == 'count' and (type(r['value']) is not int or r['value'] < 0):
            raise ValueError('count must remain an exact nonnegative integer')


def medians(items):
    keys = set.intersection(*(set(x) for x in items))
    return {k: statistics.median(x[k] for x in items) for k in sorted(keys)
            if all(type(x[k]) in (int, float) for x in items)}


def run(a):
    root = a.run_dir.resolve()
    if root.exists() or 'node-data' in root.parts: raise ValueError('new isolated run directory required')
    for endpoint in (a.cold, a.split):
        u = urlsplit(endpoint)
        if u.scheme != 'http' or u.hostname not in ('localhost', '127.0.0.1', '::1') or u.username or u.password or u.query or u.fragment:
            raise ValueError('dedicated loopback API endpoints required')
    if a.cold == a.split: raise ValueError('distinct API endpoints required')
    if a.reps < 1 or a.timeout < 1: raise ValueError('positive repetitions/timeout required')
    if bool(a.cold_profiles) != bool(a.split_profiles): raise ValueError('both profile mounts required')
    profiled = bool(a.cold_profiles)
    if profiled and not a.control: raise ValueError('diagnostic profiles require an unprofiled control')
    if a.control and not profiled: raise ValueError('--control is for the profiled phase')
    dirs = dict(cold=a.cold_profiles, split=a.split_profiles)
    if profiled and (any(not p.is_dir() or 'node-data' in p.resolve().parts for p in dirs.values()) or dirs['cold'].resolve() == dirs['split'].resolve()):
        raise ValueError('existing separate isolated profile mounts required')
    selected = list(dict.fromkeys(a.case or cases()))
    containers = dict(cold=a.cold_container, split=a.split_container)
    before = {role: runtime(c) for role, c in containers.items()}
    validate_runtime(before, a.warehouse, profiled)
    control = json.loads(a.control.read_text()) if a.control else None
    if control:
        if control['status'] != 'complete' or control['profiled'] or control['selected_cases'] != selected:
            raise ValueError('matching completed unprofiled control required')
        if any(r['query'] != cases()[r['case']] for r in control['results']):
            raise ValueError('control queries differ from this workload')
        for role in before:
            old, new = control['runtime_before'][role], before[role]
            if any(old[k] != new[k] for k in ('image', 'memory_limit', 'nano_cpus')):
                raise ValueError('control image/resources differ')
            if {k:v for k,v in old['settings'].items() if k != 'COLD_PROFILE_DIR'} != {k:v for k,v in new['settings'].items() if k != 'COLD_PROFILE_DIR'}:
                raise ValueError('control execution configuration differs')
    root.mkdir(parents=True, mode=0o700)
    report = dict(experiment='exp95', status='running', profiled=profiled, selected_cases=selected,
                  runtime_before=before, script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  control=str(a.control) if a.control else None, results=[], summaries=[],
                  notes=['One excluded warmup per case/placement/format; sequential requests with rotated order; caches uncontrolled.',
                         'Cold-only responses are the historical reference; float tolerance rel=1e-12 abs=1e-10, integers/timestamps exact.',
                         'Month case is cold-only: split would require the entire remaining month in Influx.',
                         'HTTP latency includes wire parsing/spooling, excludes semantic validation; not client dataframe latency.',
                         'Profiles are Arrow-only diagnostics, separate from primary unprofiled Arrow/NDJSON latency.',
                         'Hot read/stage includes CSV decoding, validation, NDJSON encoding and source closure; not pure Influx server time.',
                         'Existing warehouse is read-only; older unmarked uint types cannot be restored by this benchmark.'])
    def save():
        tmp = root / 'report.pending'
        tmp.write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
        tmp.replace(root / 'report.json')
    save()
    try:
        endpoints = dict(cold=a.cold, split=a.split)
        references = {}
        for name in selected:
            q = cases()[name]
            roles = ('cold',) if name == 'series_month_mean' else ('cold', 'split')
            formats = ('arrow',) if profiled else ('arrow', 'ndjson')
            for rep in range(a.reps + 1):
                work = [(role, fmt) for role in roles for fmt in formats]
                offset = rep % len(work)
                for role, fmt in work[offset:] + work[:offset]:
                    path = root / f'{name}-{role}-{rep}.{fmt}'
                    paths = profile_paths(dirs[role]) if profiled else None
                    response = receive(endpoints[role], q, fmt, path, a.timeout)
                    profile = fresh_profile(dirs[role], paths) if profiled else None
                    rows = records(path, fmt)
                    validate_rows(q, rows)
                    if response['rows'] != len(rows): raise ValueError('wire/decoded row count differs')
                    if name not in references:
                        if role != 'cold': raise ValueError('cold reference must run first')
                        references[name] = rows
                    differences, _ = compare_rows(references[name], rows)
                    if differences: raise ValueError(name + ': historical parity failed: ' + str(differences[:3]))
                    if control:
                        prior = next(r for r in control['results'] if r['case'] == name and r['placement'] == 'cold' and r['format'] == 'arrow')
                        prior_path = a.control.parent / prior['body']
                        if hashlib.sha256(prior_path.read_bytes()).hexdigest() != prior['body_sha256']: raise ValueError('control body changed')
                        diff, _ = compare_rows(records(prior_path, 'arrow'), rows)
                        if diff: raise ValueError('profiled response differs from unprofiled control')
                    if profile:
                        if profile['rows'] != len(rows) or not profile.get('engine_version', '').startswith('v2.0.'):
                            raise ValueError('profile row count/engine differs')
                        if role == 'split' and profile['phases'].get('aggregate_hot_rows', 0) <= 0:
                            raise ValueError('split did not stage any hot input')
                    report['results'].append(dict(case=name, query=q, placement=role, format=fmt, repetition=rep,
                        warmup=rep == 0, response=response, profile=profile, body=path.name,
                        body_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), parity_passed=True))
                    save()
                    print(name, role, fmt, 'warmup' if not rep else rep, f"{response['total_seconds']:.3f}s", flush=True)
        after = {role: runtime(c) for role, c in containers.items()}
        report['runtime_after'] = after
        stable(before, after)
        groups = Counter((r['case'], r['placement'], r['format']) for r in report['results'] if not r['warmup'])
        for (name, role, fmt), _ in groups.items():
            runs = [r for r in report['results'] if (r['case'], r['placement'], r['format']) == (name, role, fmt) and not r['warmup']]
            s = dict(case=name, placement=role, format=fmt, response=medians([r['response'] for r in runs]))
            if profiled:
                s.update(phases=medians([r['profile']['phases'] for r in runs]),
                         operator_work=medians([r['profile']['duckdb']['operator_work_seconds'] for r in runs]),
                         engine_metrics=medians([r['profile']['duckdb']['metrics'] for r in runs]))
            report['summaries'].append(s)
        report['status'] = 'complete'
        save()
    except Exception as exc:
        report.update(status='failed', error=str(exc))
        save()
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for role in ('cold', 'split'):
        p.add_argument('--' + role, required=True, help='dedicated loopback /api/v1/query endpoint')
        p.add_argument('--' + role + '-container', required=True)
        p.add_argument('--' + role + '-profiles', type=Path)
    p.add_argument('--warehouse', default=WAREHOUSE)
    p.add_argument('--control', type=Path)
    p.add_argument('--case', action='append', choices=tuple(cases()))
    p.add_argument('--reps', type=int, default=3)
    p.add_argument('--timeout', type=int, default=1800)
    p.add_argument('--run-dir', type=Path, required=True)
    run(p.parse_args())


if __name__ == '__main__': main()
