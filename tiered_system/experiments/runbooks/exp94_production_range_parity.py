"""Read-only production query comparison; no writes, credentials, or storage access.

Compare one frozen 30-minute extract with six adjacent extracts. Optionally
compare 14-day counts for ONE discovered name/vsn with daily counts. Agreement
cannot establish shard coverage or native Influx types through a JSON API.

--benchmark-archive instead targets the known March/April 2025 W0A0 series,
calibrates counts against raw data, then compares months, days and seam records.
"""
import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.request import Request, urlopen

ENDPOINT = 'https://data.sagecontinuum.org/api/v1/query'
PLUGIN = 'registry.sagecontinuum.org/jrobrien/waggle-wxt536:1.00'


def ns(text):
    if isinstance(text, int):
        return text
    match = re.fullmatch(r'(.+T\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)', text)
    if not match:
        raise ValueError('unrecognized timestamp in API response')
    seconds = datetime.fromisoformat(match[1] + match[3].replace('Z', '+00:00'))
    delta = seconds.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days*86400 + delta.seconds)*1_000_000_000 + int((match[2] or '').ljust(9, '0'))


def stamp(time):
    return time.isoformat(timespec='seconds').replace('+00:00', 'Z')


def frozen(value):
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return (type(value).__name__, value)
    if isinstance(value, (int, Decimal)):
        # JSON cannot reliably expose the original Influx integer/float kind.
        # Compare exact numeric values without inventing a native type here.
        return ('number', Decimal(value))
    if isinstance(value, list):
        return ('list', tuple(map(frozen, value)))
    if isinstance(value, dict):
        return ('dict', tuple((k, frozen(v)) for k, v in sorted(value.items())))
    raise ValueError('unsupported JSON value')


def raw_counter(rows):
    return Counter((ns(r['timestamp']), r['name'], frozen(r['meta']), frozen(r['value'])) for r in rows)


def count_counter(rows):
    result = Counter()
    for row in rows:
        value = row['value']
        if isinstance(value, bool) or not isinstance(value, (int, Decimal)) or value < 0 or value != int(value):
            raise ValueError('count response is not an exact nonnegative integer')
        result[(row['name'], frozen(row['meta']))] += int(value)
    return +result


def comparison(before, parts, after):
    missing, extra = parts-before, before-parts
    return dict(stable_full_reads=before == after,
                outcome=('inconclusive_data_changed' if before != after else
                         'range_dependent_difference' if before != parts else 'no_omission_observed'),
                full=sum(before.values()), split=sum(parts.values()),
                missing_from_full=sum(missing.values()), extra_in_full=sum(extra.values()),
                difference_samples=[dict(identity=repr(k)[:600], count=v) for k, v in list(missing.items())[:5]])


def saved_query(root, report, save, label, low, high, filters, count=False):
    body = dict(start=stamp(low), end=stamp(high), filter=filters)
    if count:
        body['experimental_func'] = 'count'
    payload = json.dumps(body).encode()
    (root/(label+'.request.json')).write_bytes(payload+b'\n')
    request = Request(ENDPOINT, payload, headers={'Content-Type':'application/json', 'Accept':'application/x-ndjson'})
    report['requests'] += 1
    save()
    with urlopen(request, timeout=90) as response:
        raw = response.read(32*1024*1024+1)
    if len(raw) > 32*1024*1024:
        raise ValueError('response exceeds 32 MiB cap; narrow the production check')
    (root/(label+'.ndjson')).write_bytes(raw)
    rows = [json.loads(line, parse_float=Decimal) for line in raw.splitlines() if line.strip()]
    if not count:
        a, b = ns(stamp(low)), ns(stamp(high))
        if any(not a <= ns(row['timestamp']) < b for row in rows):
            raise ValueError('API returned rows outside requested half-open bounds')
    print(label, 'rows='+str(len(rows)), flush=True)
    return rows


def benchmark_ranges():
    return [(name, datetime(2025, month, 1, tzinfo=timezone.utc),
             datetime(2025, month+1, 1, tzinfo=timezone.utc))
            for name, month in [('march', 3), ('april', 4)]]


def run_benchmark_archive(args):
    if args.end or args.history_days:
        raise ValueError('benchmark-archive uses fixed March/April 2025 bounds; omit end/history-days')
    root = Path(args.run_dir).resolve()
    if root.exists() or 'node-data' in root.parts:
        raise ValueError('new isolated result directory required')
    root.mkdir(parents=True, mode=0o700)
    # These exact filters match the retained production benchmark, including
    # plugin-version changes if there are any. Do not apply the 2026 plugin tag.
    filters = {'name':'wxt.env.temp', 'vsn':'W0A0'}
    report = dict(experiment='exp94-benchmark-archive', completed=False, endpoint=ENDPOINT,
                  filter=filters, requests=0, months={},
                  scope='Read-only known March/April 2025 production series; counts vs daily/monthly partitions and exact boundary extracts; no native type proof',
                  script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    reference = Path(__file__).resolve().parent.parent/'results/exp85_production_ndjson.json'
    reference_bytes = reference.read_bytes()
    old = json.loads(reference_bytes)
    if old.get('status') != 'complete' or old.get('endpoint') != ENDPOINT:
        raise ValueError('completed production benchmark reference required')
    expected = {name: {r['rows'] for r in old['results'] if r['case']==name}
                for name, _, _ in benchmark_ranges()}
    if any(len(counts) != 1 for counts in expected.values()):
        raise ValueError('unambiguous March/April reference counts required')
    report['reference_sha256'] = hashlib.sha256(reference_bytes).hexdigest()
    save = lambda: (root/'report.json').write_text(json.dumps(report, indent=2)+'\n')
    save()
    query = lambda label, a, b, count=False: saved_query(root, report, save, label, a, b, filters, count)
    month_totals = Counter()
    verified_months = 0
    try:
        for name, low, high in benchmark_ranges():
            sample = query(name+'-raw-minute', low, low+timedelta(minutes=1))
            result = dict(start=stamp(low), end=stamp(high), raw_sample_rows=len(sample),
                          observed_plugins=sorted({r['meta'].get('plugin','') for r in sample}),
                          previous_production_rows=next(iter(expected[name])))
            report['months'][name] = result
            if not sample:
                result['outcome'] = 'inconclusive_no_raw_sample'
                save()
                continue
            # Check that the public count function actually works on these
            # known nonempty bounds before interpreting empty historical counts.
            raw_counts = Counter((r['name'], frozen(r['meta'])) for r in sample)
            calibration = count_counter(query(name+'-minute-count', low, low+timedelta(minutes=1), True))
            result['count_calibration_matches_raw'] = calibration == raw_counts
            if calibration != raw_counts:
                result['outcome'] = 'inconclusive_count_calibration_failed'
                result['count_calibration_total'] = sum(calibration.values())
                save()
                continue
            first = count_counter(query(name+'-full-before', low, high, True))
            days = Counter()
            cursor = low
            while cursor < high:
                end = min(cursor+timedelta(days=1), high)
                days.update(count_counter(query(name+'-day-'+cursor.strftime('%Y%m%d'), cursor, end, True)))
                cursor = end
            last = count_counter(query(name+'-full-after', low, high, True))
            result.update(comparison(first, +days, last))
            result['previous_production_count_matches'] = sum(first.values()) == result['previous_production_rows']
            if not first and not days:
                result['outcome'] = 'inconclusive_counts_empty_despite_raw_sample'
            if result['outcome'] == 'no_omission_observed':
                month_totals.update(first)
                verified_months += 1
            save()
        if verified_months == 2:
            a, b = benchmark_ranges()[0][1], benchmark_ranges()[-1][2]
            first = count_counter(query('both-months-before', a, b, True))
            last = count_counter(query('both-months-after', a, b, True))
            report['both_months'] = comparison(first, +month_totals, last)
            save()
        # Exact record comparison around the March/April seam, independent of
        # aggregation correctness. This is a calendar boundary, not proven shard
        # metadata. Keep the raw response small instead of downloading months.
        border = datetime(2025,4,1,tzinfo=timezone.utc)
        a, b = border-timedelta(minutes=1), border+timedelta(minutes=1)
        first = raw_counter(query('boundary-full-before', a, b))
        parts = raw_counter(query('boundary-left', a, border))
        parts.update(raw_counter(query('boundary-right', border, b)))
        last = raw_counter(query('boundary-full-after', a, b))
        report['boundary_raw'] = comparison(first, parts, last)
        if not first and not parts:
            report['boundary_raw']['outcome'] = 'inconclusive_no_data'
        report['completed'] = True
        save()
        print(json.dumps(report, indent=2))
        return 0
    except Exception as error:
        report['error'] = type(error).__name__+': '+str(error)
        save()
        raise


def run(args):
    if args.benchmark_archive:
        return run_benchmark_archive(args)
    root = Path(args.run_dir).resolve()
    if root.exists() or 'node-data' in root.parts:
        raise ValueError('new isolated result directory required')
    if not 0 <= args.history_days <= 14:
        raise ValueError('history-days must be 0..14; history tests select only one name/vsn')
    root.mkdir(parents=True, mode=0o700)
    # Freeze bounds once so requests never compare different moving windows.
    end = datetime.fromisoformat(args.end.replace('Z', '+00:00')) if args.end else datetime.now(timezone.utc)
    if end.tzinfo is None:
        raise ValueError('end must include its timezone')
    end = end.astimezone(timezone.utc).replace(microsecond=0)
    start = end - timedelta(minutes=30)
    report = dict(experiment='exp94', completed=False, endpoint=ENDPOINT, plugin=PLUGIN,
                  start=stamp(start), end=stamp(end), requests=0,
                  scope='Public read-only API range comparisons; no native type or shard metadata proof',
                  script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    save = lambda: (root/'report.json').write_text(json.dumps(report, indent=2)+'\n')
    save()

    def query(label, low, high, filters, count=False):
        return saved_query(root, report, save, label, low, high, filters, count)

    try:
        filters = {'plugin':PLUGIN}
        before_rows = query('raw-full-before', start, end, filters)
        parts = Counter()
        for i in range(6):
            low = start + timedelta(minutes=5*i)
            parts.update(raw_counter(query('raw-part-'+str(i), low, low+timedelta(minutes=5), filters)))
        after = raw_counter(query('raw-full-after', start, end, filters))
        report['recent_raw'] = comparison(raw_counter(before_rows), parts, after)
        if not before_rows and not parts:
            report['recent_raw']['outcome'] = 'inconclusive_no_data'
        save()
        if args.history_days:
            candidates = sorted({(r['name'],r['meta']['vsn']) for r in before_rows if r['meta'].get('vsn')},
                                key=lambda x:(x[0]!='wxt.env.temp',x))
            if not candidates:
                raise ValueError('no recent name/vsn available for a targeted historical check')
            name, vsn = candidates[0]
            filters = dict(plugin=PLUGIN,name=name,vsn=vsn)
            # End before the most recent complete UTC day to reduce ingestion
            # races. Before/after controls still check for historical backfill.
            high = end.replace(hour=0,minute=0,second=0)-timedelta(days=1)
            low = high-timedelta(days=args.history_days)
            first = count_counter(query('history-full-before', low, high, filters, True))
            combined = Counter()
            for i in range(args.history_days):
                a = low+timedelta(days=i)
                combined.update(count_counter(query('history-day-'+str(i), a, a+timedelta(days=1), filters, True)))
            last = count_counter(query('history-full-after', low, high, filters, True))
            result = comparison(first, +combined, last)
            if not first and not combined: result['outcome']='inconclusive_no_data'
            report['history_counts'] = dict(result,filter=filters,start=stamp(low),end=stamp(high))
        report['completed'] = True
        save()
        print(json.dumps(report, indent=2))
        return 0
    except Exception as error:
        report['error'] = type(error).__name__ + ': ' + str(error)
        save()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True)
    parser.add_argument('--benchmark-archive',action='store_true',help='test the known March/April 2025 wxt.env.temp/W0A0 data, calibrating counts before full-month comparisons')
    parser.add_argument('--end', help='optional fixed timezone-aware end; defaults to current UTC time')
    parser.add_argument('--history-days',type=int,default=0,help='optional daily count comparison for one discovered series, max 14 days')
    try:
        return run(parser.parse_args())
    except Exception as error:
        print('Production check stopped: '+str(error), file=sys.stderr)
        return 1


if __name__=='__main__':
    raise SystemExit(main())
