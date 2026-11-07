"""Compare retained exp95 unprofiled sweeps before/after metadata dictionaries.

Offline only. Verifies every saved response against the original baseline;
permits different API images but requires matching workload/execution settings.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

from exp95_aggregation_performance import records, validate_rows, compare_rows, stable, validate_runtime


def load(path):
    spec = json.loads(path.read_text())
    if spec['status'] != 'complete' or spec['profiled']:
        raise ValueError('completed unprofiled exp95 report required')
    stable(spec['runtime_before'], spec['runtime_after'])
    warehouse = spec['runtime_before']['cold']['settings']['COLD_WAREHOUSE']
    validate_runtime(spec['runtime_before'], warehouse, False)
    output = {}
    for result in spec['results']:
        name = result['body']
        if Path(name).name != name: raise ValueError('response must be beside report')
        body = path.parent / name
        if hashlib.sha256(body.read_bytes()).hexdigest() != result['body_sha256']:
            raise ValueError('response hash mismatch: ' + str(body))
        rows = records(body, result['format'])
        validate_rows(result['query'], rows)
        if len(rows) != result['response']['rows']: raise ValueError('response row count changed')
        key = (result['case'], result['placement'], result['format'])
        output.setdefault(key, []).append((result, rows))
    return spec, output


def compare(baseline, candidate):
    a, old = load(baseline)
    b, new = load(candidate)
    if a['selected_cases'] != b['selected_cases'] or set(old) != set(new):
        raise ValueError('workloads differ')
    for role in ('cold', 'split'):
        x, y = a['runtime_before'][role], b['runtime_before'][role]
        for key in ('settings', 'memory_limit', 'nano_cpus'):
            if x[key] != y[key]: raise ValueError('execution settings/resources differ: ' + key)
    summaries = []
    checked = 0
    for key in sorted(old):
        reference = old[key][0]
        for result, rows in old[key] + new[key]:
            if result['query'] != reference[0]['query']: raise ValueError('query changed: ' + str(key))
            diff, _ = compare_rows(reference[1], rows)
            if diff: raise ValueError('response parity failed: ' + str(key) + ': ' + str(diff[:3]))
            checked += 1
        medians = []
        for runs in (old[key], new[key]):
            times = [r['response']['total_seconds'] for r, _ in runs if not r['warmup']]
            if not times or any(t <= 0 for t in times): raise ValueError('positive measured repetitions required')
            medians.append(statistics.median(times))
        summaries.append(dict(case=key[0], placement=key[1], format=key[2],
            baseline_seconds=medians[0], candidate_seconds=medians[1], speedup=medians[0]/medians[1],
            reduction_percent=100*(1-medians[1]/medians[0])))
    return dict(status='complete', scope=__doc__, baseline=str(baseline), candidate=str(candidate),
        baseline_report_sha256=hashlib.sha256(baseline.read_bytes()).hexdigest(),
        candidate_report_sha256=hashlib.sha256(candidate.read_bytes()).hexdigest(),
        baseline_image=a['runtime_before']['cold']['image'], candidate_image=b['runtime_before']['cold']['image'],
        self_check=baseline.resolve() == candidate.resolve(), responses_checked=checked, summaries=summaries)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--baseline', required=True, type=Path)
    p.add_argument('--candidate', required=True, type=Path)
    p.add_argument('--output', required=True, type=Path)
    a = p.parse_args()
    if a.output.exists() or any('node-data' in path.resolve().parts for path in (a.output, a.baseline, a.candidate)):
        p.error('fresh isolated output and inputs required')
    report = compare(a.baseline, a.candidate)
    with a.output.open('x') as file: file.write(json.dumps(report, indent=2, allow_nan=False) + '\n')
    print('Validated', report['responses_checked'], 'responses; self_check=', report['self_check'])
    for row in report['summaries']:
        print(row['case'], row['placement'], row['format'], f"{row['baseline_seconds']:.3f}s -> {row['candidate_seconds']:.3f}s ({row['speedup']:.2f}x)")


if __name__ == '__main__': main()
