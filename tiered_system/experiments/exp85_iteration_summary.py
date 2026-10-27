"""Rebuild the iteration comparison, or collect a read-only live baseline.

Report generation uses only recorded JSON. Live mode spools one response at a
time and validates outside its receive timer; it never operates Docker/storage.
"""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import statistics
import sys
import tempfile

HERE = Path(__file__).resolve().parent
RESULTS = HERE / 'results'
PLAN = HERE.parents[1] / 'plans' / '11_query_performance_summary.md'
COLUMNS = ['minute', 'hour', 'day', 'all_minute', 'all_hour', 'march', 'april', 'may']


def load(name):
    return json.loads((RESULTS / name).read_text())


def median(values):
    values = [v for v in values if isinstance(v, (float, int)) and not isinstance(v, bool)]
    return statistics.median(values) if values else None


def case_key(label):
    s = str(label).lower()
    if 'boundary' in s or 'fleet' in s or 'plugin_vsn' in s or 'plugin_hour' in s:
        return None
    if 'march' in s or '2025-03_series_month' in s:
        return 'march'
    if 'april' in s or '2025-04_series_month' in s:
        return 'april'
    if 'may' in s:
        return 'may'
    if s.startswith('hot'):
        return None
    all_names = 'all' in s or s in ('vsn_hour', 'mixed_hour')
    if 'minute' in s or '1min' in s or '1 min' in s:
        return 'all_minute' if all_names else 'minute'
    if 'hour' in s or '1hour' in s or '1 hour' in s:
        return 'all_hour' if all_names else 'hour'
    if 'day' in s:
        return 'day'
    return None


def row(label, source, timer, fmt='arrow', group=None, group_field='layout', container=None,
        note='', key='results'):
    d = load(source)
    rows = d.get(key, [])
    buckets = {c: [] for c in COLUMNS}
    details = {c: [] for c in COLUMNS}
    for x in rows:
        if group is not None and x.get(group_field) != group:
            continue
        declared_format = x.get('format', x.get('fmt'))
        if declared_format and declared_format != fmt:
            continue
        name = x.get('query', x.get('case', ''))
        if isinstance(name, dict):
            name = x.get('case', '')
        k = case_key(name)
        if k is None:
            continue
        v = x
        if container:
            v = x.get(container, x.get('error', {}).get(container, {}))
        elif 'response' in x:
            v = x['response']
        value = v.get('total_seconds', v.get('total_s', v.get('total')))
        if isinstance(value, (int, float)):
            buckets[k].append(value)
            details[k].append(v)
    profiles = {}
    for case, values in details.items():
        if not values:
            continue
        first_key = next((k for k in ['first_result_seconds', 'first_batch_seconds', 'first_body_seconds', 'ttfb_s', 'ttfb'] if k in values[0]), None)
        profiles[case] = dict(first_metric=first_key,
            first_seconds=median([v.get(first_key) for v in values]) if first_key else None,
            bytes=median([v.get('bytes', v.get('raw_mb', v.get('raw_gb', 0)) * (1e9 if 'raw_gb' in v else 1e6)) for v in values]),
            api_peak_bytes=median([v.get('api_peak', v.get('peak')) for v in values]),
            client_peak_bytes=median([v.get('client_peak_rss_bytes', v.get('client_rss')) for v in values]),
            client_cpu_seconds=median([v.get('client_cpu_seconds') for v in values]),
            rows=sorted({v['rows'] for v in values if isinstance(v.get('rows'), int)}))
    return dict(iteration=label, source=source, timer=timer, format=fmt,
                seconds={c: median(v) for c, v in buckets.items()}, response_profiles=profiles, note=note)


def num(x):
    return 'null' if x is None else f'{x:.3f}'


def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |',
                      '| ' + ' | '.join('---' for _ in headers) + ' |'] +
                     ['| ' + ' | '.join(str(v).replace('|', '/').replace('<', '&lt;').replace('>', '&gt;') for v in r) + ' |' for r in rows])


def source_link(source):
    return f'[{source}](../tiered_system/experiments/results/{source})'


def build():
    arrow, ndjson = [], []
    prod = 'exp85_production_ndjson.json'
    production = load(prod) if (RESULTS / prod).exists() else {}
    production_note = ('No Arrow support. NDJSON not measured: ' + production.get('error', 'baseline pending'))
    arrow.append(dict(iteration='Production deployment', source=prod, timer='HTTP', format='arrow',
                      seconds=dict.fromkeys(COLUMNS), note='Unsupported; all Arrow timings are null.'))
    if production.get('status') == 'complete':
        mismatched = [x['case'] for x in production.get('summaries', []) if not x.get('expected_rows_match')]
        unordered = [x['case'] for x in production.get('summaries', []) if x.get('timestamps_ordered') is False]
        ndjson.append(row('Production deployment', prod, 'HTTP', fmt='ndjson', key='summaries',
                          note='External HTTPS; inspect expected-row checks before comparing.' +
                          (' INCOMPARABLE row counts: ' + ', '.join(mismatched) + '.' if mismatched else '') +
                          (' Unordered timestamps: ' + ', '.join(unordered) + '.' if unordered else '')))
    else:
        ndjson.append(dict(iteration='Production deployment', source=prod, timer='HTTP', format='ndjson',
                           seconds=dict.fromkeys(COLUMNS), note=production_note))
    specs = [
        ('Initial Arrow / original decoder (46)', 'exp46_arrow_vs_ndjson.json', 'DF', 'medians', None),
        ('Initial curl/DuckDB reader (47)', 'exp47_arrow_vs_ndjson_curl.json', 'COUNT', 'medians', None),
        ('Hot month initial reader (48)', 'exp48_month_arrow_vs_ndjson.json', 'COUNT', 'medians', None),
        ('Streaming reader, per-query engine (52)', 'exp52_arrow_vs_ndjson_rerun.json', 'COUNT', 'results', 'nested'),
        ('Persistent engine focused rerun (54)', 'exp54_arrow_engine_reuse_e2e.json', 'COUNT', 'results', 'nested'),
        ('Persistent engine full rerun (55)', 'exp55_curl_reused_engine.json', 'COUNT', 'results', 'nested'),
        ('Persistent engine / old Python decoder (55)', 'exp55_sage_data_client_reused_engine.json', 'DF', 'results', 'nested'),
    ]
    for label, source, timer, key, nested in specs:
        for fmt, target in [('arrow', arrow), ('ndjson', ndjson)]:
            note = 'Historical environment; not production.'
            if source == 'exp52_arrow_vs_ndjson_rerun.json':
                note += ' March failed: empty response / API unavailable; no month latency.'
            if source == 'exp55_sage_data_client_reused_engine.json' and fmt == 'ndjson':
                note += ' May is PARTIAL: 19,263,320/30,388,936 rows; exclude from speedups.'
            target.append(row(label, source, timer, fmt, container=fmt if nested else None, key=key, note=note))
    arrow.append(dict(iteration='Before streaming-reader fix (49)', source='exp49_cold_month_arrow_vs_ndjson.json',
                      timer='HTTP', format='arrow', seconds=dict.fromkeys(COLUMNS),
                      note='FAILED: no response after ~17 min; ~10.4 GiB sampled API memory.'))
    arrow.append(row('Native streaming reader fix (49)', 'exp49_cold_month_arrow_streaming.json', 'COUNT', key='medians',
                     note='Single live run; ~3.6 GB sampled API peak; month now completes.'))
    for source, label in [('exp49_cold_month_arrow_vs_ndjson.json', 'Before streaming fix / 12 GiB cap (49)'),
                          ('exp49_cold_month_ndjson_14g.json', 'Separate 14 GiB cap rerun (49)')]:
        if '14g' in source:
            ndjson.append(row(label, source, 'COUNT', 'ndjson', key='medians', note='Single historical run.'))
        else:
            d = load(source)['ndjson']
            ndjson.append(dict(iteration=label, source=source, timer='COUNT', format='ndjson',
                seconds=dict(dict.fromkeys(COLUMNS), march=d['total_s']), note='Single run; 10.45 GiB API peak; 8/10 GiB caps OOM.'))
    ndjson.append(row('Early Sage client month attempt (50)', 'exp50_month_sage_data_client.json', 'DF', 'ndjson',
                      note='May PARTIAL: 18,705,434 rows. March complete; severe client backpressure.'))
    arrow.append(row('Batched Python decoder (58)', 'exp58_arrow_decoder_live.json', 'DF',
                     note='Kept. Full March and May returned; no paired new NDJSON run.'))
    for mode in ['none', 'zstd', 'lz4']:
        arrow.append(row('Compression trial: ' + mode + ' (60)', 'exp60_arrow_compression_live.json', 'DF',
                         group=mode, group_field='mode', note='Zstd selected; codec-only trial with same optimized decoder.'))
    for label, source, note in [
        ('Public-schema baseline (64)', 'exp64_cold_execution_live_baseline.json', 'Typed wire fields already removed.'),
        ('Direct JSON metadata assembly (64)', 'exp64_cold_execution_live.json', 'Kept; DuckDB 1.5.6, v2 warehouse.'),
        ('Same SQL later profiling control (65)', 'exp65_control_live.json', 'Independent rerun, not an additional optimization.'),
        ('DuckDB 2.0 migration (66)', 'exp66_duckdb20_live.json', 'Kept; v2 split stored values, Zstd.'),
        ('Async pool 4 diagnostic (66)', 'exp66_duckdb20_async4_live.json', 'Not selected; only two month runs.'),
        ('Async pool 16 diagnostic (66)', 'exp66_duckdb20_async16_live.json', 'Not selected; only two month runs.'),
        ('Promoted-tag JSON control (67)', 'exp67_before_live.json', 'Fresh control on DuckDB 2.0.'),
        ('Promoted-tag JSON fast paths (67)', 'exp67_after_live.json', 'Kept; v2, historical microsecond precision.'),
    ]:
        arrow.append(row(label, source, 'DF', note=note))
    for fmt, target in [('arrow', arrow), ('ndjson', ndjson)]:
        target.append(row('Initial v3 + VARIANT, March 1 only (70)', 'exp70_v3_live_baseline.json', 'BUFFER', fmt,
                          note='Time-first; first-batch field uses buffered collection, unlike later HTTP timer.'))
    arrow.append(row('Full v3 + VARIANT, time-first (71)', 'exp71_v2_v3_full_live.json', 'DF', key='current_client_results',
                     note='Migration regression; layout, precision and coverage changed, not isolated VARIANT cost.'))
    for fmt, target in [('arrow', arrow), ('ndjson', ndjson)]:
        for layout, label in [('time', 'Time-first day control (73)'), ('plugin_vsn_time', 'Unbounded plugin groups, March 1 (73)')]:
            target.append(row(label, 'exp73_plugin_live_compare.json', 'HTTP', fmt, layout, key='summaries',
                              note='One-day trial; unbounded plugin groups superseded.'))
        for source in ['exp73_matched_archive_batches.json', 'exp73_matched_time_batches.json', 'exp73_matched_time_batches_rest.json']:
            for layout in ['time', 'plugin_vsn_time']:
                target.append(row(source.removesuffix('.json') + ' / ' + layout, source, 'HTTP', fmt, layout, key='summaries',
                    note='Matched March-1 coverage; bounded groups. Initial time-batch run interrupted; only completed cases shown.'))
    for fmt, target, filename in [('arrow', arrow, 'http-arrow.json'), ('ndjson', ndjson, 'http-ndjson.json')]:
        for layout, label in [('time', 'Full v3 time-first control (79)'), ('plugin_vsn_time', 'Adopted bounded plugin layout (79)')]:
            target.append(row(label, 'full-layout-1792962354/' + filename, 'HTTP', fmt, layout, key='summaries',
                note=('Short/day cells aggregate four sampled days. Month rows are complete.' if fmt == 'arrow' else
                      'Partial sweep only: March 1 day completed. Time-first March OOM at 14 GB; candidate month NOT RUN.')))
    arrow.append(row('Adopted layout whole-range control (82)', 'exp82-1793050193/unprofiled.json', 'HTTP', key='summaries',
                     note='Kept. March/April; sampled phase cgroup peak 6.54 GiB.'))
    for flag, label in [('control', 'Final same-image control, reuse OFF (84)'), ('candidate', 'Uniform metadata reuse ON (84)')]:
        arrow.append(row(label, 'meta-eval-1793060263/' + flag + '.json', 'HTTP', key='summaries',
                         note='Current accepted path.' if flag == 'control' else 'REJECTED: second scan overwhelms projection savings.'))
    # An unmeasured format is a visible gap, never a reused old measurement.
    measured_ndjson = {x['iteration'] for x in ndjson}
    for r in arrow:
        if r['iteration'] not in measured_ndjson and r['timer'] in ('HTTP', 'DF'):
            ndjson.append(dict(r, format='ndjson', seconds=dict.fromkeys(COLUMNS),
                              response_profiles={},
                              note='No completed NDJSON rerun for this iteration; Arrow-only measurements cannot populate these cells.'))
    def chronological(r):
        if r['iteration'] == 'Production deployment':
            return 0
        m = re.search(r'exp(\d+)|\((\d+)\)', r['iteration'])
        return int(m[1] or m[2]) if m else 999
    arrow.sort(key=chronological)
    ndjson.sort(key=chronological)

    # Local-only candidates are deliberately outside the live API chronology.
    local = []
    for source, groupfield, groups in [
        ('exp67_metadata_projection.json', 'mode', ['baseline', 'optimized']),
        ('exp68_value_variant.json', 'mode', ['split', 'variant']),
        ('exp72_plugin_layout.json', 'layout', ['original_time', 'time_repacked', 'plugin_vsn_time']),
        ('exp74_layout_scan_profiles.json', 'layout', ['time', 'archive_plugin']),
        ('exp78_bounded_layout_benchmark.json', 'layout', ['time_first', 'archive_100000', 'archive_262144', 'archive_524288', 'archive_1000000', 'time_100000', 'time_262144', 'time_524288', 'time_1000000']),
        ('exp80_bounded_months.json', 'window_days', [0, 1, 3, 7]),
        ('exp83_projection_months.json', 'mode', ['full', 'without_meta', 'without_value', 'without_both']),
        ('exp84_metadata_reuse_short.json', 'mode', ['baseline', 'correlated', 'dictionary']),
        ('exp84_metadata_reuse_json_short.json', 'mode', ['baseline', 'correlated_json']),
        ('exp84_metadata_reuse_months.json', 'mode', ['baseline', 'correlated_json']),
        ('exp84_uniform_metadata_short.json', 'mode', ['baseline', 'uniform']),
        ('exp84_uniform_metadata_months.json', 'mode', ['baseline', 'uniform']),
    ]:
        for group in groups:
            if source == 'exp83_projection_months.json':
                # Response and checksum families have intentionally different timers.
                for family in ['response', 'checksum']:
                    subset = [x for x in load(source)['summaries'] if x.get('family') == family and x.get('mode') == group]
                    local.append(dict(iteration=f'{source.removesuffix(".json")} / {family} / {group}', source=source,
                        timer='LOCAL', format='arrow' if family == 'response' else 'checksum',
                        seconds={c: median([x['total_seconds'] for x in subset if case_key(x['case']) == c]) for c in COLUMNS},
                        note='DIAGNOSTIC: omitted fields change response; not a deployable equivalent.'))
            else:
                local.append(row(source.removesuffix('.json') + ' / ' + str(group), source, 'LOCAL',
                                 group=group, group_field=groupfield, key='summaries',
                                 note='Local files only; see source for memory, semantics and timer exclusions.'))
    profiles = []
    for source, label in [('exp65_server_profiles.json', 'v2 / DuckDB 1.5.6 (65)'),
                          ('exp66_duckdb20_profiles.json', 'v2 / DuckDB 2.0 (66)'),
                          ('exp67_march_profiles.json', 'v2 / promoted JSON fast paths (67)'),
                          ('exp70_v3_day_profiles.json', 'v3 time-first / DAY (70)'),
                          ('exp71_v3_march_profile.json', 'v3 time-first / March (71)')]:
        qs = [q for q in load(source)['queries'] if q.get('eligible') and q.get('status') == 'complete']
        if not qs:
            continue
        def metric(key):
            return median([q['duckdb']['metrics'].get(key) for q in qs])
        def work(key):
            return median([q['duckdb'].get('operator_work_seconds', {}).get(key) for q in qs])
        profiles.append(dict(iteration=label, source=source, case='day' if '(70)' in label else 'march',
            scan=work('scan'), projection=work('projection'), sort=work('sort'),
            conversion=median([q['phases'].get('arrow_conversion_seconds') for q in qs]),
            buffer=metric('system_peak_buffer_memory'), spill=metric('system_peak_temp_dir_size'),
            discovery=None))
    for source, label in [('exp82-1793050193/profiled.json', 'Adopted layout (82)'),
                          ('meta-eval-1793060263/candidate-profiled.json', 'Reuse ON, rejected (84)')]:
        for x in load(source)['summaries']:
            w, m, ph = x['operator_work'], x['engine_metrics'], x['phases']
            profiles.append(dict(iteration=label, source=source, case=x['case'], scan=w.get('scan'),
                projection=w.get('projection'), sort=w.get('sort'), conversion=ph.get('arrow_conversion_seconds'),
                buffer=m.get('system_peak_buffer_memory'), spill=m.get('system_peak_temp_dir_size'),
                discovery=ph.get('metadata_discovery_seconds')))
    # Inventory every recorded experiment report, including synthetic and failed
    # probes that cannot honestly be assigned an HTTP query speed.
    files = sorted(f for f in RESULTS.glob('exp*.json') if (m := re.match(r'exp(\d+)', f.name)) and 46 <= int(m[1]) <= 84)
    files += sorted((RESULTS / 'full-layout-1792962354').glob('*.json'))
    files += sorted((RESULTS / 'exp82-1793050193').glob('*.json'))
    files += sorted((RESULTS / 'meta-eval-1793060263').glob('*.json'))
    if (RESULTS / prod).exists():
        files.append(RESULTS / prod)
    manifest = [dict(source=str(f.relative_to(RESULTS)), sha256=hashlib.sha256(f.read_bytes()).hexdigest(),
                     status=load(str(f.relative_to(RESULTS))).get('status', 'historical'),
                     scope=load(str(f.relative_to(RESULTS))).get('scope', 'See source report for method.')) for f in files]
    data = dict(as_of='2026-10-26', production=production, arrow=arrow, ndjson=ndjson,
                local_diagnostics=local, profiles=profiles, source_manifest=manifest)
    (RESULTS / 'exp85_iteration_summary.json').write_text(json.dumps(data, indent=2) + '\n')
    with (RESULTS / 'exp85_iteration_summary.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['format', 'iteration', 'timer', *COLUMNS, 'note', 'source'])
        writer.writeheader()
        for r in arrow + ndjson + local:
            writer.writerow({**{k: r[k] for k in ['format', 'iteration', 'timer', 'note', 'source']}, **r['seconds']})
    production_text = ('**Production comparison is pending.** The production endpoint is https://data.sagecontinuum.org/api/v1/query. Its Arrow results are `null` because that deployment does not support Arrow (user supplied). A real NDJSON POST was attempted from this workspace on 2026-10-26, but DNS resolution failed; no production latency, row count, memory, or operator profile was obtained. The demo Influx measurements below are **not production measurements**. See the retained [attempt](../tiered_system/experiments/results/exp85_production_ndjson.json).'
        if production.get('status') != 'complete' else
        '**Production NDJSON baseline collected.** See its linked report for endpoint, date, actual returned rows and expected-row checks. Production Arrow remains `null` (unsupported). Results with differing row counts are not comparable. Public HTTP timing does not provide a production internal operator or memory profile.')
    parts = ['# Query performance across iterations — 2026-10-26',
        'The accepted implementation is DuckDB 2.0, Iceberg v3, stored `value VARIANT`, stored `meta MAP`, Zstd Arrow, whole-range execution, and the adopted bounded plugin layout. Uniform-metadata discovery remains **off**. This report summarizes the recorded experiments; it does not change the API or warehouse.',
        production_text,
        'Times are seconds. `null` means unsupported, unmeasured, or no completed result; failure details stay in the notes. Do not calculate an overall speedup across different timers, datasets, layouts, hosts or incomplete results. Historical runs were collected sequentially with uncontrolled OS/Pelican caches. Most short cases use excluded warmups and medians; some older months are single runs. Follow each linked source for repetitions and limits.',
        'Timer: **HTTP** = receive and file spool, plus Arrow IPC iteration for Arrow, no dataframe; **DF** = request through Sage-client dataframe completion; **COUNT** = curl download plus DuckDB count; **BUFFER** = initial v3 response collector, whose first-batch timestamp is buffered; **LOCAL** = native scan/sort/IPC on local Parquet, excluding remote execution. HTTP Arrow and NDJSON perform different client receive work, so they are complete format-path measurements, not server CPU comparisons.',
        'Series = `wxt.env.temp`, `vsn=W0A0`. Minute/hour/day begin March 1, 2025 unless explicitly noted. All-name queries retain `vsn=W0A0`. March/April/May are complete calendar months with exclusive end bounds. In the full-layout sweep only, short/day cells are medians across March 1, March 31, April 1 and April 30. Fleet/plugin cases and complete per-case timings remain in the sources.',
        'Reference counts: March 1 series minute **698**, hour **43,509**, day **1,048,491**; all-name minute **6,352**, hour **395,989**; March series **25,940,207**, April **31,189,045**, hot May **30,388,936**. Partial May client responses are explicitly marked. Earlier v2 responses have microsecond timestamps; v3 keeps nanoseconds. Migration comparisons previously checked equality using a separate microsecond-floored copy, outside the timer.']
    for title, rows in [('Arrow — live API iterations', arrow), ('NDJSON — live API iterations', ndjson), ('Local candidates and ablations — separate from live API timings', local)]:
        parts.append('## ' + title)
        parts.append(table(['Iteration', 'Timer', 'Series min', 'Series hour', 'Series day', 'All min', 'All hour', 'March', 'April', 'Hot May', 'Profile / decision / limitations'],
            [[r['iteration'], r['timer'], *[num(r['seconds'][c]) for c in COLUMNS], r['note'] + ' ' + source_link(r['source'])] for r in rows]))
    parts += ['## Month response profiles',
        'First-record, first-batch and first-body-byte metrics retain their original names; a first-byte measurement can include only headers and is not interchangeable with first decoded data. Body sizes are decimal MB. API peaks are historical sampled container usage, client peaks retain their source sampling scope, and both differ from the engine buffer metrics below. `null` means no sample. Partial May responses remain excluded from speedup claims. The timer column has the same meaning as in the main tables.']
    response_rows = []
    for r in arrow + ndjson:
        for case in ['march', 'april', 'may']:
            if case not in r.get('response_profiles', {}):
                continue
            x = r['response_profiles'][case]
            response_rows.append([r['iteration'], r['format'], r['timer'], case, ', '.join(str(v) for v in x['rows']) or 'null',
                x['first_metric'] or 'null', num(x['first_seconds']), num(r['seconds'][case]),
                num(x['bytes'] / 1e6) if x['bytes'] else 'null',
                num(x['api_peak_bytes'] / 1e9) if x['api_peak_bytes'] is not None else 'null',
                num(x['client_peak_bytes'] / 1e9) if x['client_peak_bytes'] is not None else 'null', num(x['client_cpu_seconds'])])
    parts.append(table(['Iteration', 'Format', 'Timer', 'Range', 'Rows', 'First metric', 'First s', 'Total s', 'Body MB', 'API peak GB', 'Client peak GB', 'Client CPU s'], response_rows))
    parts += ['## Recorded server profiles',
        'These profiles were collected separately from the primary unprofiled timing runs. Operator work overlaps across workers and can include waits; it is not additive wall time or pure CPU. Projection below sums all projection operators. Buffer and spill are engine counters in decimal GB, not API RSS, cgroup peaks or direct Pelican traffic. Discovery is elapsed time and is excluded from the reuse candidate’s main native profile. `null` means no corresponding measurement; no production internal profile is available.']
    parts.append(table(['Iteration', 'Range', 'Scan work s', 'Projection work s', 'Sort work s', 'Arrow conversion s', 'Discovery s', 'Buffer GB', 'Spill GB', 'Source'],
        [[x['iteration'], x['case'], *[num(x[k]) for k in ['scan', 'projection', 'sort', 'conversion', 'discovery']],
          num(x['buffer'] / 1e9) if x['buffer'] is not None else 'null', num(x['spill'] / 1e9) if x['spill'] is not None else 'null', source_link(x['source'])] for x in profiles]))
    parts += ['## Reading the results',
        'The Python decoder change removed the major client bottleneck: historical March dataframe completion went from **1,442.336 s to 137.146 s**. Zstd then reduced the March Arrow body from **7.56 GB to 431 MB**. Direct metadata JSON assembly and DuckDB 2.0 improved the v2 path further, but its **29.084 s** result is not the current nanosecond/v3 warehouse result.',
        'The full v3 time-first warehouse initially regressed badly. In the paired full-layout HTTP sweep, the bounded plugin layout reduced March **594.017 → 77.411 s** and April **620.465 → 82.139 s**. The final same-image control records **76.397 / 76.019 s** for March/April. Uniform-metadata discovery raised them to **142.589 / 166.361 s**, so it stays off. These are historical observations under the recorded conditions, not a production speedup claim.',
        'NDJSON remains supported, but the full-layout NDJSON sweep was interrupted: time-first March OOM-killed its 14 GB container, before the candidate month was submitted. Current candidate month NDJSON latency and safety are **unmeasured**. The completed month-boundary fleet-hour comparison returned **5,744,480 rows**, time-first **43.707 s**, adopted layout **40.787 s** ([source](../tiered_system/experiments/results/full-layout-1792962354/http-ndjson-boundary.json)).',
        'Stored-value VARIANT was retained for schema alignment; the local exp68 representation probe is not the full migration cost. Typed multi-column Arrow wire values were reverted to preserve the direct HTTP schema. Windowing, general metadata dictionary/correlation, and uniform-metadata discovery were rejected. Absent NDJSON reruns after decoder/codec/SQL changes stay null; Arrow measurements are never copied into NDJSON cells.',
        '## Complete experiment source inventory',
        'This inventory covers every retained exp46–84 JSON report, including setup-only, synthetic, correctness-only and failed experiments that cannot supply a comparable live speed. exp62/69/76/77/79/81 include tooling, publication or validation rather than a new independent API timing. The [machine-readable report](../tiered_system/experiments/results/exp85_iteration_summary.json) retains SHA-256 hashes for all listed sources; the [CSV](../tiered_system/experiments/results/exp85_iteration_summary.csv) contains both format tables and the local candidate matrix. Detailed synthetic decoder/compression/typed-wire and spill profiles remain linked in the inventory instead of being mislabeled as HTTP timings.']
    parts.append(table(['Artifact', 'Status', 'Measurement scope'], [[source_link(x['source']), x['status'], str(x['scope']).replace('\n', ' ')] for x in manifest]))
    parts += ['## Fill the production NDJSON baseline',
        'Run from the workspace root on a machine that can resolve the production host. PyArrow must be available (the existing VM benchmark environment already uses it). This sends read-only queries, keeps one temporary body at a time and writes a fresh result file; it does not restart services or write warehouse objects. Short queries get one excluded warmup and three measured repetitions. Month queries are optional, single runs by default, and may transfer many GB; their row counts must match before comparing speeds.',
        '```sh\npython -I sage-data-rework/tiered_system/experiments/exp85_iteration_summary.py live \\\n  --output sage-data-rework/tiered_system/experiments/results/exp85_production_ndjson_vm.json\n# Add --include-months for March/April/May (series only, never fleet months).\n# Then rebuild using the returned production report:\npython -I sage-data-rework/tiered_system/experiments/exp85_iteration_summary.py report \\\n  --production-result sage-data-rework/tiered_system/experiments/results/exp85_production_ndjson_vm.json\n```',
        'For a fresh matching current-API comparison, use the same live command with `--endpoint http://127.0.0.1:10006/api/v1/query --formats ndjson arrow --output <fresh-file.json>` and leave months disabled. Compare a production cell only where the returned row count matches. Profiling production internals requires server-side access; this public endpoint alone supplies client-observed timing, body size and correctness checks.']
    PLAN.write_text('\n\n'.join(parts) + '\n')
    print(PLAN)


def live(args):
    # Use the existing measured receive timer and bounded-memory validation.
    sys.path.insert(0, str(HERE))
    from exp73_plugin_live_compare import receive, validate_body
    from urllib.request import urlopen
    if args.output.exists():
        raise ValueError('output exists; select a fresh result path')
    if args.reps < 1 or args.month_reps < 1:
        raise ValueError('repetitions must be positive')
    base = {'name': 'wxt.env.temp', 'vsn': 'W0A0'}
    cases = [
        ('series_minute', '2025-03-01', '00:01:00', base, 698),
        ('series_hour', '2025-03-01', '01:00:00', base, 43509),
        ('all_plugins_vsn_minute', '2025-03-01', '00:01:00', {'vsn': 'W0A0'}, 6352),
        ('all_plugins_vsn_hour', '2025-03-01', '01:00:00', {'vsn': 'W0A0'}, 395989),
        ('series_day', '2025-03-01', None, base, 1048491),
    ]
    selected = [(n, dict(start=d+'T00:00:00Z', end=(d+'T'+t+'Z') if t else '2025-03-02T00:00:00Z', filter=f), count, False)
                for n, d, t, f, count in cases]
    if args.include_months:
        selected += [(n, dict(start=d+'T00:00:00Z', end=e+'T00:00:00Z', filter=base), count, True)
                     for n, d, e, count in [('march', '2025-03-01', '2025-04-01', 25940207),
                        ('april', '2025-04-01', '2025-05-01', 31189045), ('may', '2025-05-01', '2025-06-01', 30388936)]]
    report = dict(status='running', endpoint=args.endpoint, started_utc=datetime.now(timezone.utc).isoformat(),
                  scope='HTTP receive/spool; Arrow IPC iteration for Arrow; validation outside timer; no dataframe',
                  arrow=None if 'arrow' not in args.formats else 'requested', results=[], summaries=[],
                  transport='Accept-Encoding: identity', profile=None, warmups='one per short case/format; none for months')
    def save():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2)+'\n')
    # Guard the NDJSON path against HTTP 200 error documents before it is timed
    # as a successful NDJSON response. Full record validation follows outside timer.
    def opener(request, timeout):
        response = urlopen(request, timeout=timeout)
        if request.get_header('Accept') == 'application/x-ndjson':
            content_type = response.headers.get('Content-Type', '').split(';')[0].strip()
            if content_type not in ('application/x-ndjson', 'application/ndjson', 'application/json', 'text/plain'):
                response.close()
                raise ValueError('unexpected NDJSON MIME type: '+content_type)
        return response
    save()
    try:
        with tempfile.TemporaryDirectory(prefix='sage-query-comparison-') as tmp:
            path = Path(tmp) / 'response'
            for name, query, expected, month in selected:
                signature = None
                for fmt in args.formats:
                    for repetition in range(1 if month else 0, (args.month_reps if month else args.reps)+1):
                        measurement = receive(args.endpoint, query, fmt, path, args.timeout, opener=opener)
                        # Production NDJSON is not required to be time-sorted. Row identity
                        # is still compared across repetitions; ordering is recorded.
                        checked = validate_body(path, fmt, strict_time_order=False)
                        if measurement['rows'] != checked['rows']:
                            raise ValueError('receive/validation row count mismatch')
                        content = {k: v for k, v in checked.items() if k != 'timestamps_ordered'}
                        if signature is None:
                            signature = content
                        if signature != content:
                            raise ValueError('response contents differ across formats/repetitions')
                        item = dict(case=name, query=query, format=fmt, repetition=repetition,
                                    warmup=repetition == 0, expected_rows=expected,
                                    expected_rows_match=checked['rows'] == expected, validation=checked, **measurement)
                        report['results'].append(item)
                        save()
                        print(name, fmt, repetition, measurement['rows'], measurement['total_seconds'], flush=True)
                    measured = [x for x in report['results'] if x['case'] == name and x['format'] == fmt and not x['warmup']]
                    report['summaries'].append(dict(case=name, format=fmt, rows=measured[0]['rows'],
                        expected_rows_match=all(x['expected_rows_match'] for x in measured),
                        timestamps_ordered=all(x['validation']['timestamps_ordered'] for x in measured),
                        **{k: median([x[k] for x in measured]) for k in ['total_seconds', 'first_result_seconds', 'bytes', 'client_cpu_seconds']}))
                    save()
        report['status'] = 'complete'
        report['all_expected_rows_match'] = all(x['expected_rows_match'] for x in report['summaries'])
        save()
    except Exception as exc:
        report['status'] = 'failed'
        report['error'] = str(exc)
        save()
        raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    r = sub.add_parser('report')
    r.add_argument('--production-result', type=Path)
    l = sub.add_parser('live')
    l.add_argument('--endpoint', default='https://data.sagecontinuum.org/api/v1/query')
    l.add_argument('--formats', choices=['ndjson', 'arrow'], nargs='+', default=['ndjson'])
    l.add_argument('--output', type=Path, required=True)
    l.add_argument('--reps', type=int, default=3)
    l.add_argument('--month-reps', type=int, default=1)
    l.add_argument('--timeout', type=float, default=1200)
    l.add_argument('--include-months', action='store_true')
    args = p.parse_args()
    if args.command == 'live':
        live(args)
    else:
        if args.production_result:
            target = RESULTS / 'exp85_production_ndjson.json'
            incoming = args.production_result.read_bytes()
            d = json.loads(incoming)
            if d.get('endpoint') != 'https://data.sagecontinuum.org/api/v1/query':
                raise ValueError('production report must identify the production endpoint')
            if target.resolve() != args.production_result.resolve():
                target.write_bytes(incoming)
        build()


if __name__ == '__main__':
    main()
