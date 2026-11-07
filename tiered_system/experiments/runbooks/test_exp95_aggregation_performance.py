import argparse
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import exp95_aggregation_performance as exp


def states(profiled=False):
    common = dict(image='sha256:same', running=True, restarts=0, oom_killed=False,
                  memory_limit=8 << 30, nano_cpus=0, memory_events={'oom': 0})
    result = {}
    for role in ('cold', 'split'):
        env = dict.fromkeys(exp.SETTINGS, '')
        env.update(COLD_FRONTIER='static', COLD_WAREHOUSE=exp.WAREHOUSE,
                   INFLUXDB_BUCKET='retained-historical',
                   COLD_BORDER=exp.BORDER if role == 'split' else '2025-05-01T00:00:00Z',
                   COLD_PROFILE_DIR='/private/profiles' if profiled else '')
        result[role] = dict(copy.deepcopy(common), id=role, settings=env)
    return result


class PerformanceTests(unittest.TestCase):
    def test_crossing_workload(self):
        border = exp.timestamp_ns(exp.BORDER)
        for name, q in exp.cases().items():
            self.assertLess(exp.timestamp_ns(q['start']), border)
            self.assertGreater(exp.timestamp_ns(q['end']), border)
            self.assertIn(q['experimental_func'], ('count', 'mean', 'sum', 'min', 'max'))

    def test_runtime_rejects_unmatched_or_dynamic_instances(self):
        exp.validate_runtime(states(), exp.WAREHOUSE, False)
        for mutation in ('image', 'border', 'dynamic', 'profile'):
            s = states()
            if mutation == 'image': s['split']['image'] = 'other'
            if mutation == 'border': s['split']['settings']['COLD_BORDER'] = '2025-05-01T00:00:00Z'
            if mutation == 'dynamic': s['split']['settings']['COLD_FRONTIER'] = 'dynamic'
            if mutation == 'profile': s['split']['settings']['COLD_PROFILE_DIR'] = '/profiles'
            with self.assertRaises(ValueError): exp.validate_runtime(s, exp.WAREHOUSE, False)
        after = states()
        after['split']['memory_events']['oom'] = 1
        with self.assertRaises(ValueError): exp.stable(states(), after)

    def test_exact_unsigned_and_schema(self):
        q = exp.cases()['series_day_count']
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'body'
            p.write_text(json.dumps(dict(timestamp=q['start'], name='a', meta={}, value=2**64-1)) + '\n')
            rows = exp.records(p, 'ndjson')
            exp.validate_rows(q, rows)
            self.assertEqual(rows[0]['value'], 2**64-1)
            self.assertTrue(exp.compare_rows(rows, [dict(rows[0], value=float(2**64-1))])[0])
            with self.assertRaises(ValueError): exp.validate_rows(q, rows + rows)

    def test_arrow_ndjson_preserve_nanoseconds_and_unsigned_max(self):
        with tempfile.TemporaryDirectory() as d:
            # JSON strings inside Arrow retain the public wire schema.
            stamp = exp.timestamp_ns('2025-03-01T12:00:00.000000194Z')
            schema = exp.pa.schema([('timestamp', exp.pa.timestamp('ns')), ('name', exp.pa.string()),
                                    ('value', exp.pa.string()), ('meta', exp.pa.string())])
            table = exp.pa.Table.from_arrays([
                exp.pa.array([stamp], type=exp.pa.timestamp('ns')), exp.pa.array(['a']),
                exp.pa.array([str(2**64-1)]), exp.pa.array(['{"vsn":"W0A0"}'])], schema=schema)
            arrow = Path(d) / 'body.arrow'
            with arrow.open('wb') as file, exp.ipc.new_stream(file, schema) as writer:
                writer.write_table(table)
            ndjson = Path(d) / 'body.ndjson'
            ndjson.write_text(json.dumps(dict(timestamp='2025-03-01T12:00:00.000000194Z', name='a',
                                              value=2**64-1, meta={'vsn': 'W0A0'})) + '\n')
            self.assertEqual(exp.records(arrow, 'arrow'), exp.records(ndjson, 'ndjson'))
            self.assertEqual(exp.records(arrow, 'arrow')[0]['time'] % 1000, 194)

    def test_run_retains_failure_and_bodies_on_parity_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            a = argparse.Namespace(run_dir=Path(d)/'run', cold='http://127.0.0.1:8081/api/v1/query',
                split='http://127.0.0.1:8082/api/v1/query', cold_container='cold', split_container='split',
                cold_profiles=None, split_profiles=None, control=None, reps=1, timeout=10,
                case=['series_day_count'], warehouse=exp.WAREHOUSE)
            def receive(endpoint, query, fmt, path, timeout):
                # Force the first response into NDJSON for this wire-independent orchestration check.
                path.write_text(json.dumps(dict(timestamp=query['start'], name='a', meta={},
                    value=1 if '8081' in endpoint else 2)) + '\n')
                return dict(rows=1, bytes=path.stat().st_size, total_seconds=.1)
            original = exp.records
            with patch.object(exp, 'runtime', side_effect=lambda c: states()[c]), \
                 patch.object(exp, 'receive', side_effect=receive), \
                 patch.object(exp, 'records', side_effect=lambda p, f: original(p, 'ndjson')):
                with self.assertRaisesRegex(ValueError, 'historical parity failed'): exp.run(a)
            report = json.loads((a.run_dir/'report.json').read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertTrue(list(a.run_dir.glob('*.arrow')))
            self.assertEqual(len(report['results']), 2)


if __name__ == '__main__': unittest.main()
