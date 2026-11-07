"""Offline harness checks; no Docker, Influx, DuckDB, or Pelican is contacted."""
import importlib.util
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

SPEC = importlib.util.spec_from_file_location('exp86', Path(__file__).with_name('exp86_aggregation_semantics.py'))
experiment = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(experiment)


class ExperimentTests(unittest.TestCase):
    def case(self, name): return next(c for c in experiment.cases() if c['id'] == name)

    def test_nanosecond_roundtrip_and_negative_floor(self):
        for value in (experiment.BASE + 194, -1, 0, -420000000194):
            self.assertEqual(experiment.ns(experiment.stamp(value)), value)
        expression, boundaries = experiment.windows(self.case('pre-epoch'))
        self.assertIn('%', expression)
        self.assertEqual(boundaries[0][0], self.case('pre-epoch')['start'] // (7 * experiment.MINUTE))

    def test_clipped_window_stops_keep_remainder(self):
        case = self.case('mean-5m-clipped-ns')
        _, bounds = experiment.windows(case)
        self.assertEqual(bounds[-1][1], case['end'])
        self.assertEqual(bounds[-1][1] % 1000, 194)

    def test_explicit_calendar_origin_and_mixed_interval(self):
        expression, bounds = experiment.windows(self.case('calendar-1mo'))
        self.assertIn('1970-01-01', expression)
        self.assertEqual(bounds[-1][1], self.case('calendar-1mo')['end'])
        with self.assertRaises(ValueError): experiment.windows(self.case('calendar-1mo1d'))

    def test_calendar_assignment_floors_before_microsecond_overload(self):
        expression, _ = experiment.windows(self.case('calendar-1mo'))
        self.assertIn('make_timestamp(epoch_ns(t)//1000', expression)
        self.assertIn('epoch_ns(t)%1000<0', expression)

    def test_empty_series_membership_survives_zero_input_limit(self):
        sql = experiment.aggregate_sql(self.case('head-zero'), 'full_raw')
        self.assertIn('series AS (SELECT DISTINCT name,meta FROM filtered)', sql)
        self.assertIn('rn<=0',sql)
        self.assertNotIn('WHERE a.n IS NOT NULL',sql)

    def test_window_minmax_retains_empty_rows_from_recorded_oracle(self):
        for name in ('min-5m','max-5m','nano-min','nano-max'):
            sql = experiment.aggregate_sql(self.case(name), 'full_raw')
            self.assertNotIn('WHERE a.n IS NOT NULL', sql)
            self.assertIn('CROSS JOIN boundaries',sql)
        # Plain selectors without windowing still do not fabricate empty results.
        self.assertIn('WHERE a.n IS NOT NULL',experiment.aggregate_sql(self.case('min-whole'),'full_raw'))

    def test_no_numeric_cast_policy_for_count(self):
        sql = experiment.aggregate_sql(self.case('text-count'), 'full_raw')
        self.assertIn('count(*)::BIGINT', sql)
        self.assertNotIn('unsupported or mixed', sql)

    def test_head_is_applied_before_grouping(self):
        sql = experiment.aggregate_sql(self.case('head-before-mean'), 'full_raw')
        self.assertLess(sql.index('row_number()'), sql.index('avg('))
        self.assertIn('rn<=2', sql)
        self.assertIn('PARTITION BY name,meta', sql)

    def test_split_is_disjoint_and_preserves_primitive_kinds(self):
        setup, source = experiment.placement_sql(experiment.fixture(), self.case('mean-whole'), Path('/tmp/isolated'), 'split')
        self.assertEqual(source, 'combined')
        self.assertIn('UNION ALL', setup)
        self.assertIn('read_parquet', setup)
        self.assertIn('AS UBIGINT', setup)
        self.assertIn('list_reverse(map_entries(meta))',setup)
        self.assertIn('list_sort(map_entries(meta))',setup)
        self.assertIn('COMPRESSION ZSTD', setup)
        self.assertNotIn('iceberg_scan', setup)

    def test_influx_name_guard(self):
        with self.assertRaises(ValueError): experiment.Influx('http://localhost:8086', 'secret', 'waggle', 'waggle')
        client = experiment.Influx('http://localhost:8086', 'secret', 'waggle', 'agg-exp86-' + 'a' * 32)
        with self.assertRaises(ValueError): client.request('/api/v2/delete')

    def test_http_error_details_are_bounded_and_redacted(self):
        client = experiment.Influx('http://localhost:8086', 'private-secret', 'waggle', 'agg-exp86-' + 'a' * 32)
        error = HTTPError('http://localhost', 400, 'bad', {}, io.BytesIO(b'unsupported input type private-secret'))
        with patch.object(experiment, 'urlopen', side_effect=error), self.assertRaises(RuntimeError) as caught:
            client.query('fixture')
        self.assertNotIn('private-secret', str(caught.exception))
        self.assertEqual(experiment.error_category(caught.exception), 'input_type')

    def test_csv_keeps_uint_and_nanos_and_null(self):
        csv = ('#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,unsignedLong,string\n'
               ',result,table,_time,_start,_measurement,_value,sensor\n'
               ',,0,2025-03-01T00:00:00.000000194Z,2025-03-01T00:00:00Z,exp86.unsigned,18446744073709551613,a\n')
        record = experiment.parse_flux(csv, True)[0]
        self.assertEqual(record['time'], experiment.BASE + 194)
        self.assertEqual(record['value'], 18446744073709551613)
        self.assertEqual(record['kind'], 'uint')
        self.assertEqual(record['meta'], {'sensor': 'a'})
        self.assertIsNone(experiment.parse_flux(csv.replace(',18446744073709551613,a', ',,a'), True)[0]['value'])

    def test_parser_failures_do_not_count_as_engine_parity(self):
        self.assertEqual(experiment.error_category(ValueError('malformed annotated CSV')), 'unclassified')
        self.assertEqual(experiment.error_category(RuntimeError('Binder Error: missing table')), 'unclassified')
        self.assertEqual(experiment.error_category(RuntimeError('schema collision: type conflict')), 'input_type')
        with self.assertRaises(ValueError): experiment.parse_flux(',error,reference\n,failed,1\n', True)

    def test_raw_types_and_multiset_comparison(self):
        row = dict(time=194, name='t', meta={'sensor': 'a'}, value=9007199254740993, kind='int')
        self.assertIsNone(experiment.compare([row], [dict(row)]))
        self.assertIsNotNone(experiment.compare([row], [dict(row, value=9007199254740992)]))
        self.assertIsNotNone(experiment.compare([row], []))

    def test_flux_order_and_null_fixture(self):
        flux = experiment.flux_query('isolated', self.case('head-before-window'))
        self.assertLess(flux.index('limit('), flux.index('aggregateWindow('))
        self.assertIn('debug.null', experiment.flux_query('isolated', self.case('all-null-count')))

    def test_wrong_engine_fails_before_any_influx_access(self):
        with TemporaryDirectory() as directory:
            root = Path(directory) / 'new'
            args = type('Args', (), dict(run_dir=str(root), duckdb='duckdb'))()
            with patch.object(experiment.Duck, 'run', return_value=([{'version': 'v1.5.6'}], 0)), patch.object(experiment.Influx, 'setup') as setup:
                with self.assertRaisesRegex(ValueError, 'requires DuckDB 2.0'): experiment.run(args)
                setup.assert_not_called()


if __name__ == '__main__': unittest.main()
