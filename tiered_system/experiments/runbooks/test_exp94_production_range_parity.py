"""Offline controls for read-only production comparisons; no network calls."""
import importlib.util
from decimal import Decimal
from pathlib import Path
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch
from types import SimpleNamespace
from datetime import timedelta
import contextlib
import io
import json

spec=importlib.util.spec_from_file_location('prod_probe',Path(__file__).with_name('exp94_production_range_parity.py'))
e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)


class ProductionComparisonTests(unittest.TestCase):
    def archive_run(self, response):
        with TemporaryDirectory() as directory:
            args=SimpleNamespace(run_dir=str(Path(directory)/'run'),end=None,history_days=0,benchmark_archive=True)
            with patch.object(e,'saved_query',side_effect=response) as query, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(e.run(args),0)
            return json.loads((Path(args.run_dir)/'report.json').read_text()),query.call_count

    @staticmethod
    def archive_response(root,report,save,label,low,high,filters,count=False):
        report['requests']+=1
        if count:
            return [dict(name='wxt.env.temp',meta={'vsn':'W0A0'},value=max((high-low).days,1))]
        return [dict(timestamp=e.stamp(low+timedelta(minutes=i)),name='wxt.env.temp',meta={'vsn':'W0A0'},value=Decimal('11.7'))
                for i in range(int((high-low).total_seconds()//60))]

    def test_known_archive_checks_all_days_and_both_months_without_recent_plugin(self):
        report,calls=self.archive_run(self.archive_response)
        self.assertEqual(calls,75)
        self.assertEqual(report['filter'],{'name':'wxt.env.temp','vsn':'W0A0'})
        self.assertEqual(report['months']['march']['full'],31)
        self.assertEqual(report['months']['april']['full'],30)
        self.assertEqual(report['both_months']['full'],61)
        self.assertEqual(report['both_months']['outcome'],'no_omission_observed')
        self.assertEqual(report['boundary_raw']['outcome'],'no_omission_observed')

    def test_empty_count_on_known_raw_data_is_not_reported_as_no_historical_data(self):
        def response(*args,**kwargs):
            return [] if args[-1] is True else self.archive_response(*args,**kwargs)
        report,calls=self.archive_run(response)
        self.assertEqual(calls,8)
        for month in ('march','april'):
            self.assertEqual(report['months'][month]['outcome'],'inconclusive_count_calibration_failed')
        self.assertNotIn('both_months',report)

    def test_absent_archive_samples_skip_expensive_count_scans(self):
        report,calls=self.archive_run(lambda *args,**kwargs:[])
        self.assertEqual(calls,6)
        self.assertEqual(report['months']['march']['outcome'],'inconclusive_no_raw_sample')
        self.assertEqual(report['boundary_raw']['outcome'],'inconclusive_no_data')

    def test_original_recent_mode_still_compares_frozen_30_minutes(self):
        with TemporaryDirectory() as directory:
            args=SimpleNamespace(run_dir=str(Path(directory)/'run'),end='2026-11-05T12:30:00Z',history_days=0,benchmark_archive=False)
            with patch.object(e,'saved_query',side_effect=self.archive_response) as query, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(e.run(args),0)
            report=json.loads((Path(args.run_dir)/'report.json').read_text())
            self.assertEqual(query.call_count,8)
            self.assertEqual(report['recent_raw']['outcome'],'no_omission_observed')
            self.assertEqual(report['recent_raw']['full'],30)
            self.assertEqual(report['start'],'2026-11-05T12:00:00Z')

    def test_exact_values_and_nanoseconds_survive_identity_comparison(self):
        row=dict(timestamp='2025-03-01T00:00:00.000000194Z',name='t',meta={'vsn':'a'},value=2**64-1)
        same=dict(row,timestamp='2025-02-28T18:00:00.000000194-06:00',value=Decimal(2**64-1))
        self.assertEqual(e.raw_counter([row]),e.raw_counter([same]))
        self.assertNotEqual(e.raw_counter([row]),e.raw_counter([dict(row,value=Decimal('18446744073709551616'))]))
        self.assertNotEqual(e.raw_counter([row]),e.raw_counter([dict(row,timestamp='2025-03-01T00:00:00.000000195Z')]))

    def test_changed_data_is_inconclusive_not_a_storage_bug(self):
        a=e.Counter({'a':1});b=e.Counter({'a':2})
        self.assertEqual(e.comparison(a,b,b)['outcome'],'inconclusive_data_changed')
        self.assertEqual(e.comparison(a,b,a)['missing_from_full'],1)
        self.assertEqual(e.comparison(a,b,a)['outcome'],'range_dependent_difference')
        self.assertEqual(e.comparison(b,b,b)['outcome'],'no_omission_observed')

    def test_counts_combine_per_series_without_losing_large_integers(self):
        row=dict(name='t',meta={'vsn':'a'},value=2**63+1)
        result=e.count_counter([row,dict(row,value=1)])
        self.assertEqual(sum(result.values()),2**63+2)
        for value in (True,-1,Decimal('1.5')):
            with self.assertRaises(ValueError):e.count_counter([dict(row,value=value)])
