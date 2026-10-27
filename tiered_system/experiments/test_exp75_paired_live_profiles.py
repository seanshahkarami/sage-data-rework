import json
from pathlib import Path
import tempfile
import unittest

from exp75_paired_live_profiles import fresh_profile,profile_paths
from exp73_plugin_live_compare import matched_coverage,validate_candidate_audit


class TestPairedProfiles(unittest.TestCase):
    def test_only_fresh_complete_pair_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'old.timing.json').write_text('{}');before=profile_paths(root)
            native={'operator':[{'type':'TABLE_SCAN','timing':.25,'rows_scanned':10}],'query':{'total_time':.3}}
            (root/'new.duckdb.json').write_text(json.dumps(native))
            timing=dict(status='complete',rows=10,batches=1,phases={},duckdb_file='new.duckdb.json',duckdb_profile_present=True)
            (root/'new.timing.json').write_text(json.dumps(timing))
            result=fresh_profile(root,before,timeout=0)
            self.assertTrue(result['eligible']);self.assertEqual(result['duckdb']['operator_work_seconds']['scan'],.25)
            (root/'extra.timing.json').write_text(json.dumps(timing))
            with self.assertRaisesRegex(ValueError,'multiple'):fresh_profile(root,before,timeout=0)

    def test_missing_or_incomplete_pair_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with self.assertRaisesRegex(ValueError,'no fresh'):fresh_profile(root,set(),timeout=0)

    def test_coverage_and_time_batch_proof_required(self):
        baseline=dict(layout='time',rows=4,min_timestamp_ns=1740787200000000194,max_timestamp_ns=1740787200000000200,
                      snapshots=1,value_type='variant',value_field_id=11)
        candidate=dict(baseline,layout='plugin-vsn-time',one_group_per_plugin=True,file_row_limit=10_000_000,
                       mixed_plugin_groups=0,ordering_checked=True,batch_order='time',time_batch_ranges_checked=True)
        matched_coverage(baseline,candidate);validate_candidate_audit(candidate)
        with self.assertRaisesRegex(ValueError,'snapshot'):matched_coverage(dict(baseline,snapshots=61),candidate)
        with self.assertRaisesRegex(ValueError,'ranges'):validate_candidate_audit(dict(candidate,time_batch_ranges_checked=False))
        bounded=dict(candidate,one_group_per_plugin=False,bounded_plugin_groups=True,plugin_pure_groups=True,
                     row_group_row_limit=100000,max_row_group_rows=100000)
        validate_candidate_audit(bounded)
        with self.assertRaisesRegex(ValueError,'maximum'):validate_candidate_audit(dict(bounded,max_row_group_rows=100001))
        with self.assertRaisesRegex(ValueError,'purity'):validate_candidate_audit(dict(bounded,plugin_pure_groups=False))


if __name__=='__main__':unittest.main()
