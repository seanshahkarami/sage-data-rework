import copy
import json
import unittest
from pathlib import Path

from exp82_whole_range_profiles import SETTINGS,CASES,validate_settings,reference_signatures,healthy,median_mapping


class TestWholeRangeProfiles(unittest.TestCase):
    def test_settings_require_adopted_warehouse_and_correct_profiling_phase(self):
        validate_settings(dict(SETTINGS,COLD_PROFILE_DIR=''),False)
        validate_settings(dict(SETTINGS,COLD_PROFILE_DIR='/profiles'),True)
        with self.assertRaisesRegex(ValueError,'profiling'):validate_settings(dict(SETTINGS,COLD_PROFILE_DIR='/profiles'),False)
        with self.assertRaisesRegex(ValueError,'adopted'):validate_settings(dict(SETTINGS,COLD_WAREHOUSE='old-warehouse'),False)
        validate_settings(dict(SETTINGS,COLD_PROFILE_DIR='',COLD_UNIFORM_METADATA='true'),False,True)
        with self.assertRaisesRegex(ValueError,'uniform-metadata'):validate_settings(dict(SETTINGS,COLD_UNIFORM_METADATA='true'),False)
        with self.assertRaisesRegex(ValueError,'uniform-metadata'):validate_settings(dict(SETTINGS,COLD_UNIFORM_METADATA='false'),False,True)

    def test_actual_reference_has_all_selected_signatures_and_exact_queries(self):
        path=Path(__file__).parent/'results/full-layout-1792962354/http-arrow.json'
        report=json.loads(path.read_text());signatures=reference_signatures(report,list(CASES))
        self.assertEqual(signatures['2025-03_series_month']['rows'],25940207)
        self.assertEqual(signatures['2025-04_series_month']['rows'],31189045)
        changed=copy.deepcopy(report)
        for result in changed['results']:
            if result['case']=='2025-03_series_month':result.pop('validation',None)
        with self.assertRaisesRegex(ValueError,'missing'):reference_signatures(changed,['2025-03_series_month'])
        with self.assertRaisesRegex(ValueError,'incomplete'):reference_signatures(dict(report,status='running'),list(CASES))

    def test_restarts_oom_events_and_configuration_changes_reject_measurement(self):
        before=dict(container_id='one',image='sha256:image',settings=SETTINGS,status='running',restarts=0,
                    oom_killed=False,cgroup_memory_events=dict(oom=0,oom_kill=0))
        healthy(before,copy.deepcopy(before))
        for changed in (dict(before,restarts=1),dict(before,oom_killed=True),dict(before,status='exited'),
                        dict(before,container_id='two'),dict(before,cgroup_memory_events={'oom':1,'oom_kill':0})):
            with self.assertRaises(ValueError):healthy(before,changed)

    def test_only_numeric_common_profile_metrics_are_aggregated(self):
        self.assertEqual(median_mapping([dict(seconds=2,rows=10,flag=True),dict(seconds=4,rows=10,extra=3,flag=False)]),
                         dict(seconds=3,rows=10))


if __name__=='__main__':unittest.main()
