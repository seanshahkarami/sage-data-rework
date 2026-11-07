import argparse
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import exp100_aggregation_http as bench


def state(layout,placement='cold'):
    env=dict.fromkeys(bench.SETTINGS,'')
    env.update(COLD_FRONTIER='static',COLD_WAREHOUSE=bench.WAREHOUSE,
        COLD_BORDER='2025-05-01T00:00:00Z' if placement=='cold' else bench.BORDER,
        INFLUXDB_BUCKET='exp95-hot',ARROW_COMPRESSION='zstd')
    return dict(id=layout,image='old' if layout=='baseline' else 'new',running=True,oom_killed=False,
        restarts=0,memory_limit=0,nano_cpus=0,settings=env,memory_events={'oom':0})


def reference():return dict(status='complete',profiled=False,runtime_before={p:state('baseline',p) for p in ('cold','split')})


class HTTPTests(unittest.TestCase):
    def test_repeats_overlap_return_and_limits(self):
        steps=bench.sequence('cold','mixed')
        self.assertEqual(steps[0]['query'],steps[1]['query']);self.assertEqual(steps[0]['query'],steps[-1]['query'])
        self.assertGreater(steps[2]['query']['start'],steps[0]['query']['start'])
        self.assertLess(steps[2]['query']['end'],steps[0]['query']['end'])
        short=bench.sequence('split','mixed')
        self.assertEqual(len(short),10)
        self.assertTrue(any('head' in s['query'] for s in short));self.assertTrue(any('tail' in s['query'] for s in short))
        self.assertTrue(all(s['query']['start']<bench.BORDER<s['query']['end'] for s in short))

    def test_retained_baseline_and_configuration_required(self):
        states={layout:state(layout) for layout in ('baseline','candidate')}
        bench.check_runtime(states,'cold',reference(),False)
        for mutation in ('image','memory','border'):
            s=copy.deepcopy(states)
            if mutation=='image':s['baseline']['image']='not-reference'
            if mutation=='memory':s['candidate']['memory_limit']=123
            if mutation=='border':s['candidate']['settings']['COLD_BORDER']=bench.BORDER
            with self.assertRaises(ValueError):bench.check_runtime(s,'cold',reference(),False)
        with self.assertRaises(ValueError):bench.validate_endpoints(dict(baseline='https://data.sagecontinuum.org/api/v1/query',candidate='http://localhost:1'))

    def test_previous_candidate_can_be_the_next_baseline(self):
        states={layout:state(layout,'split') for layout in ('baseline','candidate')}
        ref=dict(status='complete',profiled=False,placement='split',runtime_before={'candidate':state('baseline','split')})
        bench.check_runtime(states,'split',ref,False,True)
        with self.assertRaisesRegex(ValueError,'placement'):
            bench.check_runtime(states,'cold',ref,False,True)
        ref['runtime_before']['candidate']['image']='wrong'
        with self.assertRaisesRegex(ValueError,'reference image'):
            bench.check_runtime(states,'split',ref,False,True)

    def scenario(self,root,mismatch=False):
        (root/'reference.json').write_text(json.dumps(reference()))
        a=argparse.Namespace(run_dir=root/'run',reference=root/'reference.json',reps=1,timeout=10,
            baseline='http://127.0.0.1:1/api/v1/query',candidate='http://127.0.0.1:2/api/v1/query',
            baseline_container='baseline',candidate_container='candidate',baseline_profiles=None,candidate_profiles=None,
            control=None,format=['ndjson'],placement='cold',workload='short')
        calls=[]
        def receive(endpoint,q,fmt,body,timeout):
            calls.append(endpoint)
            body.write_text(json.dumps(dict(timestamp=q['start'],name='wxt.env.temp',meta={},value=2 if mismatch and ':2/' in endpoint else 1))+'\n')
            return dict(rows=1,bytes=body.stat().st_size,total_seconds=.1,first_result_seconds=.09)
        with patch.object(bench,'runtime',side_effect=lambda layout:state(layout)),patch.object(bench,'receive',side_effect=receive):
            if mismatch:
                with self.assertRaisesRegex(ValueError,'parity failed'):bench.run(a)
            else:bench.run(a)
        return json.loads((a.run_dir/'report.json').read_text()),calls

    def test_reused_whole_sequences_rotate_backend_order(self):
        with tempfile.TemporaryDirectory() as d:
            r,calls=self.scenario(Path(d));self.assertEqual(r['status'],'complete');self.assertEqual(len(r['results']),40)
            self.assertTrue(all(':1/' in c for c in calls[:10]));self.assertTrue(all(':2/' in c for c in calls[10:30]));self.assertTrue(all(':1/' in c for c in calls[30:]))

    def test_mismatch_retains_failed_report_and_body(self):
        with tempfile.TemporaryDirectory() as d:
            r,_=self.scenario(Path(d),True);self.assertEqual(r['status'],'failed');self.assertEqual(len(r['results']),10)
            self.assertTrue((Path(d)/'run/ndjson-0-candidate-series_day_count.ndjson').exists())


if __name__=='__main__':unittest.main()
