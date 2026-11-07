import argparse
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import exp103_head_tail_performance as bench
from exp70_v3_live_baseline import timestamp_ns


def state(role, profiled=False):
    env=dict.fromkeys(bench.SETTINGS,'')
    env.update(COLD_FRONTIER='static',COLD_WAREHOUSE=bench.WAREHOUSE,
        COLD_BORDER='2025-05-01T00:00:00Z' if role=='cold' else '2025-03-01T12:00:00Z',
        COLD_ASYNC_THREADS='8',COLD_READ_AHEAD_DEPTH='-1',ARROW_COMPRESSION='zstd',
        COLD_AGGREGATE_MAX_STAGING_BYTES='17179869184',INFLUXDB_BUCKET='exp95-hot',
        COLD_PROFILE_DIR='/profiles' if profiled else '')
    return dict(id=role,image='same-current-image',running=True,oom_killed=False,restarts=0,
        memory_limit=0,nano_cpus=0,settings=env,memory_events={'oom':0})


def row(value=1):
    q=next(iter(bench.cases().values()))
    return dict(time=timestamp_ns(q['start'])+194,name='wxt.env.temp',meta={'vsn':'W0A0'},value=value)


class BenchmarkTests(unittest.TestCase):
    def test_queries_are_unaggregated_crossing_limits(self):
        cases=bench.cases();self.assertEqual(len(cases),16)
        for q in cases.values():
            self.assertNotIn('experimental_func',q);self.assertNotIn('experimental_window',q)
            self.assertTrue(('head' in q)^('tail' in q))
            self.assertLess(q['start'],'2025-03-01T12:00:00Z');self.assertGreater(q['end'],'2025-03-01T12:00:00Z')

    def test_scalar_parity_is_exact_except_float_tolerance(self):
        bench.compare_rows([row(2**64-1)],[row(2**64-1)])
        for value in ['text',True,None,1.25]:bench.compare_rows([row(value)],[row(value)])
        for a,b in [(2**64-1,float(2**64-1)),(True,1),('1',1),(1,2)]:
            with self.assertRaises(ValueError):bench.compare_rows([row(a)],[row(b)])
        bench.compare_rows([row(1.0)],[row(1.0+1e-13)])

    def test_per_series_bounds_duplicates_and_order(self):
        q=next(iter(bench.cases().values()));r=row()
        bench.validate_rows(q,[r])
        for rows in [[],[r,r],[dict(r,time=timestamp_ns(q['end']))],[dict(r,name='wrong')],
                     [r,dict(r,time=r['time']+1)]]:
            with self.assertRaises(ValueError):bench.validate_rows(q,rows)
        # One row per each of two series is valid with head=1.
        fleet=dict(q,filter={});bench.validate_rows(fleet,[r,dict(r,meta={'vsn':'OTHER'})])

    def test_configuration_reference_and_control_are_pinned(self):
        states={r:state(r) for r in ('cold','split')}
        ref=dict(status='complete',profiled=False,placement='split',runtime_before={'candidate':state('split')})
        bench.check_reference(states,ref,False)
        wrong=copy.deepcopy(states);wrong['split']['image']='other'
        with self.assertRaises(ValueError):bench.check_reference(wrong,ref,False)
        wrong=copy.deepcopy(states);wrong['split']['settings']['INFLUXDB_BUCKET']='wrong'
        with self.assertRaises(ValueError):bench.check_reference(wrong,ref,False)
        selected=[next(iter(bench.cases()))]
        control=dict(experiment='exp103',status='complete',profiled=False,selected_cases=selected,
            queries={selected[0]:bench.cases()[selected[0]]},runtime_before=states)
        bench.check_control(control,{r:state(r,True) for r in states},selected)
        wrong=copy.deepcopy(control);wrong['queries'][selected[0]]['head']=3
        with self.assertRaises(ValueError):bench.check_control(wrong,states,selected)

    def scenario(self,root,mismatch=False,profiled=False,control=None):
        reference=root/'reference.json'
        reference.write_text(json.dumps(dict(status='complete',profiled=False,placement='split',runtime_before={'candidate':state('split')})))
        dirs={r:root/(r+'-profiles') for r in ('cold','split')}
        if profiled:
            for p in dirs.values():p.mkdir()
        a=argparse.Namespace(run_dir=root/('profiles-run' if profiled else 'control-run'),reference=reference,
            cold='http://127.0.0.1:1/api/v1/query',split='http://127.0.0.1:2/api/v1/query',
            cold_container='cold',split_container='split',reps=1,timeout=10,
            cold_profiles=dirs['cold'] if profiled else None,split_profiles=dirs['split'] if profiled else None,
            control=control,case=[next(iter(bench.cases()))])
        calls=[]
        def receive(endpoint,q,fmt,body,timeout):
            calls.append((endpoint,fmt));body.write_text(json.dumps([row(2 if mismatch and ':2/' in endpoint else 1)]))
            return dict(rows=1,bytes=body.stat().st_size,total_seconds=.1,first_result_seconds=.09)
        def records(path,fmt):return json.loads(path.read_text())
        profile=dict(rows=1,engine_version='v2.0.0',phases={k:1 for k in ('aggregate_hot_rows','aggregate_input_bytes','aggregate_parquet_bytes','aggregate_hot_read_stage_seconds','aggregate_staging_complete_since_backend_start_seconds')},duckdb={'metrics':{'latency':.01}})
        with patch.object(bench,'runtime',side_effect=lambda r:state(r,profiled)),patch.object(bench,'receive',side_effect=receive),patch.object(bench,'records',side_effect=records),patch.object(bench,'profile_paths',return_value=set()),patch.object(bench,'fresh_profile',return_value=profile):
            if mismatch:
                with self.assertRaises(ValueError):bench.run(a)
            else:bench.run(a)
        return a.run_dir,json.loads((a.run_dir/'report.json').read_text()),calls

    def test_control_and_diagnostics_retain_bodies_and_rotate(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);p,control,calls=self.scenario(root)
            self.assertEqual(control['status'],'complete');self.assertEqual(len(control['results']),8)
            self.assertEqual(len(control['summaries']),4);self.assertEqual(sum(r['warmup'] for r in control['results']),4)
            self.assertEqual(calls[0],('http://127.0.0.1:1/api/v1/query','arrow'))
            self.assertEqual(calls[4],('http://127.0.0.1:1/api/v1/query','ndjson'))
            _,profile,_=self.scenario(root,profiled=True,control=p/'report.json')
            self.assertEqual(profile['status'],'complete');self.assertEqual(len(profile['results']),4)
            self.assertTrue(all(r['format']=='arrow' for r in profile['results']))

    def test_failure_retains_report_and_response(self):
        with tempfile.TemporaryDirectory() as d:
            p,report,_=self.scenario(Path(d),mismatch=True)
            self.assertEqual(report['status'],'failed');self.assertEqual(len(report['results']),2)
            self.assertTrue(list(p.glob('*-split-0.arrow')))


if __name__=='__main__':unittest.main()
