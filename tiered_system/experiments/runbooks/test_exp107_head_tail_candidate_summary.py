import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import exp107_head_tail_candidate_summary as bench


class SummaryTests(unittest.TestCase):
    def fixture(self,root):
        report=dict(status='complete',profiled=True,measured_repetitions=1,notes=[],trials=[])
        for case in bench.CASES:
            for plan in ('direct','raw_candidates'):
                for rep in (0,1):
                    directory=root/f'{case}-{plan}-{rep}';directory.mkdir()
                    (directory/'rows.json').write_text('[{"time":1}]')
                    (directory/'query.timing.json').write_text(json.dumps({'duckdb_file':'query.duckdb.json'}))
                    (directory/'query.duckdb.json').write_text('{}')
                    report['trials'].append(dict(case=case,plan=plan,repetition=rep,warmup=rep==0,status='complete',parity=True,
                        directory=directory.name,timing='query.timing.json',rows=1,seconds=3 if plan=='direct' else 1))
        path=root/'report.json';path.write_text(json.dumps(report));return path,report

    def profile(self,path):
        native=2 if '-direct-' in str(path) else .5
        if path.parent.name.endswith('-0'):native*=100
        return dict(eligible=True,engine_version='v2.0.0',rows=1,phases={'total_backend_lifetime_seconds':native},
            duckdb={'metrics':{'cpu_time':native*2},'operator_work_seconds':{'projection':native}})

    def test_complete_matrix_excludes_warmups(self):
        with tempfile.TemporaryDirectory() as d:
            path,_=self.fixture(Path(d))
            with patch.object(bench,'load_timing',side_effect=self.profile):result=bench.summarize(path)
            self.assertEqual(len(result['trials']),32);self.assertEqual(len(result['summaries']),16)
            self.assertEqual(len(result['comparisons']),8)
            self.assertTrue(all(x['direct_over_candidate_ratio']==4 for x in result['comparisons']))
            self.assertTrue(all(s['phases']['total_backend_lifetime_seconds'] in (2,.5) for s in result['summaries']))

    def test_failures_incomplete_matrix_and_profile_are_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            path,valid=self.fixture(Path(d))
            for mutation in ('status','parity','duplicate','warmup','path'):
                report=copy.deepcopy(valid)
                if mutation=='status':report['status']='failed'
                if mutation=='parity':report['trials'][0]['parity']=False
                if mutation=='duplicate':report['trials'][-1]['repetition']=0;report['trials'][-1]['warmup']=True
                if mutation=='warmup':report['trials'][0]['warmup']=False
                if mutation=='path':report['trials'][0]['directory']='../outside'
                path.write_text(json.dumps(report))
                with patch.object(bench,'load_timing',side_effect=self.profile),self.assertRaises(ValueError):bench.summarize(path)
            path.write_text(json.dumps(valid))
            with patch.object(bench,'load_timing',return_value=dict(eligible=False)),self.assertRaises(ValueError):bench.summarize(path)


if __name__=='__main__':unittest.main()
