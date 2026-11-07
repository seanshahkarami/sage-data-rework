import argparse
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import exp108_head_tail_paired as bench
from exp108_prepare_head_tail_candidate import prepare, digest
from test_exp103_head_tail_performance import state, row

def source_fixture(root):
    # Archived preparation-tool contract only; runtime SQL semantics are covered
    # by the API's independent oracle, not by this synthetic source fixture.
    files = {
        'go.mod':'module beehive-data-api\n', 'go.sum':'', 'Dockerfile':'FROM scratch\n',
        '.dockerignore':'', 'main.go':'package main\n',
        'observability/grafana/beehive-data-api.json':'{}\n',
        'observability/prometheus/alerts.yaml':'groups: []\n',
        'internal/backend/cold/selection_sql.go':'package cold\nfunc buildSelectionSQL() {}\n',
        'internal/backend/cold/selection_isolation_test.go':'package cold\nfunc probe() { buildSelectionSQL() }\n',
        'internal/backend/cold/selection_reduce_probe_test.go':
            'package cold\nfunc probe() { buildSelectionSQL() }\n'
            'func buildRawCandidateSelection(q *model.Query, source string, now time.Time) (string, error) {\n'
            'return buildSelectionSQL(q, "SELECT * FROM raw_candidates", now)\n}\n'
            'func TestRawCandidateSelectionParity(t *testing.T) {}\n',
    }
    for name, text in files.items():
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(text)
    return root


def states(profiled=False):
    result = {role: state('split', profiled) for role in ('baseline', 'candidate')}
    for role, value in result.items(): value.update(id=role, image=role+'-image')
    return result


def reference():
    return dict(experiment='exp103', status='complete', profiled=False, queries=bench.cases(),
                runtime_before={'split':state('split')})


class PairedTests(unittest.TestCase):
    def setUp(self):
        self.fixture=tempfile.TemporaryDirectory()
        self.addCleanup(self.fixture.cleanup)
        self.api=source_fixture(Path(self.fixture.name)/'api')

    def test_source_snapshot_is_exact_and_checkout_untouched(self):
        before = digest(self.api/'internal/backend/cold/selection_sql.go')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'source'; manifest=prepare(self.api,root)
            self.assertEqual(len(manifest['changed']),4)
            self.assertEqual(bench.check_snapshot(root/'manifest.json'),manifest)
            self.assertEqual(before,digest(self.api/'internal/backend/cold/selection_sql.go'))
            self.assertEqual(digest(root/'candidate/go.mod'),digest(self.api/'go.mod'))
            for asset in ('observability/grafana/beehive-data-api.json',
                          'observability/prometheus/alerts.yaml'):
                self.assertEqual(digest(root/'baseline'/asset),digest(self.api/asset))
                self.assertEqual(digest(root/'candidate'/asset),digest(self.api/asset))
            wrapper=(root/'candidate/internal/backend/cold/selection_candidate.go').read_text()
            self.assertIn('func buildSelectionSQL(',wrapper)
            self.assertIn('buildCanonicalSelectionSQL(q, "SELECT * FROM raw_candidates", now)',wrapper)
            with self.assertRaises(ValueError):prepare(self.api,root)
            (root/'candidate/main.go').write_text('changed')
            with self.assertRaises(ValueError):bench.check_snapshot(root/'manifest.json')

    def test_runtime_and_control_reject_wrong_placement_image_and_inputs(self):
        before=states();ref=reference();bench.check_reference(before,ref,False)
        for change in ({'image':'baseline-image'},{'id':'baseline'},{'memory_limit':1},
                       {'settings':dict(before['candidate']['settings'], COLD_BORDER='2025-05-01T00:00:00Z')},
                       {'settings':dict(before['candidate']['settings'], INFLUXDB_BUCKET='other')}):
            bad=copy.deepcopy(before);bad['candidate'].update(change)
            with self.assertRaises(ValueError):bench.check_reference(bad,ref,False)
        selected=[next(iter(bench.cases()))]
        control=dict(experiment='exp108',status='complete',profiled=False,selected_cases=selected,
                     queries={k:bench.cases()[k] for k in selected},runtime_before=before)
        bench.check_control(control,states(True),selected)
        broken=states(True);broken['candidate']['image']='other'
        with self.assertRaises(ValueError):bench.check_control(control,broken,selected)

    def test_small_paired_run_warmups_parity_and_failure_report(self):
        for mismatch in (False,True):
            with self.subTest(mismatch=mismatch),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);source=root/'source';prepare(self.api,source)
                name=next(iter(bench.cases()));ref=reference()
                body=root/'reference.ndjson';body.write_text('oracle')
                ref['results']=[dict(case=name,role='cold',format='ndjson',warmup=False,parity_passed=True,
                                     body=body.name,body_sha256=digest(body))]
                path=root/'reference.json';path.write_text(json.dumps(ref))
                args=argparse.Namespace(run_dir=root/'run',reference=path,source_manifest=source/'manifest.json',
                    baseline='http://127.0.0.1:18481/api/v1/query',candidate='http://127.0.0.1:18482/api/v1/query',
                    baseline_container='baseline',candidate_container='candidate',
                    baseline_image='baseline-image',candidate_image='candidate-image',
                    baseline_profiles=None,candidate_profiles=None,control=None,reps=1,timeout=10,case=[name])
                def receive(url,q,fmt,path,timeout):
                    path.write_text('response');return dict(rows=1,total_seconds=2 if '18481' in url else 1)
                def records(path,fmt):
                    return [row(999 if mismatch and 'candidate' in path.name else 1)]
                with patch.object(bench,'runtime',side_effect=lambda role:states()[role]),patch.object(bench,'receive',side_effect=receive),patch.object(bench,'records',side_effect=records):
                    if mismatch:
                        with self.assertRaises(ValueError):bench.run(args)
                    else:bench.run(args)
                report=json.loads((root/'run/report.json').read_text())
                self.assertEqual(report['status'],'failed' if mismatch else 'complete')
                if not mismatch:
                    self.assertEqual(len(report['results']),8)
                    self.assertEqual(sum(r['warmup'] for r in report['results']),4)
                    self.assertEqual({r['speedup'] for r in report['comparisons']},{2})
                    args.control=root/'run/report.json';args.run_dir=root/'profiles-run'
                    args.baseline_profiles=root/'baseline-profiles';args.baseline_profiles.mkdir()
                    args.candidate_profiles=root/'candidate-profiles';args.candidate_profiles.mkdir()
                    profile=dict(rows=1,engine_version='v2.0.0',phases=dict.fromkeys((
                        'aggregate_hot_rows','aggregate_input_bytes','aggregate_parquet_bytes',
                        'aggregate_hot_read_stage_seconds','aggregate_staging_complete_since_backend_start_seconds'),1),
                        duckdb={'metrics':{'cpu_time':1}})
                    with patch.object(bench,'runtime',side_effect=lambda role:states(True)[role]),patch.object(bench,'receive',side_effect=receive),patch.object(bench,'records',side_effect=records),patch.object(bench,'profile_paths',return_value=set()),patch.object(bench,'fresh_profile',return_value=profile):
                        bench.run(args)
                    diagnostic=json.loads((args.run_dir/'report.json').read_text())
                    self.assertEqual(diagnostic['status'],'complete')
                    self.assertEqual(len(diagnostic['results']),4)
                    body.write_text('tampered')
                    with self.assertRaises(ValueError):bench.reference_rows(path,ref,name)


if __name__=='__main__':unittest.main()
