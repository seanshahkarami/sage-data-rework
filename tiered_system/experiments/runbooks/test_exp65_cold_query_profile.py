import json
from pathlib import Path
import tempfile
import unittest

from exp65_cold_query_profile import load_timing, summarize_duckdb


class ProfileTests(unittest.TestCase):
    def test_operator_work_separate_from_latency(self):
        report = summarize_duckdb(dict(latency=2, cpu_time=3, planner=.1, optimizer=.02,
            query_name="SELECT 'private filter'", children=[dict(operator_name="ORDER_BY",
                operator_timing=1, operator_cardinality=10, children=[dict(operator_name="TABLE_SCAN",
                    operator_timing=2, operator_cardinality=10, operator_rows_scanned=100)])]))
        self.assertEqual(report["metrics"]["latency"], 2)
        self.assertEqual(report["operator_work_seconds"], dict(sort=1, scan=2))
        self.assertEqual(report["detailed_planning_seconds"], dict(planner=.1, optimizer=.02))
        self.assertNotIn("private filter", json.dumps(report))
        parquet = summarize_duckdb(dict(children=[dict(operator_name="READ_PARQUET", operator_timing=.2)]))
        self.assertEqual(parquet["operator_work_seconds"], dict(scan=.2))

    def test_duckdb20_grouped_metrics(self):
        # Constructed fixture following the 2.0 metrics documentation, not a VM result.
        raw = dict(query=dict(total_time=2.5, cpu_time=4, total_rows_scanned=900,
                              total_intermediate_rows=2000, sql="SELECT 'private filter'"),
                   system=dict(peak_buffer_memory=100, peak_temp_dir_size=200,
                               peak_streaming_buffer_size=50), io=dict(total_bytes_read=300),
                   planner=dict(total_time=.1, binding_time=.08),
                   operator=[dict(type="ORDER_BY", timing=.4, intermediate_rows=100,
                                  children=[dict(type="TABLE_SCAN", timing=1.2,
                                                 intermediate_rows=100, rows_scanned=900,
                                                 row_groups_scanned=3)])])
        report = summarize_duckdb(raw)
        self.assertEqual(report["format"], "duckdb_2_grouped")
        self.assertEqual(report["metrics"]["latency"], 2.5)
        self.assertEqual(report["operator_work_seconds"], dict(sort=.4, scan=1.2))
        self.assertEqual(report["scoped_numeric_metrics"]["system.peak_streaming_buffer_size"], 50)
        self.assertNotIn("rows_returned", report["metrics"])
        self.assertNotIn("private filter", json.dumps(report))
        with self.assertRaises(ValueError):
            summarize_duckdb(dict(query=dict(total_time=2)))

    def test_pairing_status_and_missing_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory)/"query.timing.json"
            raw = dict(status="complete", rows=10, batches=1, phases={},
                       duckdb_file="query.duckdb.json", duckdb_profile_present=True)
            p.with_name(raw["duckdb_file"]).write_text('{"children":[]}')
            for status in ("complete", "incomplete", "failed", "cancelled"):
                raw["status"] = status
                p.write_text(json.dumps(raw))
                self.assertEqual(load_timing(p)["eligible"], status == "complete")
            raw["status"] = "complete"
            raw["engine_version"] = "v2.0.0"
            p.with_name(raw["duckdb_file"]).write_text('{"operator":[],"query":{"total_time":1.2}}')
            p.write_text(json.dumps(raw))
            modern = load_timing(p)
            self.assertTrue(modern["eligible"])
            self.assertEqual(modern["engine_version"], "v2.0.0")
            self.assertEqual(modern["duckdb"]["metrics"]["latency"], 1.2)
            raw["duckdb_profile_present"] = False
            p.write_text(json.dumps(raw))
            self.assertFalse(load_timing(p)["eligible"])
            raw["duckdb_profile_present"] = True
            p.with_name(raw["duckdb_file"]).write_text('')
            p.write_text(json.dumps(raw))
            self.assertIn("profile_error", load_timing(p))
            raw["duckdb_file"] = "../elsewhere.json"
            p.write_text(json.dumps(raw))
            with self.assertRaises(ValueError):
                load_timing(p)


if __name__ == "__main__":
    unittest.main()
