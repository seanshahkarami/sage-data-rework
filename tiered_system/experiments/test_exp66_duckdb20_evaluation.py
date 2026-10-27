import unittest

from exp66_duckdb20_evaluation import signature, sql_quote, validate_version


class EvaluationTests(unittest.TestCase):
    def test_rejects_mislabeled_engine_and_wrong_codec(self):
        report = dict(engine=dict(version="v1.5.6"), compression="zstd")
        validate_version("1.5.6", report)
        with self.assertRaises(ValueError):
            validate_version("2.0.0", report)
        report["compression"] = "none"
        with self.assertRaises(ValueError):
            validate_version("1.5.6", report)

    def test_signature_includes_values_and_schema(self):
        row = dict(rows=3, schema="public four fields", row_hash_sum="1", row_hash_xor="2", total_seconds=8)
        other = dict(row, total_seconds=1)
        self.assertEqual(signature(row), signature(other))
        self.assertNotEqual(signature(row), signature(dict(other, row_hash_sum="3")))
        self.assertNotEqual(signature(row), signature(dict(other, schema="different")))
        self.assertEqual(sql_quote("operator's/file"), "'operator''s/file'")


if __name__ == "__main__":
    unittest.main()
