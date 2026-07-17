import unittest

from pipeline.bronze.schema_evolution import (
    compare_headers,
    evolve_known_columns,
    is_unknown_field_exception,
    parse_csv_header,
    run_with_schema_retry,
)


class SchemaEvolutionTests(unittest.TestCase):
    def test_unchanged_header(self):
        result = compare_headers(["id", "amount"], ["id", "amount"])
        self.assertFalse(result.has_drift)
        self.assertFalse(result.reordered)

    def test_added_columns_keep_observed_order(self):
        result = compare_headers(["id", "amount"], ["id", "amount", "risk", "note"])
        self.assertEqual(("risk", "note"), result.added_columns)
        self.assertEqual((), result.missing_columns)

    def test_missing_columns_keep_contract_order(self):
        result = compare_headers(["id", "amount", "currency"], ["id"])
        self.assertEqual(("amount", "currency"), result.missing_columns)

    def test_reordering_is_not_drift(self):
        result = compare_headers(["id", "amount"], ["amount", "id"])
        self.assertFalse(result.has_drift)
        self.assertTrue(result.reordered)

    def test_parse_quoted_header_and_bom(self):
        self.assertEqual(
            ["id", "display,name", "amount"],
            parse_csv_header('\ufeffid,"display,name",amount\n1,"Doe, Jane",4'),
        )

    def test_evolve_retains_missing_historical_columns(self):
        self.assertEqual(
            ["id", "amount", "risk"],
            evolve_known_columns(["id", "amount"], ["id", "risk"]),
        )

    def test_unknown_field_detection_walks_exception_chain(self):
        inner = RuntimeError("[UNKNOWN_FIELD_EXCEPTION] new field: risk")
        outer = RuntimeError("stream stopped")
        outer.__cause__ = inner
        self.assertTrue(is_unknown_field_exception(outer))

    def test_retry_expected_schema_failure(self):
        calls = []

        def operation():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("UnknownFieldException: risk")
            return "ok"

        self.assertEqual("ok", run_with_schema_retry(operation, 2))
        self.assertEqual(2, len(calls))

    def test_retry_does_not_swallow_unrelated_failure(self):
        with self.assertRaisesRegex(RuntimeError, "storage unavailable"):
            run_with_schema_retry(
                lambda: (_ for _ in ()).throw(RuntimeError("storage unavailable")),
                3,
            )

    def test_retry_limit_is_enforced(self):
        with self.assertRaisesRegex(RuntimeError, "UnknownFieldException"):
            run_with_schema_retry(
                lambda: (_ for _ in ()).throw(RuntimeError("UnknownFieldException")),
                2,
            )


if __name__ == "__main__":
    unittest.main()
