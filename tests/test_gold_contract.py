import unittest


class GoldContractTests(unittest.TestCase):
    def test_case_summary_uses_only_approved_case_fields(self):
        from pipeline.gold.contract import build_case_summary

        self.assertEqual(
            build_case_summary("high", "account_takeover", "open", "2026-07-01"),
            "high priority account takeover investigation opened 2026-07-01; current status open.",
        )
        self.assertEqual(
            build_case_summary(None, None, None, None),
            "unknown priority unknown fraud type investigation opened unknown date; current status unknown.",
        )

    def test_forbidden_field_names_are_detected_case_insensitively(self):
        from pipeline.gold.contract import REQUIRED_INPUT_TABLES, forbidden_field_names

        self.assertEqual(forbidden_field_names(["case_id", "card_last4", "pipeline_run_id"]), [])
        self.assertEqual(
            forbidden_field_names(["customer_id", "author_employee_id", "device_ip"]),
            ["author_employee_id", "customer_id", "device_ip"],
        )
        self.assertEqual(len(REQUIRED_INPUT_TABLES), 16)
        self.assertIn("investigation_cases", REQUIRED_INPUT_TABLES)
        self.assertIn("dispute_reason_codes", REQUIRED_INPUT_TABLES)


if __name__ == "__main__":
    unittest.main()
