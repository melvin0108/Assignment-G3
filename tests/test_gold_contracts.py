"""Local contract checks for the Databricks-only Gold implementation."""

import unittest
from pathlib import Path

from pipeline.gold import gold_common


class GoldContractTests(unittest.TestCase):
    def test_gold_model_inventory_matches_the_plan(self):
        self.assertEqual(gold_common.GOLD_MODELS, {
        "dim_date",
        "dim_case",
        "dim_merchant",
        "dim_channel",
        "dim_dispute_reason",
        "dim_currency",
        "fact_case_transaction",
        "fact_authorization_attempt",
        "fact_dispute",
        "fact_chargeback",
        "fact_fraud_alert",
        "fact_investigation_note",
        "fact_case_party_summary",
        "investigation_context",
        })

    def test_gold_metadata_contract_is_uniform_and_ai_restricted(self):
        self.assertEqual(gold_common.STANDARD_METADATA_COLUMNS, {
        "pipeline_run_id",
        "batch_id",
        "last_refreshed_at",
        "quality_status",
        "warning_flags",
        "source_references",
        "usage_restrictions",
        })
        self.assertEqual(gold_common.USAGE_RESTRICTIONS, "internal_only")

    def test_surrogate_keys_are_deterministic_and_model_scoped(self):
        case_key = gold_common.stable_key_value("dim_case", "CASE-001")
        self.assertEqual(case_key, gold_common.stable_key_value("dim_case", "CASE-001"))
        self.assertNotEqual(case_key, gold_common.stable_key_value("dim_merchant", "CASE-001"))

    def test_forbidden_ai_columns_are_explicitly_blocked(self):
        forbidden = gold_common.FORBIDDEN_AI_COLUMNS
        for column in {"customer_id", "employee_id", "account_id", "card_id", "party_id", "pan", "email", "phone", "address", "ip_address"}:
            self.assertIn(column, forbidden)

    def test_unknown_members_build_array_values_without_an_array_cast(self):
        source = (Path(__file__).parents[1] / "pipeline" / "gold" / "gold_models.py").read_text(encoding="utf-8")
        self.assertIn("warnings=None", source)
        self.assertIn('F.array().cast("array<string>")', source)
        self.assertIn("if isinstance(field.dataType, ArrayType) and value is not None:", source)
