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

    def test_gold_metadata_contract_distinguishes_internal_and_ai_outputs(self):
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
        self.assertEqual(gold_common.AI_ALLOWED_RESTRICTIONS, "ai_allowed")

    def test_surrogate_keys_are_deterministic_and_model_scoped(self):
        case_key = gold_common.stable_key_value("dim_case", "CASE-001")
        self.assertEqual(case_key, gold_common.stable_key_value("dim_case", "CASE-001"))
        self.assertNotEqual(case_key, gold_common.stable_key_value("dim_merchant", "CASE-001"))

    def test_forbidden_ai_columns_are_explicitly_blocked(self):
        forbidden = gold_common.FORBIDDEN_AI_COLUMNS
        for column in {"customer_id", "employee_id", "account_id", "card_id", "party_id", "pan", "email", "phone", "address", "ip_address"}:
            self.assertIn(column, forbidden)

    def test_unknown_members_do_not_cast_arrays(self):
        source = (Path(__file__).parents[1] / "pipeline" / "gold" / "gold_models.py").read_text(encoding="utf-8")
        self.assertNotIn('F.array().cast("array<string>")', source)
        self.assertIn("elif values[field.name] is None:", source)
        self.assertIn('field.dataType.typeName() == "array"', source)
        self.assertNotIn("isinstance(field.dataType, ArrayType)", source)
        self.assertIn("return df.limit(1).select(*expressions)", source)

    def test_gold_uses_output_policy_labels_without_workspace_grants(self):
        root = Path(__file__).parents[1]
        models = (root / "pipeline" / "gold" / "gold_models.py").read_text(encoding="utf-8")
        runner = (root / "pipeline" / "gold" / "gold_all_tables.py").read_text(encoding="utf-8")
        self.assertIn("usage_restrictions=AI_ALLOWED_RESTRICTIONS", models)
        self.assertNotIn("GRANT ", runner)

    def test_gold_validation_allows_empty_optional_facts_and_reconciles_chargebacks(self):
        source = (Path(__file__).parents[1] / "pipeline" / "validation" / "validate_m3_gold.py").read_text(encoding="utf-8")
        self.assertIn('OPTIONAL_FACT_MODELS = {model for model in GOLD_MODELS if model.startswith("fact_")}', source)
        self.assertIn("fact_chargeback does not reconcile to case-scoped disputes", source)
