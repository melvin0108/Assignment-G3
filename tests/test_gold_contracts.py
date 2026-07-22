"""Local semantic contract checks for the Databricks-only Gold implementation."""

import unittest
from pathlib import Path

import yaml

from pipeline.gold import gold_common


ROOT = Path(__file__).parents[1]
MODEL_DIR = ROOT / "docs" / "models" / "gold"
ROUTING_PATH = MODEL_DIR / "questions-to-metrics.yaml"

EXPECTED_PRIMARY_KEYS = {
    "dim_date": ["date_key"],
    "dim_case": ["case_id"],
    "dim_merchant": ["merchant_id"],
    "dim_channel": ["channel_code"],
    "dim_dispute_reason": ["reason_code"],
    "dim_currency": ["currency_code"],
    "fact_case_transaction": ["case_id", "transaction_id"],
    "fact_authorization_attempt": ["case_id", "attempt_id"],
    "fact_dispute": ["case_id", "dispute_id"],
    "fact_chargeback": ["case_id", "chargeback_id"],
    "fact_fraud_alert": ["case_id", "alert_id"],
    "fact_investigation_note": ["case_id", "note_id"],
    "fact_case_party_summary": ["case_id", "party_type", "role"],
    "investigation_context": ["case_id"],
}

EXPECTED_METRICS = {
    "case_count", "transaction_count", "transaction_amount_total",
    "transaction_amount_average", "authorization_attempt_count",
    "authorization_approval_rate", "authorization_decline_rate", "dispute_count",
    "dispute_amount_total", "chargeback_count", "chargeback_amount_total",
    "fraud_alert_count", "fraud_alert_score_average", "safe_note_count", "party_count",
}

LEGACY_HASHED_COLUMNS = {
    "case_key", "merchant_key", "channel_key", "currency_key", "dispute_reason_key",
    "case_transaction_key", "authorization_attempt_key", "dispute_key", "chargeback_key",
    "fraud_alert_key", "investigation_note_key", "case_party_summary_key",
}


def load_contracts():
    return {
        document["model"]: document
        for path in sorted(MODEL_DIR.glob("*.yml"))
        for document in [yaml.safe_load(path.read_text(encoding="utf-8"))]
    }


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

    def test_gold_metadata_contract_marks_every_output_ai_allowed(self):
        self.assertEqual(gold_common.STANDARD_METADATA_COLUMNS, {
        "pipeline_run_id",
        "batch_id",
        "last_refreshed_at",
        "quality_status",
        "warning_flags",
        "source_references",
        "usage_restrictions",
        })
        self.assertEqual(gold_common.USAGE_RESTRICTIONS, "ai_allowed")
        self.assertEqual(gold_common.AI_ALLOWED_RESTRICTIONS, "ai_allowed")

    def test_sha256_helpers_are_absent(self):
        source = ((ROOT / "pipeline" / "gold" / "gold_common.py").read_text(encoding="utf-8")
            + (ROOT / "pipeline" / "gold" / "gold_models.py").read_text(encoding="utf-8"))
        for token in ("hashlib", "sha256", "stable_key_value", "F.sha2", "def _key"):
            self.assertNotIn(token, source)

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

    def test_gold_uses_ai_allowed_policy_labels_without_workspace_grants(self):
        root = Path(__file__).parents[1]
        models = (root / "pipeline" / "gold" / "gold_models.py").read_text(encoding="utf-8")
        runner = (root / "pipeline" / "gold" / "gold_all_tables.py").read_text(encoding="utf-8")
        self.assertIn("usage_restrictions=AI_ALLOWED_RESTRICTIONS", models)
        self.assertNotIn("GRANT ", runner)

    def test_gold_validation_allows_empty_optional_facts_and_reconciles_chargebacks(self):
        source = (Path(__file__).parents[1] / "pipeline" / "validation" / "validate_m3_gold.py").read_text(encoding="utf-8")
        self.assertIn('OPTIONAL_FACT_MODELS = {model for model in GOLD_MODELS if model.startswith("fact_")}', source)
        self.assertIn("fact_chargeback does not reconcile to case-scoped disputes", source)


class GoldSemanticContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = load_contracts()

    def test_natural_primary_keys_match_declared_grains(self):
        self.assertEqual(set(self.contracts), set(EXPECTED_PRIMARY_KEYS))
        for model, expected_key in EXPECTED_PRIMARY_KEYS.items():
            self.assertEqual(self.contracts[model]["primary_key"], expected_key, model)

    def test_model_contracts_use_the_shared_semantic_schema(self):
        required_model_fields = {
            "schema_version", "model", "table", "description", "domain", "type",
            "grain", "tags", "synonyms", "sources", "ai_access", "primary_key",
            "relationships", "columns", "dimensions", "metrics",
        }
        required_column_fields = {
            "name", "physical_type", "description", "semantic_role", "tags", "synonyms",
        }
        required_metric_fields = {
            "id", "description", "expression", "aggregation", "format",
            "supported_dimensions", "time_field",
        }
        metric_ids = set()
        for model, contract in self.contracts.items():
            self.assertEqual(set(contract), required_model_fields, model)
            self.assertEqual(contract["schema_version"], "1.0.0", model)
            self.assertEqual(contract["table"], f"gold.{model}", model)
            self.assertEqual(contract["ai_access"], {
                "classification": "ai_allowed",
                "pii_safe": True,
            })
            self.assertTrue(contract["description"], model)
            self.assertTrue(contract["tags"], model)
            self.assertTrue(contract["synonyms"], model)
            self.assertTrue(contract["sources"], model)

            columns = {column["name"]: column for column in contract["columns"]}
            self.assertTrue(columns, model)
            self.assertFalse(set(columns) & LEGACY_HASHED_COLUMNS, model)
            for column in columns.values():
                self.assertTrue(required_column_fields <= set(column), f"{model}.{column['name']}")
                self.assertTrue(column["description"], f"{model}.{column['name']}")
            self.assertTrue(set(contract["primary_key"]) <= set(columns), model)

            dimensions = {dimension["id"]: dimension for dimension in contract["dimensions"]}
            for dimension in dimensions.values():
                self.assertEqual(
                    set(dimension), {"id", "column", "description", "synonyms"},
                    f"{model}.{dimension['id']}",
                )
                self.assertIn(dimension["column"], columns, f"{model}.{dimension['id']}")

            for relationship in contract["relationships"]:
                self.assertEqual(
                    set(relationship), {"target_model", "cardinality", "join_columns"}, model,
                )
                target_model = relationship["target_model"].removeprefix("gold.")
                self.assertIn(target_model, self.contracts)
                target_columns = {column["name"] for column in self.contracts[target_model]["columns"]}
                for join in relationship["join_columns"]:
                    self.assertEqual(set(join), {"from", "to"})
                    self.assertIn(join["from"], columns)
                    self.assertIn(join["to"], target_columns)

            for metric in contract["metrics"]:
                self.assertEqual(set(metric), required_metric_fields, f"{model}.{metric['id']}")
                self.assertNotIn(metric["id"], metric_ids)
                metric_ids.add(metric["id"])
                self.assertTrue(set(metric["supported_dimensions"]) <= set(dimensions), metric["id"])
                if metric["time_field"] is not None:
                    self.assertIn(metric["time_field"], columns, metric["id"])
                if "amount" in metric["id"]:
                    self.assertEqual(metric["format"], "currency_by_currency_code")
                    self.assertIn("currency_code", metric["supported_dimensions"])

        self.assertEqual(metric_ids, EXPECTED_METRICS)

    def test_question_routing_is_bilingual_and_referentially_valid(self):
        routing = yaml.safe_load(ROUTING_PATH.read_text(encoding="utf-8"))
        self.assertEqual(routing["schema_version"], "1.0.0")
        self.assertEqual(routing["supported_languages"], ["vi", "en"])
        self.assertEqual(len(routing["patterns"]), 9)

        metric_locations = {
            metric["id"]: model
            for model, contract in self.contracts.items()
            for metric in contract["metrics"]
        }
        pattern_ids = [pattern["id"] for pattern in routing["patterns"]]
        self.assertEqual(len(pattern_ids), len(set(pattern_ids)))

        for pattern in routing["patterns"]:
            self.assertEqual(
                set(pattern), {
                    "id", "intent", "query_mode", "examples", "keywords", "synonyms",
                    "metric_ids", "dimensions", "filters", "time_dimension",
                    "primary_table", "supporting_tables",
                },
                pattern["id"],
            )
            for language in routing["supported_languages"]:
                self.assertTrue(pattern["examples"][language], f"{pattern['id']}:{language}")
                self.assertTrue(pattern["keywords"][language], f"{pattern['id']}:{language}")
                self.assertTrue(pattern["synonyms"][language], f"{pattern['id']}:{language}")

            routed_tables = [pattern["primary_table"], *pattern["supporting_tables"]]
            routed_contracts = [self.contracts[table.removeprefix("gold.")] for table in routed_tables]
            for contract in routed_contracts:
                self.assertEqual(contract["ai_access"]["classification"], "ai_allowed")

            available_dimensions = {
                dimension["id"]
                for contract in routed_contracts
                for dimension in contract["dimensions"]
            }
            self.assertTrue(set(pattern["dimensions"]) <= available_dimensions, pattern["id"])
            for filter_spec in pattern["filters"]:
                self.assertEqual(set(filter_spec), {"dimension", "operators"})
                self.assertIn(filter_spec["dimension"], available_dimensions)
            if pattern["time_dimension"] is not None:
                self.assertIn(pattern["time_dimension"], available_dimensions)

            if pattern["query_mode"] == "analytics":
                self.assertTrue(pattern["metric_ids"], pattern["id"])
                self.assertTrue(set(pattern["metric_ids"]) <= set(metric_locations), pattern["id"])
                routed_models = {table.removeprefix("gold.") for table in routed_tables}
                self.assertTrue(
                    {metric_locations[metric_id] for metric_id in pattern["metric_ids"]} <= routed_models,
                    pattern["id"],
                )
            else:
                self.assertEqual(pattern["query_mode"], "detail")
                self.assertEqual(pattern["metric_ids"], [])
                self.assertEqual(pattern["primary_table"], "gold.investigation_context")

    def test_m3_validation_covers_natural_grain_schema_and_ai_policy(self):
        source = (ROOT / "pipeline" / "validation" / "validate_m3_gold.py").read_text(encoding="utf-8")
        for token in (
            "EXPECTED_PRIMARY_KEYS", "physical_type", "usage_restrictions", "UNKNOWN",
            "legacy hashed columns", "investigation_context must have one row per case_id",
        ):
            self.assertIn(token, source)
