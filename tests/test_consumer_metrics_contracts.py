"""Static contracts for the authenticated Consumer metrics data product."""

from pathlib import Path

import yaml
import pytest

from pipeline.gold.gold_common import GOLD_MODELS, assert_no_forbidden_columns


ROOT = Path(__file__).parents[1]
CONTRACT_DIR = ROOT / "docs" / "models" / "gold"
METRIC_DIR = ROOT / "consumer_metrics_view"

BROKER_MODELS = {
    "dim_consumer_account": {
        "customer_id",
        "account_id",
        "account_reference",
        "product_type",
        "account_status",
        "currency_code",
        "open_date",
    },
    "dim_consumer_card": {
        "customer_id",
        "account_id",
        "card_id",
        "account_reference",
        "card_reference",
        "card_last_four",
        "card_type",
        "expiry_month",
        "card_status",
    },
    "fact_consumer_transaction": {
        "customer_id",
        "account_id",
        "card_id",
        "transaction_reference",
        "account_reference",
        "card_reference",
        "merchant_name",
        "merchant_category",
        "merchant_country",
        "channel",
        "currency_code",
        "transaction_at",
        "transaction_status",
        "amount",
    },
    "fact_consumer_dispute": {
        "customer_id",
        "account_id",
        "card_id",
        "dispute_reference",
        "transaction_reference",
        "account_reference",
        "card_reference",
        "reason_description",
        "currency_code",
        "raised_at",
        "dispute_status",
        "amount",
    },
}

METRIC_VIEWS = {
    "mv_consumer_accounts": {
        "source": "dim_consumer_account",
        "fields": {
            "account_reference",
            "product_type",
            "account_status",
            "currency_code",
            "open_date",
            "quality_status",
            "data_as_of",
        },
        "measures": {"account_count", "active_account_count"},
        "parameters": ["scope_customer_id", "scope_account_id"],
    },
    "mv_consumer_cards": {
        "source": "dim_consumer_card",
        "fields": {
            "account_reference",
            "card_reference",
            "card_last_four",
            "card_type",
            "expiry_month",
            "card_status",
            "quality_status",
            "data_as_of",
        },
        "measures": {"card_count", "active_card_count"},
        "parameters": [
            "scope_customer_id",
            "scope_account_id",
            "scope_card_id",
        ],
    },
    "mv_consumer_transactions": {
        "source": "fact_consumer_transaction",
        "fields": {
            "transaction_reference",
            "account_reference",
            "card_reference",
            "merchant_name",
            "merchant_category",
            "merchant_country",
            "channel",
            "currency_code",
            "transaction_at",
            "transaction_status",
            "amount",
            "quality_status",
            "data_as_of",
        },
        "measures": {
            "transaction_count",
            "total_transaction_amount",
            "average_transaction_amount",
        },
        "parameters": [
            "scope_customer_id",
            "scope_account_id",
            "scope_card_id",
        ],
    },
    "mv_consumer_disputes": {
        "source": "fact_consumer_dispute",
        "fields": {
            "dispute_reference",
            "transaction_reference",
            "account_reference",
            "card_reference",
            "reason_description",
            "currency_code",
            "raised_at",
            "dispute_status",
            "amount",
            "quality_status",
            "data_as_of",
        },
        "measures": {"dispute_count", "total_disputed_amount"},
        "parameters": [
            "scope_customer_id",
            "scope_account_id",
            "scope_card_id",
        ],
    },
}

TECHNICAL_FIELD_NAMES = {
    "customer_id",
    "account_id",
    "card_id",
    "pipeline_run_id",
    "batch_id",
    "source_references",
    "usage_restrictions",
}


def _load_yaml(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_consumer_broker_models_are_protected_gold_contracts():
    assert set(BROKER_MODELS) <= GOLD_MODELS

    for model, expected_columns in BROKER_MODELS.items():
        contract = _load_yaml(CONTRACT_DIR / f"{model}.yml")
        columns = {column["name"] for column in contract["columns"]}

        assert contract["model"] == model
        assert contract["table"] == f"gold.{model}"
        assert contract["ai_access"] == {
            "classification": "internal_only",
            "pii_safe": True,
        }
        assert {"protected_broker_source", "consumer"} <= set(contract["tags"])
        assert expected_columns <= columns
        assert contract["primary_key"]


def test_only_declared_consumer_brokers_may_retain_ownership_keys():
    assert_no_forbidden_columns(
        {"customer_id", "account_id"}, "dim_consumer_account"
    )
    with pytest.raises(ValueError):
        assert_no_forbidden_columns({"customer_id"}, "dim_case")
    with pytest.raises(ValueError):
        assert_no_forbidden_columns({"customer_id", "pan"}, "dim_consumer_account")


def test_consumer_metric_views_require_customer_scope_and_only_narrow_it():
    for view_name, expected in METRIC_VIEWS.items():
        definition = _load_yaml(METRIC_DIR / f"{view_name}.yaml")
        parameters = definition["parameters"]
        filter_expression = definition["filter"]

        assert definition["version"] == 1.1
        assert definition["source"] == f"g3_catalog.gold.{expected['source']}"
        assert "customer_facing" in definition["comment"]
        assert "materialization" not in definition
        assert [parameter["name"] for parameter in parameters] == expected["parameters"]
        assert parameters[0] == {
            "name": "scope_customer_id",
            "data_type": "string",
        }
        assert "source.customer_id = scope_customer_id" in filter_expression

        for parameter in parameters[1:]:
            assert parameter["default"] == "'__all_owned__'"
            key = parameter["name"].removeprefix("scope_")
            assert f"source.{key} = {parameter['name']}" in filter_expression


def test_consumer_metric_views_expose_only_the_approved_contract():
    for view_name, expected in METRIC_VIEWS.items():
        definition = _load_yaml(METRIC_DIR / f"{view_name}.yaml")
        fields = {field["name"] for field in definition["fields"]}
        measures = {measure["name"] for measure in definition["measures"]}

        assert fields == expected["fields"]
        assert measures == expected["measures"]
        assert fields.isdisjoint(TECHNICAL_FIELD_NAMES)
        assert not any(
            f"source.{technical_name}" in field["expr"]
            for field in definition["fields"]
            for technical_name in TECHNICAL_FIELD_NAMES
        )

        if any("amount" in measure for measure in measures):
            assert "currency_code" in fields


def test_consumer_deployment_is_separate_and_service_principal_scoped():
    runner = (METRIC_DIR / "create_consumer_metric_views.py").read_text(
        encoding="utf-8"
    )

    assert 'TARGET_SCHEMA = "consumer_metrics"' in runner
    assert "consumer_service_principal" in runner
    assert "GRANT SELECT ON VIEW" in runner
    assert "GRANT SELECT ON TABLE" in runner
    for view_name in METRIC_VIEWS:
        assert view_name in runner


def test_consumer_routes_define_bounded_customer_safe_queries():
    routes = _load_yaml(METRIC_DIR / "routing_examples.yaml")

    assert routes["defaults"]["recent_transactions_days"] == 30
    assert routes["defaults"]["detail_limit"] == 100
    assert routes["defaults"]["detail_order"] == "newest_first"
    assert routes["defaults"]["empty_result"] == "no matching records"
    assert routes["defaults"]["mixed_currency_aggregation"] == "forbidden"
    assert TECHNICAL_FIELD_NAMES <= set(routes["response_policy"]["strip_fields"])
    assert {
        "balances",
        "available_credit",
        "card_limits",
        "fraud_scores",
        "investigation_cases",
        "financial_advice",
        "predictions",
    } <= set(routes["response_policy"]["unsupported"])

    operation_names = {operation["name"] for operation in routes["operations"]}
    assert operation_names == {
        "list_my_accounts",
        "list_my_cards",
        "get_my_transactions",
        "get_my_disputes",
    }
    for operation in routes["operations"]:
        assert "scope_customer_id" in operation["backend_injected_parameters"]
        assert not (
            {"customer_id", "account_id", "card_id"}
            & set(operation["agent_arguments"])
        )
