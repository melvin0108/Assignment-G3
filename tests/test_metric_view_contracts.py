from pathlib import Path

import yaml


METRIC_VIEW_DIR = Path(__file__).parents[1] / "metrics_view"
METRIC_VIEW_RUNNER = METRIC_VIEW_DIR / "14_create_metric_views.py"


def test_metric_view_field_and_measure_names_are_unique():
    """Databricks metric views require one namespace for fields and measures."""
    for definition_path in sorted(METRIC_VIEW_DIR.glob("*.yaml")):
        definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
        field_names = {field["name"] for field in definition.get("fields", [])}
        measure_names = {measure["name"] for measure in definition.get("measures", [])}

        assert field_names.isdisjoint(measure_names), definition_path.name


def test_investigation_context_metric_view_exposes_ai_allowed_case_metrics():
    definition_path = METRIC_VIEW_DIR / "14_investigation_context_metrics.yaml"

    assert definition_path.is_file()

    definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
    field_names = {field["name"] for field in definition["fields"]}
    measure_names = {measure["name"] for measure in definition["measures"]}

    assert definition["source"] == "g3_catalog.gold.investigation_context"
    assert {"case_id", "context_category", "priority", "quality_status"} <= field_names
    assert measure_names == {"investigation_context_count"}


def test_investigation_context_metric_view_is_registered_for_deployment():
    runner = METRIC_VIEW_RUNNER.read_text(encoding="utf-8")

    assert '("14_investigation_context_metrics.yaml", "mv_investigation_context_metrics")' in runner
