from pathlib import Path

import yaml


METRIC_VIEW_DIR = Path(__file__).parents[1] / "metrics_view"


def test_metric_view_field_and_measure_names_are_unique():
    """Databricks metric views require one namespace for fields and measures."""
    for definition_path in sorted(METRIC_VIEW_DIR.glob("*.yaml")):
        definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
        field_names = {field["name"] for field in definition.get("fields", [])}
        measure_names = {measure["name"] for measure in definition.get("measures", [])}

        assert field_names.isdisjoint(measure_names), definition_path.name
