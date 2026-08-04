# Databricks notebook source
# ============================================================================
# GOVERNANCE PIPELINE: apply_column_tags
# ============================================================================
# Creates governed tag keys from governance/column_classification.yml and applies
# the manifest's column tags to Unity Catalog tables.
#
# Intended usage:
#   1. Run after Bronze/Silver/Gold tables have been published.
#   2. Run as a principal with:
#      - CREATE on governed tags, if create_governed_tags=true and tags are missing
#      - MANAGE on governed tags, if sync_allowed_values=true
#      - ASSIGN on each governed tag key
#      - APPLY TAG on target Unity Catalog tables/columns
#      - SELECT or metadata visibility on target tables for validation
#
# Notes:
# - Governed tags are account-level resources. Tag keys in the manifest are
#   intentionally generic and catalog-independent.
# - Table names in the manifest are catalog-relative: <schema>.<table>.
# - This script prepends the runtime catalog widget value.
# ============================================================================

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG_CHOICES = ["g3_dev", "g3_test", "g3_catalog"]
DEFAULT_MANIFEST_PATHS = [
    # Repo-local path when executed from a synced workspace/repo context.
    "governance/column_classification.yml",
    "Assignment-G3/governance/column_classification.yml",
    # Workspace absolute path used by the existing job notebooks.
    "/Workspace/Users/thinhpham1807@gmail.com/Assignment-G3/governance/column_classification.yml",
]

TAG_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,255}$")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _get_widget(name: str, default: str, choices: list[str] | None = None) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        if choices:
            dbutils.widgets.dropdown(name, default, choices)
        else:
            dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)


CATALOG = _get_widget("catalog", "g3_dev", CATALOG_CHOICES)
MANIFEST_PATH = _get_widget("manifest_path", "")
CREATE_GOVERNED_TAGS = _get_widget("create_governed_tags", "true", ["true", "false"]).lower() == "true"
SYNC_ALLOWED_VALUES = _get_widget("sync_allowed_values", "false", ["true", "false"]).lower() == "true"
DRY_RUN = _get_widget("dry_run", "false", ["true", "false"]).lower() == "true"
MISSING_TABLE_BEHAVIOR = _get_widget("missing_table_behavior", "skip", ["skip", "fail"])
MISSING_COLUMN_BEHAVIOR = _get_widget("missing_column_behavior", "fail", ["skip", "fail"])

if CATALOG not in CATALOG_CHOICES:
    raise ValueError(f"Unsupported catalog: {CATALOG}. Allowed catalogs: {CATALOG_CHOICES}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_yaml(path: str) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "PyYAML is required to read governance/column_classification.yml. "
            "Install PyYAML on the Databricks cluster or convert the manifest loader."
        ) from exc

    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Manifest must be a YAML mapping: {path}")
    return data


def _resolve_manifest_path() -> str:
    candidates = [MANIFEST_PATH] if MANIFEST_PATH else DEFAULT_MANIFEST_PATHS
    for candidate in candidates:
        if not candidate:
            continue
        try:
            if candidate.startswith("/Workspace/"):
                # Workspace files are visible to Python open() on Databricks.
                with open(candidate, "r", encoding="utf-8"):
                    return candidate
            if Path(candidate).exists():
                return str(Path(candidate))
        except Exception:
            continue
    raise FileNotFoundError(
        "Could not locate column classification manifest. Set the manifest_path widget "
        "to the full path of governance/column_classification.yml."
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _validate_tag_key(tag_key: str) -> None:
    if not TAG_KEY_PATTERN.match(tag_key):
        raise ValueError(
            f"Invalid tag key '{tag_key}'. Governed tag keys in this project must use "
            "letters, numbers, and underscores, and must start with a letter or underscore."
        )


def _validate_catalog_relative_table(table_name: str) -> tuple[str, str]:
    parts = table_name.split(".")
    if len(parts) != 2:
        raise ValueError(f"Manifest table must be catalog-relative '<schema>.<table>': {table_name}")
    schema, table = parts
    for part in parts:
        if not IDENTIFIER_PATTERN.match(part):
            raise ValueError(f"Invalid schema/table identifier in manifest table '{table_name}'")
    return schema, table


def _full_table_name(table_name: str) -> str:
    schema, table = _validate_catalog_relative_table(table_name)
    return ".".join([_quote_identifier(CATALOG), _quote_identifier(schema), _quote_identifier(table)])


def _plain_full_table_name(table_name: str) -> str:
    schema, table = _validate_catalog_relative_table(table_name)
    return f"{CATALOG}.{schema}.{table}"


def _execute(sql: str) -> None:
    compact_sql = " ".join(line.strip() for line in sql.strip().splitlines() if line.strip())
    if DRY_RUN:
        print(f"[DRY RUN] {compact_sql}")
        return
    print(f"[SQL] {compact_sql}")
    spark.sql(sql)


def _table_exists(full_table: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(full_table))
    except Exception:
        try:
            spark.table(full_table).limit(0).collect()
            return True
        except Exception:
            return False


def _columns_for_table(full_table: str) -> set[str]:
    return set(spark.table(full_table).columns)


def _governed_tag_exists(tag_key: str) -> bool:
    rows = spark.sql(f"SHOW GOVERNED TAGS LIKE {_sql_string(tag_key)}").collect()
    return any(row.asDict().get("Tag Key") == tag_key for row in rows)


def _taxonomy_values(taxonomy_entry: Any) -> list[str]:
    if not isinstance(taxonomy_entry, dict):
        raise ValueError(f"Invalid tag taxonomy entry: {taxonomy_entry!r}")
    values = taxonomy_entry.get("values")
    if isinstance(values, dict):
        return list(values.keys())
    if isinstance(values, list):
        return [str(value) for value in values]
    raise ValueError(f"Tag taxonomy entry must define values as a mapping or list: {taxonomy_entry!r}")


def _create_or_sync_governed_tags(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    taxonomy = manifest.get("tag_taxonomy")
    if not isinstance(taxonomy, dict):
        raise ValueError("Manifest must define tag_taxonomy.")

    results: list[dict[str, Any]] = []
    for tag_key, definition in taxonomy.items():
        _validate_tag_key(tag_key)
        values = _taxonomy_values(definition)
        description = str(definition.get("description", f"G3 governed tag: {tag_key}"))
        values_sql = ", ".join(_sql_string(value) for value in values)

        exists = _governed_tag_exists(tag_key)
        if not exists:
            if CREATE_GOVERNED_TAGS:
                _execute(
                    f"""
                    CREATE GOVERNED TAG {tag_key}
                    DESCRIPTION {_sql_string(description)}
                    VALUES ({values_sql})
                    """
                )
                action = "created"
            else:
                raise ValueError(
                    f"Governed tag '{tag_key}' does not exist and create_governed_tags=false."
                )
        elif SYNC_ALLOWED_VALUES:
            _execute(f"ALTER GOVERNED TAG {tag_key} SET VALUES ({values_sql})")
            _execute(f"ALTER GOVERNED TAG {tag_key} SET DESCRIPTION {_sql_string(description)}")
            action = "synced"
        else:
            action = "exists"

        results.append({"tag_key": tag_key, "values": values, "action": action})
    return results


def _validate_manifest_columns(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    columns = manifest.get("columns")
    taxonomy = manifest.get("tag_taxonomy")
    if not isinstance(columns, list):
        raise ValueError("Manifest must define columns as a list.")
    if not isinstance(taxonomy, dict):
        raise ValueError("Manifest must define tag_taxonomy as a mapping.")

    allowed_by_key = {tag_key: set(_taxonomy_values(definition)) for tag_key, definition in taxonomy.items()}
    normalized: list[dict[str, Any]] = []

    for index, item in enumerate(columns, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"columns[{index}] must be a mapping.")
        table_name = item.get("table")
        column_name = item.get("column")
        tags = item.get("tags")
        if not isinstance(table_name, str) or not table_name:
            raise ValueError(f"columns[{index}] must define table.")
        if not isinstance(column_name, str) or not column_name:
            raise ValueError(f"columns[{index}] must define column.")
        if not isinstance(tags, dict) or not tags:
            raise ValueError(f"columns[{index}] must define tags.")

        _validate_catalog_relative_table(table_name)
        for tag_key, tag_value in tags.items():
            if tag_key not in allowed_by_key:
                raise ValueError(f"{table_name}.{column_name} uses undefined tag key '{tag_key}'.")
            if str(tag_value) not in allowed_by_key[tag_key]:
                raise ValueError(
                    f"{table_name}.{column_name} uses invalid value '{tag_value}' for tag '{tag_key}'. "
                    f"Allowed values: {sorted(allowed_by_key[tag_key])}"
                )

        normalized.append(
            {
                "table": table_name,
                "column": column_name,
                "tags": {str(key): str(value) for key, value in tags.items()},
            }
        )

    return normalized


def _apply_column_tags(columns: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "applied": 0,
        "skipped_missing_tables": 0,
        "skipped_missing_columns": 0,
    }
    columns_by_table: dict[str, list[dict[str, Any]]] = {}
    for item in columns:
        columns_by_table.setdefault(item["table"], []).append(item)

    for table_name, table_columns in sorted(columns_by_table.items()):
        plain_full_table = _plain_full_table_name(table_name)
        sql_full_table = _full_table_name(table_name)

        if not _table_exists(plain_full_table):
            message = f"Manifest table does not exist: {plain_full_table}"
            if MISSING_TABLE_BEHAVIOR == "fail":
                raise ValueError(message)
            print(f"[SKIP] {message}")
            counts["skipped_missing_tables"] += len(table_columns)
            continue

        actual_columns = _columns_for_table(plain_full_table)
        for item in table_columns:
            column_name = item["column"]
            if column_name not in actual_columns:
                message = f"Manifest column does not exist: {plain_full_table}.{column_name}"
                if MISSING_COLUMN_BEHAVIOR == "fail":
                    raise ValueError(message)
                print(f"[SKIP] {message}")
                counts["skipped_missing_columns"] += 1
                continue

            tags_sql = ", ".join(
                f"{_sql_string(tag_key)} = {_sql_string(tag_value)}"
                for tag_key, tag_value in sorted(item["tags"].items())
            )
            _execute(
                f"""
                ALTER TABLE {sql_full_table}
                ALTER COLUMN {_quote_identifier(column_name)}
                SET TAGS ({tags_sql})
                """
            )
            counts["applied"] += 1

    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

manifest_path = _resolve_manifest_path()
manifest = _load_yaml(manifest_path)
columns = _validate_manifest_columns(manifest)

print(
    json.dumps(
        {
            "event": "apply_column_tags_start",
            "catalog": CATALOG,
            "manifest_path": manifest_path,
            "column_entries": len(columns),
            "create_governed_tags": CREATE_GOVERNED_TAGS,
            "sync_allowed_values": SYNC_ALLOWED_VALUES,
            "dry_run": DRY_RUN,
            "missing_table_behavior": MISSING_TABLE_BEHAVIOR,
            "missing_column_behavior": MISSING_COLUMN_BEHAVIOR,
        },
        sort_keys=True,
    )
)

tag_results = _create_or_sync_governed_tags(manifest)
apply_counts = _apply_column_tags(columns)

print(
    json.dumps(
        {
            "event": "apply_column_tags_complete",
            "catalog": CATALOG,
            "governed_tags": tag_results,
            **apply_counts,
        },
        sort_keys=True,
    )
)
