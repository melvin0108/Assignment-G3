"""Static contracts for table-local Bronze Auto Loader configuration."""

import ast
import unittest
from pathlib import Path

import yaml

from mock.config import TABLE_SCHEMAS


ROOT = Path(__file__).parents[1]
BRONZE_DIR = ROOT / "pipeline" / "bronze"
COMMON_LOADER = BRONZE_DIR / "autoloader_common.py"
REGISTRY = BRONZE_DIR / "table_registry.py"
SPECIAL_CONFIGS = {
    "defects_manifest": (
        [
            "source_table",
            "record_key",
            "rule_id",
            "rule_name",
            "failure_reason",
            "severity",
        ],
        ["source_table", "record_key", "rule_id"],
    ),
}


def _literal_assignments(path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assignments = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                try:
                    assignments[target.id] = ast.literal_eval(node.value)
                except (ValueError, TypeError):
                    pass
    return tree, assignments


def _source_contract(table_name):
    path = ROOT / "docs" / "contracts" / "sources" / f"{table_name}.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


class BronzeConfigContractTests(unittest.TestCase):
    def test_each_table_notebook_owns_its_complete_config(self):
        config_paths = sorted(
            path
            for path in BRONZE_DIR.glob("bronze_*.py")
            if path.name != "bronze_all_tables.py"
        )
        expected_tables = set(TABLE_SCHEMAS) | set(SPECIAL_CONFIGS)
        self.assertEqual(
            {path.stem.removeprefix("bronze_") for path in config_paths},
            expected_tables,
        )

        for path in config_paths:
            with self.subTest(notebook=path.name):
                tree, config = _literal_assignments(path)
                table_name = path.stem.removeprefix("bronze_")
                self.assertEqual(config.get("TABLE_NAME"), table_name)

                if table_name in SPECIAL_CONFIGS:
                    expected_source_columns, expected_record_ids = SPECIAL_CONFIGS[
                        table_name
                    ]
                else:
                    expected_source_columns = TABLE_SCHEMAS[table_name]
                    expected_record_ids = _source_contract(table_name)["primary_key"]

                self.assertEqual(
                    config.get("SOURCE_COLUMNS"), expected_source_columns
                )
                self.assertEqual(
                    config.get("RECORD_ID_COLUMNS"), expected_record_ids
                )
                calls = [
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "ingest_table"
                ]
                self.assertEqual(len(calls), 1)
                expected_guard = ast.parse(
                    '__name__ == "__main__"', mode="eval"
                ).body
                main_guards = [
                    node
                    for node in tree.body
                    if isinstance(node, ast.If)
                    and ast.dump(node.test) == ast.dump(expected_guard)
                ]
                self.assertEqual(len(main_guards), 1)
                self.assertIn(calls[0], list(ast.walk(main_guards[0])))
                self.assertEqual(
                    [getattr(argument, "id", None) for argument in calls[0].args],
                    ["TABLE_NAME", "SOURCE_COLUMNS", "RECORD_ID_COLUMNS"],
                )

    def test_registry_contains_each_table_module_once(self):
        tree = ast.parse(REGISTRY.read_text(encoding="utf-8"), filename=str(REGISTRY))
        registry_modules = None
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            if not any(
                isinstance(target, ast.Name) and target.id == "_TABLE_MODULES"
                for target in node.targets
            ):
                continue
            registry_modules = [element.id for element in node.value.elts]
            break

        expected_modules = sorted(
            path.stem
            for path in BRONZE_DIR.glob("bronze_*.py")
            if path.name != "bronze_all_tables.py"
        )
        self.assertIsNotNone(registry_modules)
        self.assertEqual(len(registry_modules), len(set(registry_modules)))
        self.assertEqual(sorted(registry_modules), expected_modules)

    def test_shared_loader_contains_no_table_registry(self):
        tree = ast.parse(
            COMMON_LOADER.read_text(encoding="utf-8"), filename=str(COMMON_LOADER)
        )
        assigned_names = {
            target.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        self.assertNotIn("TABLE_CONFIG", assigned_names)


if __name__ == "__main__":
    unittest.main()
