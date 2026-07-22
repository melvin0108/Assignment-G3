"""Contracts for the optimized DQ notebook execution plan."""

import ast
import re
import unittest
from pathlib import Path


DQ_PATH = Path(__file__).parents[1] / "pipeline" / "dq" / "dq_03_failures_all_rules.py"


def load_sql_helpers():
    tree = ast.parse(DQ_PATH.read_text(encoding="utf-8"))
    selected_nodes = []
    helper_names = {
        "_has_code",
        "_statements",
        "_head",
        "_statement_keyword",
        "_insert_select",
        "_use_current_source_views",
    }
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in helper_names:
            selected_nodes.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in {"SOURCE_TABLES", "SQL"}
            for target in node.targets
        ):
            selected_nodes.append(node)

    namespace = {}
    helper_module = ast.Module(body=selected_nodes, type_ignores=[])
    exec(compile(helper_module, str(DQ_PATH), "exec"), namespace)
    return namespace


class DQExecutionContractTests(unittest.TestCase):
    def test_all_rule_queries_combine_without_changing_inventory(self):
        namespace = load_sql_helpers()
        transformed_sql = namespace["_use_current_source_views"](namespace["SQL"])
        statements = namespace["_statements"](transformed_sql)
        keywords = [namespace["_statement_keyword"](stmt) for stmt in statements]
        inserts = [
            statement
            for statement, keyword in zip(statements, keywords)
            if keyword == "INSERT"
        ]
        combined_query = "\nUNION ALL\n".join(
            namespace["_insert_select"](statement) for statement in inserts
        )

        self.assertEqual(1, keywords.count("DELETE"))
        self.assertEqual(35, len(inserts))
        self.assertEqual(1, keywords.count("SELECT"))
        self.assertEqual(34, combined_query.count("\nUNION ALL\n"))
        self.assertNotIn("INSERT INTO", combined_query)
        self.assertNotIn("__CATALOG__.bronze.", transformed_sql)
        self.assertEqual(
            35,
            len(set(re.findall(r"'(DQ-[A-Z0-9-]+)'", combined_query))),
        )

    def test_current_snapshots_use_serverless_safe_temp_views(self):
        source = DQ_PATH.read_text(encoding="utf-8")
        self.assertIn('.groupBy("_source_table")', source)
        self.assertIn("createOrReplaceTempView", source)
        self.assertNotIn("StorageLevel", source)
        self.assertNotIn(".persist(", source)
        self.assertNotIn(".cache(", source)
        self.assertNotIn(".unpersist(", source)

    def test_quarantine_uses_one_current_run_append_without_history_rewrite(self):
        source = DQ_PATH.read_text(encoding="utf-8")
        self.assertIn("_combined_rule_query", source)
        self.assertIn("_run(_combined_rule_query).toDF(*QUARANTINE_COLUMNS)", source)
        self.assertIn('.mode("append")', source)
        self.assertNotIn("def _deduplicate_quarantine_records", source)
        self.assertNotIn("spark.read.table(QUARANTINE_TABLE_NAME)", source)

    def test_statement_keyword_handles_multiline_select(self):
        namespace = load_sql_helpers()
        self.assertEqual("SELECT", namespace["_statement_keyword"]("SELECT\n  rule_id"))

    def test_runtime_uses_the_contract_tested_keyword_classifier(self):
        source = DQ_PATH.read_text(encoding="utf-8")
        execution_source = source[source.index("SNAPSHOT_BATCH_ID, SNAPSHOT_RUN_ID"):]
        self.assertIn('_statement_keyword(stmt) == "SELECT"', execution_source)
        self.assertNotIn('.startswith("SELECT ")', execution_source)


if __name__ == "__main__":
    unittest.main()
