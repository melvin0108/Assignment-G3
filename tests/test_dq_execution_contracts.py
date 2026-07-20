"""Contracts for the optimized DQ notebook execution plan."""

import ast
import re
import unittest
from pathlib import Path


DQ_PATH = Path(__file__).parents[1] / "pipeline" / "dq" / "dq_03_failures_all_rules.py"


def load_sql_helpers():
    tree = ast.parse(DQ_PATH.read_text(encoding="utf-8"))
    selected_nodes = []
    helper_names = {"_has_code", "_statements", "_head", "_insert_select"}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in helper_names:
            selected_nodes.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "SQL"
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
        statements = namespace["_statements"](namespace["SQL"])
        heads = [namespace["_head"](statement).split()[0].upper() for statement in statements]
        inserts = [
            statement
            for statement, head in zip(statements, heads)
            if head == "INSERT"
        ]
        combined_query = "\nUNION ALL\n".join(
            namespace["_insert_select"](statement) for statement in inserts
        )

        self.assertEqual(1, heads.count("DELETE"))
        self.assertEqual(35, len(inserts))
        self.assertEqual(1, heads.count("SELECT"))
        self.assertEqual(34, combined_query.count("\nUNION ALL\n"))
        self.assertNotIn("INSERT INTO", combined_query)
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


if __name__ == "__main__":
    unittest.main()
