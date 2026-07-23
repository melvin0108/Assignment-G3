"""Contracts for mock CSV sources must match the generator and DQ registry."""

import re
import unittest
from pathlib import Path

import yaml

from mock.config import TABLE_SCHEMAS


ROOT = Path(__file__).parents[1]
CONTRACT_DIR = ROOT / "docs" / "contracts" / "sources"
DQ_REGISTRY = ROOT / "pipeline" / "dq" / "dq_02_load_dq_rules.py"


class SourceContractTests(unittest.TestCase):
    def test_every_generated_source_has_a_complete_contract(self):
        registry_rule_ids = set(re.findall(r'"(DQ-[A-Z0-9-]+)"', DQ_REGISTRY.read_text(encoding="utf-8")))

        contract_paths = sorted(CONTRACT_DIR.glob("*.yml"))
        self.assertEqual(
            {path.stem for path in contract_paths},
            set(TABLE_SCHEMAS),
            "Each mock-generated source must have one contract YAML file",
        )

        for path in contract_paths:
            with self.subTest(contract=path.name):
                contract = yaml.safe_load(path.read_text(encoding="utf-8"))
                self.assertEqual(contract["dataset"], path.stem)
                self.assertEqual(contract["source"], f"data/raw/{path.stem}.csv")
                self.assertTrue(contract["grain"])
                self.assertTrue(contract["primary_key"])

                fields = contract["fields"]
                self.assertEqual([field["name"] for field in fields], TABLE_SCHEMAS[path.stem])
                for field in fields:
                    self.assertIn("type", field)
                    self.assertIn("required", field)
                    self.assertIn("example", field)
                    self.assertIn("classification", field)
                    self.assertIn("rules", field)
                    self.assertTrue(set(field["rules"]) <= registry_rule_ids)


if __name__ == "__main__":
    unittest.main()
