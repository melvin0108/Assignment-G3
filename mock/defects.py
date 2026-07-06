"""Defect manifest.

Every intentionally-injected bad record is logged here so the pipeline's
quarantine output can be validated against it (expected vs actual failures).
Schema aligns with data-model.md §7 quarantine fields.
"""
import csv


class DefectManifest:
    def __init__(self):
        self.rows = []

    def add(self, source_table, record_key, rule_id, rule_name, failure_reason, severity="quarantine"):
        self.rows.append({
            "source_table": source_table,
            "record_key": record_key,
            "rule_id": rule_id,
            "rule_name": rule_name,
            "failure_reason": failure_reason,
            "severity": severity,
        })

    def __len__(self):
        return len(self.rows)

    def count_for(self, table):
        return sum(1 for r in self.rows if r["source_table"] == table)

    def write(self, path):
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["source_table", "record_key", "rule_id",
                                               "rule_name", "failure_reason", "severity"])
            w.writeheader()
            w.writerows(self.rows)
