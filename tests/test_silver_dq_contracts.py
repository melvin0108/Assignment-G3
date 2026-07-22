"""Local contracts that keep Silver rejection logic aligned with DQ."""

import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


def source(path):
    return (ROOT / path).read_text(encoding="utf-8")


def task_block(job_source, task_key):
    marker = f"\n        - task_key: {task_key}\n"
    start = job_source.index(marker) + 1
    next_task = job_source.find("\n        - task_key: ", start + len(marker))
    return job_source[start:] if next_task == -1 else job_source[start:next_task]


class SilverDQContractTests(unittest.TestCase):
    def test_dq_quarantine_is_an_authoritative_silver_filter(self):
        snapshot = source("pipeline/silver/snapshot.py")
        self.assertIn("def exclude_dq_quarantined_rows", snapshot)
        self.assertIn('F.col("run_id") == f"{run_id}-DQ"', snapshot)
        self.assertIn('how="left_anti"', snapshot)

        dq_filtered_tables = [
            "silver_customers.py", "silver_employees.py",
            "silver_accounts.py", "silver_cards.py", "silver_merchants.py",
            "silver_transactions.py", "silver_auth_attempts.py",
            "silver_transaction_devices.py", "silver_disputes.py",
            "silver_chargebacks.py", "silver_fraud_alerts.py",
            "silver_investigation_cases.py", "silver_investigation_notes.py",
            "silver_case_transactions.py", "silver_case_parties.py",
            "silver_customer_contact_logs.py",
        ]
        for filename in dq_filtered_tables:
            with self.subTest(filename=filename):
                silver_source = source(f"pipeline/silver/{filename}")
                self.assertGreaterEqual(
                    silver_source.count("exclude_dq_quarantined_rows"),
                    2,
                )

    def test_duplicate_rankings_match_authoritative_dq_order(self):
        dq = source("pipeline/dq/dq_03_failures_all_rules.py")
        customers = source("pipeline/silver/silver_customers.py")
        employees = source("pipeline/silver/silver_employees.py")
        cards = source("pipeline/silver/silver_cards.py")
        transactions = source("pipeline/silver/silver_transactions.py")

        effective_order = (
            "F.expr(\"try_to_timestamp(replace(replace(effective_at, 'T', ' '), 'Z', ''))\")"
            ".desc_nulls_last()"
        )
        self.assertIn(effective_order, customers)
        self.assertIn(effective_order, cards)
        self.assertEqual(2, dq.count("ORDER BY try_to_timestamp(replace(replace(effective_at,'T',' '),'Z','')) DESC NULLS LAST"))
        self.assertIn("PARTITION BY transaction_id ORDER BY _source_record_id", dq)
        self.assertIn("PARTITION BY email ORDER BY _source_record_id", dq)
        self.assertIn("PARTITION BY full_name ORDER BY _source_record_id", dq)
        self.assertIn(
            'near_dup_window = Window.partitionBy("first_name", "last_name", "dob", "address", "tax_id")',
            customers,
        )
        self.assertIn('.orderBy(F.col("_source_record_id").asc())', customers)
        self.assertEqual(2, employees.count('orderBy(F.col("_source_record_id").asc())'))
        self.assertIn(
            'txn_window = Window.partitionBy("transaction_id").orderBy(F.col("_source_record_id").asc())',
            transactions,
        )

    def test_m2_validation_rejects_physical_row_overlap(self):
        validation = source("pipeline/validation/validate_m2_dq.py")
        self.assertIn("QUARANTINED_SOURCE_TABLES", validation)
        self.assertIn("ON q.source_record_id = s._source_record_id", validation)
        self.assertIn("WHERE q.run_id IN ('{DQ_RUN_ID}', '{SILVER_RUN_ID}')", validation)

    def test_m2_validation_rejects_stale_or_accumulated_silver_rows(self):
        validation = source("pipeline/validation/validate_m2_dq.py")
        self.assertIn("SILVER_DATA_TABLES", validation)
        self.assertIn("_batch_id <> {SNAPSHOT_BATCH_ID}", validation)
        self.assertIn("_run_id <> '{SILVER_RUN_ID}'", validation)

    def test_m2_validation_runs_after_silver_and_gates_gold(self):
        jobs = source("pipeline/jobs.yaml")
        self.assertIn(
            "- task_key: silver_date_dim",
            task_block(jobs, "validate_m2_dq"),
        )
        self.assertIn(
            "- task_key: validate_m2_dq",
            task_block(jobs, "dim_date"),
        )



if __name__ == "__main__":
    unittest.main()
