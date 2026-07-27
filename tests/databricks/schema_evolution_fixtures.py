"""Controlled CSV batches for the Databricks schema-evolution integration test."""

from __future__ import annotations

from dataclasses import dataclass


TEST_TABLE = "schema_evolution_test"
CONTRACT_COLUMNS = ("id", "amount", "currency")
RECORD_ID_COLUMNS = ("id",)


@dataclass(frozen=True)
class CsvFixture:
    """One numbered source file and the schema scenario it introduces."""

    scenario: str
    batch_id: int
    csv_text: str

    @property
    def file_name(self) -> str:
        return f"{TEST_TABLE}{self.batch_id:02d}.csv"


FIXTURES = (
    CsvFixture(
        scenario="baseline",
        batch_id=1,
        csv_text=(
            "id,amount,currency\n"
            "SE-001,100.50,USD\n"
        ),
    ),
    CsvFixture(
        scenario="added_column",
        batch_id=2,
        csv_text=(
            "id,amount,currency,risk_score\n"
            "SE-002,200.00,EUR,0.85\n"
        ),
    ),
    CsvFixture(
        scenario="missing_column",
        batch_id=3,
        csv_text=(
            "id,amount,risk_score\n"
            "SE-003,300.00,0.70\n"
        ),
    ),
    CsvFixture(
        scenario="reordered_columns",
        batch_id=4,
        csv_text=(
            "risk_score,currency,id,amount\n"
            "0.65,THB,SE-004,400.00\n"
        ),
    ),
    CsvFixture(
        scenario="malformed_csv",
        batch_id=5,
        csv_text=(
            "id,amount,currency,risk_score\n"
            "SE-005,500.00,USD,0.50,UNEXPECTED_EXTRA_FIELD\n"
        ),
    ),
    CsvFixture(
        scenario="invalid_typed_value",
        batch_id=6,
        csv_text=(
            "id,amount,currency,risk_score\n"
            "SE-006,not-a-number,USD,0.90\n"
        ),
    ),
)
