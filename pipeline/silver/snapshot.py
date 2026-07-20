"""Helpers for deriving current-state Silver outputs from Bronze snapshots."""

from pyspark.sql import functions as F


QUARANTINE_KEY_COLUMNS = [
    "run_id",
    "source_table",
    "source_record_id",
    "rule_id",
]


def latest_batch_snapshot(df):
    """Return only the latest complete Bronze snapshot in ``df``.

    Bronze is append-only, while the mock publisher writes each batch as a
    complete snapshot. Filtering before data-quality checks preserves genuine
    duplicates inside the current batch while preventing earlier snapshots
    from being mistaken for duplicate current records.
    """
    if "_batch_id" not in df.columns:
        raise ValueError("Bronze input is missing required _batch_id metadata")

    latest_batch_id = (
        df.agg(F.max("_batch_id").alias("_batch_id"))
        .first()["_batch_id"]
    )
    if latest_batch_id is None:
        raise ValueError("Bronze input does not contain a complete batch")
    return df.filter(F.col("_batch_id") == latest_batch_id)


def snapshot_run_id(snapshot_df):
    """Return the one Bronze run identifier represented by a snapshot."""
    if "_run_id" not in snapshot_df.columns:
        raise ValueError("Bronze snapshot is missing required _run_id metadata")

    run_ids = [
        row["_run_id"]
        for row in snapshot_df.select("_run_id").distinct().limit(2).collect()
    ]
    if len(run_ids) != 1 or run_ids[0] is None:
        raise ValueError(
            "Latest Bronze snapshot must contain exactly one non-null _run_id"
        )
    return run_ids[0]


def assert_matching_latest_snapshots(spark, catalog, source_tables):
    """Reject a run when Bronze source tables do not share one snapshot identity."""
    identities = {}
    for table_name in source_tables:
        snapshot_df = latest_batch_snapshot(
            spark.read.table(f"{catalog}.bronze.{table_name}")
        )
        batch_id = snapshot_df.select("_batch_id").first()["_batch_id"]
        identities[table_name] = (batch_id, snapshot_run_id(snapshot_df))

    distinct_identities = set(identities.values())
    if len(distinct_identities) != 1:
        details = ", ".join(
            f"{table_name}={batch_id}/{run_id}"
            for table_name, (batch_id, run_id) in sorted(identities.items())
        )
        raise ValueError(
            "Bronze sources do not share one latest complete snapshot: " + details
        )

    return distinct_identities.pop()


def deduplicate_quarantine_rows(df):
    """Keep one quarantine record per run, source record, and failed rule."""
    return df.dropDuplicates(QUARANTINE_KEY_COLUMNS)


def exclude_dq_quarantined_rows(df, spark, catalog, source_table, run_id):
    """Remove physical source rows rejected by the authoritative DQ stage."""
    if "_source_record_id" not in df.columns:
        raise ValueError("Silver output is missing required _source_record_id metadata")

    rejected_source_rows = (
        spark.read.table(f"{catalog}.silver.quarantine_records")
        .filter(
            (F.col("run_id") == f"{run_id}-DQ")
            & (F.col("source_table") == source_table)
        )
        .select(F.col("source_record_id").alias("_source_record_id"))
        .distinct()
    )
    return (
        df.join(rejected_source_rows, on="_source_record_id", how="left_anti")
        .select(*df.columns)
    )
