"""Helpers for deriving current-state Silver outputs from Bronze snapshots."""

from pyspark.sql import functions as F


def latest_batch_snapshot(df):
    """Return only the latest complete Bronze snapshot in ``df``.

    Bronze is append-only, while the mock publisher writes each batch as a
    complete snapshot. Filtering before data-quality checks preserves genuine
    duplicates inside the current batch while preventing earlier snapshots
    from being mistaken for duplicate current records.
    """
    latest_batch_id = (
        df.agg(F.max("_batch_id").alias("_batch_id"))
        .first()["_batch_id"]
    )
    return df.filter(F.col("_batch_id") == latest_batch_id)
