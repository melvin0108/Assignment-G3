"""Safe notes, party roles, and source-reference inputs for Gold."""

from pyspark.sql import functions as F

from pipeline.gold.common import aggregate, source_rows


def build_supporting_context(inputs, quarantine):
    quarantined_notes = quarantine.where(F.col("source_table") == "investigation_notes").select(
        F.col("record_key").alias("note_id")
    ).distinct()
    notes = inputs["investigation_notes"].join(quarantined_notes, "note_id", "left_anti").select(
        "case_id", "note_id", "note_text", "created_at", "_source_record_id"
    )
    safe_notes = aggregate(notes, F.struct("note_id", "note_text", "created_at"), "safe_notes")

    parties = inputs["case_parties"].select("case_id", "party_type", "role", "_source_record_id")
    party_context = aggregate(parties, F.struct("party_type", "role"), "party_context")
    return {
        "collections": [safe_notes, party_context],
        "sources": [source_rows(notes, "investigation_notes"), source_rows(parties, "case_parties")],
    }
