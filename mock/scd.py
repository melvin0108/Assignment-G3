"""SCD Type 2 — derive each later snapshot from the previous one by mutating dimension rows.

Approach B (multi-snapshot): every snapshot folder is a *complete* dataset state.
Non-dimension tables are copied verbatim from snapshot_T0; only the SCD2
dimensions (customers / cards / merchants) carry version changes — a selected
subset of keys is re-extracted with one attribute changed and `effective_at`
bumped to the snapshot's as-of date.

These version changes are legitimate history, NOT data-Quality defects: they are
recorded in `scd_changes_manifest.csv` (the oracle for the future Silver SCD2
windowing step) and are deliberately kept disjoint from DefectManifest —
defective dim rows are excluded from mutation so the two oracles never overlap.

Bronze needs no row-level deduplication: Auto Loader appends each unseen
snapshot file in full and records a deterministic `_record_hash`. Silver uses
the effective timestamp and hash to interpret SCD2 versions and repeated
unchanged extracts.
"""
import csv
import os
import random
import shutil

from . import config as C
from .helpers import iso, make_faker

# Natural key column for each SCD2 dimension.
_NATURAL_KEY = {"customers": "customer_id", "cards": "card_id", "merchants": "merchant_id"}


def _fresh_value(dim, faker, rng, old_value):
    """A coherent replacement for the dimension's tracked attribute."""
    if dim == "customers":
        return f"{faker.street_address()}, {faker.city()}"
    if dim == "cards":
        opts = [s for s in C.CARD_STATUS if s != old_value] or C.CARD_STATUS
        return rng.choice(opts)
    if dim == "merchants":
        opts = [r for r in C.RISK_RATING if r != old_value] or C.RISK_RATING
        return rng.choice(opts)
    raise ValueError(f"no mutation defined for dimension {dim!r}")


class ScdManifest:
    """Oracle listing every dimension version change across snapshots."""

    def __init__(self):
        self.rows = []

    def add(self, source_table, natural_key, snapshot, changed_attribute,
            old_value, new_value, prior_effective_at, effective_at):
        self.rows.append({
            "source_table": source_table,
            "natural_key": natural_key,
            "snapshot": snapshot,
            "changed_attribute": changed_attribute,
            "old_value": old_value,
            "new_value": new_value,
            "prior_effective_at": prior_effective_at,
            "effective_at": effective_at,
        })

    def __len__(self):
        return len(self.rows)

    def count_for(self, table):
        return sum(1 for r in self.rows if r["source_table"] == table)

    @classmethod
    def read(cls, path):
        """Load a previously-written oracle so later batches remain cumulative."""
        manifest = cls()
        if os.path.exists(path):
            with open(path, newline="") as fh:
                manifest.rows.extend(csv.DictReader(fh))
        return manifest

    def write(self, path):
        with open(path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=[
                "source_table", "natural_key", "snapshot", "changed_attribute",
                "old_value", "new_value", "prior_effective_at", "effective_at",
            ])
            w.writeheader()
            w.writerows(self.rows)


def _defective_keys(src_dir):
    """{source_table: set(record_key)} for every intentionally-defective row.

    Used to exclude defective dim rows from SCD2 mutation so the DQ-defect
    oracle (defects_manifest.csv) and the SCD2 oracle never describe the same row.
    """
    defective = {}
    path = os.path.join(src_dir, "defects_manifest.csv")
    if not os.path.exists(path):
        return defective
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            defective.setdefault(r["source_table"], set()).add(r["record_key"])
    return defective


def repair_transaction_account_links(snapshot_dir):
    """Make cards the canonical source of transaction account ownership.

    Later Databricks snapshots normally copy facts from the previous batch.
    This repair upgrades those existing facts without regenerating transaction
    timestamps or child-table relationships. Unknown cards are intentionally
    left untouched so the documented orphan pair remains a DQ fixture.
    """
    cards_path = os.path.join(snapshot_dir, "cards.csv")
    transactions_path = os.path.join(snapshot_dir, "transactions.csv")
    defects_path = os.path.join(snapshot_dir, "defects_manifest.csv")

    card_accounts = {}
    with open(cards_path, newline="") as fh:
        for row in csv.DictReader(fh):
            card_id = row["card_id"]
            account_id = row["account_id"]
            previous = card_accounts.setdefault(card_id, account_id)
            if previous != account_id:
                raise ValueError(
                    f"card {card_id!r} maps to multiple accounts: "
                    f"{previous!r}, {account_id!r}"
                )

    corrected = 0
    unresolved_transaction_ids = set()
    temp_transactions_path = transactions_path + ".tmp"
    with open(transactions_path, newline="") as source, open(
        temp_transactions_path, "w", newline=""
    ) as destination:
        reader = csv.DictReader(source)
        writer = csv.DictWriter(destination, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            canonical_account = card_accounts.get(row["card_id"])
            if canonical_account is None:
                unresolved_transaction_ids.add(row["transaction_id"])
            elif row["account_id"] != canonical_account:
                row["account_id"] = canonical_account
                corrected += 1
            writer.writerow(row)
    os.replace(temp_transactions_path, transactions_path)

    removed_stale_defects = 0
    if os.path.exists(defects_path):
        with open(defects_path, newline="") as fh:
            reader = csv.DictReader(fh)
            defect_fieldnames = reader.fieldnames
            defect_rows = []
            for row in reader:
                stale_orphan = (
                    row["source_table"] == "transactions"
                    and row["rule_id"] == "DQ-TXN-ACCT-FK"
                    and row["record_key"] not in unresolved_transaction_ids
                )
                if stale_orphan:
                    removed_stale_defects += 1
                else:
                    defect_rows.append(row)

        temp_defects_path = defects_path + ".tmp"
        with open(temp_defects_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=defect_fieldnames)
            writer.writeheader()
            writer.writerows(defect_rows)
        os.replace(temp_defects_path, defects_path)

    return corrected, removed_stale_defects


def derive_snapshot(src_dir, snap_dir, snapshot_index, as_of_date, rate, seed, manifest):
    """Copy the previous snapshot verbatim into snap_dir, then mutate the SCD2 dims in place.

    Returns the total number of dimension rows mutated across all dimensions.
    Each mutation is recorded on `manifest` (an ScdManifest).
    """
    os.makedirs(snap_dir, exist_ok=True)

    # 1) Copy every source CSV verbatim — non-dim tables are identical across snapshots.
    for fname in os.listdir(src_dir):
        if fname.endswith(".csv"):
            shutil.copy2(os.path.join(src_dir, fname), os.path.join(snap_dir, fname))

    # 2) Mutate the SCD2 dimensions (in place on the copied files).
    faker = make_faker(seed + 8000 + snapshot_index)   # isolated, deterministic address stream
    rng = random.Random(seed + 7000 + snapshot_index)  # isolated, deterministic selection stream
    as_of_iso = iso(as_of_date)
    bad_keys = _defective_keys(src_dir)
    mutated = 0

    for dim in C.SCD2_DIMENSIONS:
        attr = C.SCD2_MUTATIONS[dim]
        key_field = _NATURAL_KEY[dim]
        dim_path = os.path.join(snap_dir, f"{dim}.csv")
        with open(dim_path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        if not rows:
            continue

        # Only mutate clean rows: exclude any key flagged as a DQ defect in T0.
        bad = bad_keys.get(dim, set())
        eligible = [i for i, r in enumerate(rows) if r[key_field] not in bad]
        k = min(max(1, int(len(rows) * rate)), len(eligible))
        if k <= 0:
            continue

        required_keys = C.SCD2_REQUIRED_KEYS.get(dim, set())
        required = [
            i for i in eligible if rows[i][key_field] in required_keys
        ]
        required = required[:k]
        remaining = [i for i in eligible if i not in set(required)]
        selected = required + rng.sample(remaining, k - len(required))

        for idx in sorted(selected):
            row = rows[idx]
            old = row[attr]
            new = _fresh_value(dim, faker, rng, old)
            manifest.add(
                source_table=dim,
                natural_key=row[key_field],
                snapshot=f"T{snapshot_index}",
                changed_attribute=attr,
                old_value=old,
                new_value=new,
                prior_effective_at=row.get("effective_at", ""),
                effective_at=as_of_iso,
            )
            row[attr] = new
            row["effective_at"] = as_of_iso
            mutated += 1

        with open(dim_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=C.TABLE_SCHEMAS[dim], extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({c: ("" if r.get(c) is None else r.get(c)) for c in C.TABLE_SCHEMAS[dim]})

    return mutated
