"""CLI entry point: generate mock Transaction-Investigation Bronze source data.

Examples
--------
# default assignment baseline (transactions=2,000,000) -> data/raw/
python -m mock.generate

# small fast run for a quick demo / test
python -m mock.generate --transactions 2000 --customers 150 --out data/raw

# scale everything down for a smaller local sample
python -m mock.generate --scale 0.25

# explicit stress test (~2M transactions) — streaming, memory-bounded
python -m mock.generate --stress --out data/raw

# only a subset of tables
python -m mock.generate --tables customers,accounts,transactions

# heavier defect density
python -m mock.generate --defect-rate 0.15

Outputs one CSV per table under --out, plus defects_manifest.csv listing every
intentionally-injected bad record (use it to validate the Silver quarantine).
"""
import argparse
import csv
import os
import sys
import time

from . import config as C
from .helpers import make_faker, make_rng
from .defects import DefectManifest
from .generators import Ctx, GENERATORS


def build_counts(args):
    """Derive the full per-table volume map from the two knobs + scale."""
    s = max(0.0, args.scale)
    counts = {k: int(v * s) if v else 0 for k, v in C.BASE_VOLUMES.items()}
    if args.transactions is not None:
        counts["transactions"] = args.transactions
    if args.customers is not None:
        counts["customers"] = args.customers

    txn = counts["transactions"]
    cust = counts["customers"]

    # parents derived from customers
    counts["accounts"] = max(10, int(cust * 1.5))
    counts["cards"] = max(10, int(counts["accounts"] * 1.2))

    # children derived from transactions
    counts["auth_attempts"] = int(txn * 1.2)
    counts["transaction_devices"] = int(txn * 0.8)
    counts["disputes"] = max(1, int(txn * 0.02))
    counts["chargebacks"] = max(0, int(counts["disputes"] * 0.2))
    counts["fraud_alerts"] = max(1, int(txn * 0.005))
    cases = max(5, int(txn * 0.001))
    counts["investigation_cases"] = cases
    counts["investigation_notes"] = cases * 5
    counts["case_transactions"] = cases * 3
    counts["case_parties"] = cases * 2
    counts["customer_contact_logs"] = max(1, cases)
    return counts


def write_csv(path, schema, rows):
    """Stream any iterable of dict rows to a CSV. Returns number of data rows."""
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=schema, extrasaction="ignore")
        w.writeheader()
        count = 0
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in schema})
            count += 1
        return count


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="python -m mock.generate",
        description="Generate mock Transaction-Investigation Bronze source data with intentional DQ defects.",
    )
    ap.add_argument("--seed", type=int, default=42, help="random seed (default 42)")
    ap.add_argument("--out", default="data/raw", help="output directory (default data/raw)")
    ap.add_argument("--customers", type=int, default=None, help="override customer count")
    ap.add_argument("--transactions", type=int, default=None, help="override transaction count")
    ap.add_argument("--scale", type=float, default=1.0, help="multiplier on base volumes (default 1.0)")
    ap.add_argument("--defect-rate", type=float, default=0.05,
                    help="fraction of eligible rows that get a defect (default 0.05)")
    ap.add_argument("--stress", action="store_true",
                    help="shortcut: transactions=2,000,000 for explicit stress testing")
    ap.add_argument("--tables", default=None,
                    help="comma-separated subset of tables to generate (default: all)")
    ap.add_argument("--no-manifest", action="store_true", help="skip writing defects_manifest.csv")
    ap.add_argument("--quiet", action="store_true", help="suppress per-table console output")
    args = ap.parse_args(argv)

    if args.defect_rate < 0 or args.defect_rate > 1:
        ap.error("--defect-rate must be between 0 and 1")
    if args.stress and args.transactions is None:
        args.transactions = 2_000_000

    counts = build_counts(args)
    selected = {t.strip() for t in args.tables.split(",")} if args.tables else None

    ctx = Ctx(
        f=make_faker(args.seed),
        rng=make_rng(args.seed + 1),
        manifest=DefectManifest(),
        defect_rate=args.defect_rate,
        counts=counts,
    )

    os.makedirs(args.out, exist_ok=True)
    summary = []
    grand_total = 0
    t_start = time.time()

    for table in C.GENERATION_ORDER:
        if selected and table not in selected:
            continue
        gen = GENERATORS.get(table)
        if gen is None:
            continue
        n = counts.get(table)
        t0 = time.time()
        # reference tables ignore n; pass n for everything else
        rows = gen(ctx) if n is None else gen(ctx, n)
        path = os.path.join(args.out, f"{table}.csv")
        written = write_csv(path, C.TABLE_SCHEMAS[table], rows)
        dt = time.time() - t0
        defects = ctx.manifest.count_for(table)
        summary.append((table, written, defects, dt))
        grand_total += written
        if not args.quiet:
            sys.stderr.write(f"  {table:<24} {written:>10,} rows   defects={defects:<5} ({dt:.1f}s)\n")

    if not args.no_manifest:
        manifest_path = os.path.join(args.out, "defects_manifest.csv")
        ctx.manifest.write(manifest_path)

    # ---- summary ----
    total_defects = len(ctx.manifest)
    sys.stderr.write("\n")
    sys.stderr.write(f"Generated {grand_total:,} rows across {len(summary)} tables "
                     f"in {time.time() - t_start:.1f}s\n")
    sys.stderr.write(f"Output dir : {args.out}\n")
    sys.stderr.write(f"Seed       : {args.seed}   Defect rate: {args.defect_rate}\n")
    sys.stderr.write(f"Total injected defects: {total_defects:,}")
    if not args.no_manifest:
        sys.stderr.write(f"  -> {_defects_filename(args.out)}")
    sys.stderr.write("\n")
    return 0


def _defects_filename(out):
    return os.path.join(out, "defects_manifest.csv")


if __name__ == "__main__":
    raise SystemExit(main())
