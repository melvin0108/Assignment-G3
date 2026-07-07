# Assignment-G3

NAB "TAC@NABVNSC22" Core Data Engineer assignment — Transaction Investigation
Context batch pipeline. Turns mock banking data into governed, AI-ready context.

This README covers **mock data generation**. For generator internals see
`mock/README.md`; for the source-table contract (columns, row counts, derivation
rules) see `docs/data-dictionary.md`.

## Prerequisites

```bash
pip install -r requirements.txt   # only dependency: Faker
```

## Generating mock data

The generator is seeded (default seed `42`), so the same command reproduces
identical data and defects. Output lands in `data/raw/` — one CSV per table plus
`defects_manifest.csv` listing every intentionally-injected bad record.

> Run all commands from `D:\NAB\Assignment-G3` (the `mock` package lives here).

### Default — 15% defect rate

Generates the full assignment baseline (2,000,000 transactions, 5,000 customers,
all 25 source tables) with ~15% of eligible rows carrying an intentional
data-quality defect:

```bash
python -m mock.generate --defect-rate 0.15
```

### Fast dev sample — 15% defect rate

Same 25 tables and derivation ratios, small enough to inspect quickly:

```bash
python -m mock.generate --transactions 2000 --customers 150 --defect-rate 0.15
```

### Other useful variants

```bash
python -m mock.generate --defect-rate 0.15 --scale 0.25                 # 25% of every volume
python -m mock.generate --defect-rate 0.15 --tables transactions,customers
python -m mock.generate --defect-rate 0.15 --seed 42 --out data/raw
```

### Flags

| flag | default | meaning |
|---|---|---|
| `--defect-rate` | `0.05` | fraction of eligible rows that get a defect (0–1) |
| `--transactions` | `2000000` | override transaction count (drives facts) |
| `--customers` | `5000` | override customer count (drives accounts/cards) |
| `--scale` | `1.0` | multiplier on base volumes |
| `--seed` | `42` | RNG seed (same seed → identical data + defects) |
| `--out` | `data/raw` | output directory |
| `--tables` | all | comma-separated subset to generate |
| `--stress` | off | shortcut for `--transactions 2000000` |
| `--no-manifest` | off | skip `defects_manifest.csv` |
| `--quiet` | off | suppress per-table console output |

### Output

- `<table>.csv` for each of the 25 source tables (column order matches
  `docs/data-dictionary.md`).
- `defects_manifest.csv` — every injected bad record with
  `source_table, record_key, rule_id, rule_name, failure_reason, severity`. Use
  it to validate the Silver `quarantine_records` output (expected vs actual).

---

helloooooo
123
thinh 456
nhatvu
eloise
