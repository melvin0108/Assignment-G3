# Bronze Layer — Mock Dataset & Databricks Ingestion (Concrete Picture)

> **Platform:** Databricks (Free Edition). **Scope of this doc:** the **Bronze** layer only — source mock files landed as Delta, raw + unmodified, with metadata and schema validation. Silver is documented separately.
> **Topic:** Banking **Transaction Investigation** (transactions, disputes/chargebacks, merchants, fraud alerts, investigation notes).
> **ETL mode:** Batch (per the briefing).

---

## 1. Bronze layer contract (what Bronze guarantees)

Bronze is the **immutable landing zone**. It makes **one promise** and nothing more:

| Rule | Meaning |
|---|---|
| **Raw & complete** | Source data is written **exactly as received**. No cleansing, no filtering, no type coercion beyond what Delta needs. Bad rows are **kept**, never dropped. |
| **Append-only** | Bronze tables are never overwritten or updated in place. Reruns add new files/batches, never mutate history (full auditability). |
| **Schema-validated, drift-tolerant** | Expected schema is enforced; **unexpected/malformed columns are captured**, not rejected, via a rescue column. |
| **Metadata-enriched** | Every row gets traceability columns: `_source_file`, `_source_file_mod_time`, `_ingest_ts`, `_run_id`, `_batch_id`, `_rescued_data`, plus `_source_record_id` and `_record_hash` for dedup / replay / quarantine joins. |
| **Idempotent** | Re-running ingestion does not duplicate rows already loaded (COPY INTO / Auto Loader track consumed files). |

> Quarantining, masking, dedup, and type-fixing are **Silver's** job. Bronze only *captures* everything so Silver has the raw truth to clean.

---

## 2. The mock dataset (topic: Transaction Investigation)

**Core spine — 10 source tables** (covers every defect type the brief requires). All are landed 1:1 into Bronze.

| Source file → Bronze table | Purpose | PII | Key injected defects |
|---|---|---|---|
| `customers.csv` → `bronze.customers` | Customer master | 🔴 | missing email; near-duplicate person (same name+dob+addr, diff id) |
| `accounts.csv` → `bronze.accounts` | Bank accounts | — | orphan `customer_id` (RI break); future `open_date` |
| `cards.csv` → `bronze.cards` | Payment cards | 🔴 | raw synthetic PAN; expired-but-active; duplicate card |
| `merchants.csv` → `bronze.merchants` | Merchant master | — | closed merchant still referenced; inconsistent risk casing |
| `merchant_categories.csv` → `bronze.merchant_categories` | MCC reference | — | enum/RI lookup |
| `transactions.csv` → `bronze.transactions` | **The big fact (stress target)** | — | dup `txn_id`; negative amount; missing `merchant_id`; orphan account+card; future timestamp |
| `disputes.csv` → `bronze.disputes` | Customer disputes | — | orphan `transaction_id`; missing reason; status outside enum |
| `investigation_cases.csv` → `bronze.investigation_cases` | Fraud/SAR cases | — | stale "open" case (>180d); status outside enum |
| `investigation_notes.csv` → `bronze.investigation_notes` | Free-text notes | 🔴 | PII + PAN leaked in text; `legal_hold` note (must-not-expose) |
| `employees.csv` → `bronze.employees` | Investigators | 🔴 | duplicate emails |

**Reference tables (extend toward 20–30):** `channels`, `case_status_types`, `dispute_reason_codes`, `fraud_types`, `countries`, `currencies`, `branches`. **Richness/bridge:** `transaction_devices`, `auth_attempts`, `chargebacks`, `fraud_alerts`, `case_transactions`, `case_parties`, `customer_contact_logs`. → ~22 source tables in Bronze.

> Full per-field **data contracts**, status enums + transitions, PII/masking matrix, quarantine schema, and the Gold AI-ready grain live in **`docs/data-model.md`** (§4–§8). Bronze source files must match those contracts exactly (CTA 10).

---

## 3. Concrete sample data (seeded; good + bad rows)

IDs are internally consistent so foreign keys line up *except* where an RI break is intentional.

### `customers.csv` (PII → masked in Silver)
```csv
customer_id,first_name,last_name,dob,email,phone,address,tax_id,created_at
CUST-1001,Jane,Smith,1989-03-12,jane.smith@example.com,+61412345678,12 King St Melbourne,111222333,2023-01-15
CUST-1002,Bob,Jones,1978-11-02,,+61422222222,5 Queen Rd Sydney,222333444,2023-02-20
CUST-1003,Alice,Wong,1992-07-30,alice.wong@example.com,+61433330000,88 George St Brisbane,333444555,2023-03-10
CUST-1001,Jane,Smith,1989-03-12,jane.smith@example.com,+61412345678,12 King St Melbourne,111222333,2023-01-15
CUST-1004,Jane,Smith,1989-03-12,jane.s2@example.com,+61499990000,12 King St Melbourne,111222333,2023-04-01
```
Injected defects: row 2 missing `email`; row 4 exact duplicate of row 1 (same `customer_id`); row 5 near-duplicate (same name+dob+address, **and same `tax_id`**, different id) → fuzzy-dedup target. `tax_id` values are synthetic mock (no real TFN). Matches data-model §4.1.

### `accounts.csv`
```csv
account_id,customer_id,product_type,open_date,status,currency
ACC-2001,CUST-1001,Everyday,2023-01-16,active,AUD
ACC-2002,CUST-1002,Savings,2023-02-21,active,AUD
ACC-2003,CUST-1003,Credit,2023-03-11,active,AUD
ACC-2004,CUST-1001,Everyday,2024-06-01,active,AUD
ACC-2005,CUST-9999,Savings,2031-05-10,active,AUD
```
Defects: `ACC-2005` → orphan `customer_id` (CUST-9999 doesn't exist); `ACC-2005.open_date` in the future.

### `cards.csv` (PII)
```csv
card_id,account_id,card_type,pan,expiry,status
CARD-3001,ACC-2001,debit,4532-1111-2222-1234,2027-08,active
CARD-3002,ACC-2002,debit,4532-1111-2222-3333,2024-02,active
CARD-3003,ACC-2003,credit,4532-3333-4444-5678,2028-11,active
CARD-3004,ACC-2004,debit,4532-5555-6666-9012,2025-03,closed
```
Defects: `CARD-3002` has `expiry` 2024-02 in the past but `status=active`; `CARD-3004` is closed. `pan` is intentionally raw synthetic source data and must be masked/tokenized by the Silver pipeline.

### `merchants.csv`
```csv
merchant_id,name,mcc,country,risk_rating,status
MCH-4001,Coffee Corner,5499,AU,low,active
MCH-4002,TechGalaxy Online,5732,AU,HIGH,active
MCH-4003,CityStay Hotel,7011,AU,Medium,active
MCH-4099,Old Electronics,5732,AU,low,closed
```
Defects: inconsistent `risk_rating` casing (`low` / `HIGH` / `Medium`); `MCH-4099` closed but still referenced by transactions.

### `merchant_categories.csv` (reference, clean)
```csv
mcc,category_name,category_group
5499,Cafes & Restaurants,Food
5732,Electronics,Retail
7011,Hotels & Lodging,Travel
5812,Eating Places,Food
```

### `transactions.csv` (stress table — ramp to millions)
```csv
transaction_id,account_id,card_id,merchant_id,channel,amount,currency,txn_ts,status
TXN-500001,ACC-2001,CARD-3001,MCH-4001,pos,129.50,AUD,2026-07-05T10:14:00Z,settled
TXN-500002,ACC-2002,CARD-3002,MCH-4099,online,-50.00,AUD,2026-07-05T10:20:00Z,settled
TXN-500003,ACC-2001,CARD-3001,,atm,200.00,AUD,2026-07-05T11:00:00Z,settled
TXN-500004,ACC-2099,CARD-3099,MCH-4002,mobile,15.00,AUD,2026-07-05T11:30:00Z,settled
TXN-500001,ACC-2001,CARD-3001,MCH-4001,pos,129.50,AUD,2026-07-05T10:14:00Z,settled
TXN-500006,ACC-2003,CARD-3003,MCH-4003,pos,75.25,USD,2031-01-01T00:00:00Z,settled
TXN-500007,ACC-2004,CARD-3004,MCH-4002,online,49.99,AUD,2026-07-05T12:10:00Z,declined
```
Defects: row 2 negative `amount`; row 3 missing `merchant_id`; row 4 orphan `account_id`+`card_id` (ACC-2099/CARD-3099 don't exist); row 5 exact duplicate of row 1 (`transaction_id`); row 6 future `txn_ts`; row 7 uses a `closed` card (`CARD-3004`) — business-rule break.

### `disputes.csv`
```csv
dispute_id,transaction_id,reason_code,amount,status,raised_at
DSP-6001,TXN-500001,10.4,129.50,open,2026-07-06T08:00:00Z
DSP-6002,TXN-5099,10.4,50.00,Open,2026-07-06T08:30:00Z
DSP-6003,TXN-500006,,75.25,pending,2026-07-06T09:00:00Z
```
Defects: `DSP-6002` orphan `transaction_id` (TXN-5099) and inconsistent status casing (`Open` vs `open`); `DSP-6003` missing `reason_code`.

### `investigation_cases.csv`
```csv
case_id,priority,status_code,fraud_type_code,owner_employee_id,opened_at,closed_at,legal_hold
CASE-7001,high,open,card_fraud,EMP-9001,2026-07-05T09:00:00Z,,false
CASE-7002,medium,closed,none,EMP-9002,2024-01-10T09:00:00Z,2024-02-01T10:00:00Z,false
CASE-7003,high,open,account_takeover,EMP-9001,2025-12-01T09:00:00Z,,false
CASE-7999,critical,on_hold,sar,EMP-9001,2026-07-01T09:00:00Z,,true
```
Defects: `CASE-7003` stale open case (>180 days); `CASE-7999` `legal_hold=true` (must never reach AI); inconsistent `status_code` (`on_hold` vs enum `suspended`).

### `investigation_notes.csv` (PII in free text)
```csv
note_id,case_id,author_employee_id,note_text,created_at
NOTE-1,CASE-7001,EMP-9001,"Spoke to Jane Smith (jane.smith@example.com, +61412345678). Card 4532########1234 compromised.",2026-07-05T09:10:00Z
NOTE-2,CASE-7002,EMP-9002,"Reviewed 3 txns, all match merchant profile. No fraud.",2026-07-05T09:30:00Z
NOTE-3,CASE-7999,EMP-9001,"SAR filed - legal hold. Do not disclose.",2026-07-05T10:00:00Z
```
Defects: `NOTE-1` leaks PII (email, phone) **and** PAN inside `note_text`; `NOTE-3` belongs to a `legal_hold` case (access-gated).

### `employees.csv` (PII)
```csv
employee_id,full_name,email,team,role
EMP-9001,Sarah Chen,sarah.chen@nab-mock.dev,Fraud Ops,investigator
EMP-9002,David Lee,david.lee@nab-mock.dev,Fraud Ops,investigator
EMP-9003,Sarah Chen,sarah.chen@nab-mock.dev,QA,supervisor
```
Defects: `EMP-9003` duplicate email across two people; near-duplicate name.

### `customer_contact_logs.csv` (PII in free text + DNC logic)
```csv
contact_id,customer_id,direction,contact_method,do_not_contact,contacted_at,employee_id,note
CTL-8001,CUST-1001,outbound,phone,false,2026-07-05T13:00:00Z,EMP-9001,"Left voicemail re: disputed TXN-500001."
CTL-8002,CUST-1003,outbound,email,true,2026-07-05T13:30:00Z,EMP-9002,"Emailed alice.wong@example.com to confirm chargeback."
CTL-8003,CUST-1002,inbound,phone,false,2026-07-05T14:00:00Z,EMP-9001,"Caller confirmed name Bob Jones and card 4532########3333."
```
Defects: `CTL-8002` contacts a `do_not_contact=true` customer (DNC business-rule break) and the `note` leaks an email; `CTL-8003` `note` leaks a name + PAN. `contact_method` is its own enum (not the txn `channels`). Matches data-model §4.11.

### Reference tables (small, clean-ish)
```csv
# channels.csv
channel_code,channel_name
pos,Point of Sale
online,E-Commerce
mobile,Mobile App
atm,ATM

# case_status_types.csv
status_code,description
open,Case open
in_progress,Under investigation
suspended,Temporarily halted
closed,Case closed

# dispute_reason_codes.csv
reason_code,description
10.4,Fraud - Card Absent
13.1,Merchandise Not Received
13.7,Cancelled Merchandise

# fraud_types.csv
fraud_type_code,description,severity
card_fraud,Card compromise,high
account_takeover,Account takeover,high
sar,Suspicious Activity Report,critical
none,Not fraud,low
```

---

## 4. Bronze Delta table anatomy (raw + metadata)

Bronze stores the source columns **unchanged**, plus traceability columns. Example — `bronze.transactions`:

| transaction_id | amount | status | `_source_file` | `_source_file_mod_time` | `_ingest_ts` | `_run_id` | `_batch_id` | `_source_record_id` | `_record_hash` | `_rescued_data` |
|---|---|---|---|---|---|---|---|---|---|---|
| TXN-500001 | 129.50 | settled | transactions.csv | 2026-07-06T08:00 | 2026-07-06T08:05 | RUN-20260706-1 | 1 | TXN-500001 | 9a2f…c1 | null |
| TXN-500002 | -50.00 | settled | transactions.csv | … | … | RUN-20260706-1 | 1 | TXN-500002 | 7b1e…0a | null |
| … | … | … | … | … | … | … | … | … | … | `{extra_col:"x"}` |

- All 7 source rows are present (Bronze keeps the dirty ones).
- Auto Loader adds newly observed header columns to Bronze as `STRING`; `_rescued_data` is reserved for incomplete or malformed CSV records.
- All columns are stored as **strings** in Bronze (typing is deferred to Silver) — this is intentional: it preserves the raw shape and lets Silver own type coercion + the resulting quarantine decisions.

---

## 5. Databricks Bronze ingestion (concrete)

### Unity Catalog layout
```
Catalog : tx_inv                          (transaction investigation)
 ├─ Schema: bronze                        (this doc)
 │    ├─ customers, accounts, cards, merchants, merchant_categories,
 │    │  transactions, disputes, investigation_cases, investigation_notes,
 │    │  employees, channels, case_status_types, dispute_reason_codes, fraud_types, …
 └─ Volume : /Volumes/tx_inv/landing/…    (raw files land here per table)
      └─ customers/, accounts/, transactions/, …
```
Dev/prod separation via catalogs: `tx_inv_dev`, `tx_inv_stg`, `tx_inv`.

### Bronze table DDL (string-typed, append-only, with metadata + rescue)
```sql
CREATE TABLE tx_inv.bronze.transactions (
  transaction_id      STRING,
  account_id          STRING,
  card_id             STRING,
  merchant_id         STRING,
  channel             STRING,
  amount              STRING,
  currency            STRING,
  txn_ts              STRING,
  status              STRING,
  _source_file        STRING,
  _source_file_mod_time TIMESTAMP,
  _ingest_ts          TIMESTAMP,
  _run_id             STRING,
  _batch_id           BIGINT,
  _source_record_id   STRING,   -- stable per source row (source PK) for dedup & quarantine joins
  _record_hash        STRING,   -- sha256 of the raw source row, for change/replay detection
  _rescued_data       STRING
) USING DELTA;
```

### Option A — Batch ingest with `COPY INTO` (recommended for batch mandate)
`COPY INTO` is **idempotent** — it tracks loaded files in Delta transaction logs, so re-runs only consume new files and never duplicate.
```sql
SET VAR run_id = 'RUN-20260706-1';

COPY INTO tx_inv.bronze.transactions
FROM (
  SELECT
    transaction_id, account_id, card_id, merchant_id, channel, amount, currency, txn_ts, status,
    _metadata.file_name            AS _source_file,
    _metadata.file_mod_time        AS _source_file_mod_time,
    current_timestamp()            AS _ingest_ts,
    ${run_id}                      AS _run_id,
    1                              AS _batch_id,
    transaction_id                 AS _source_record_id,
    sha2(concat_ws('|', transaction_id, account_id, card_id, merchant_id, channel, amount, currency, txn_ts, status), 256) AS _record_hash
  FROM '/Volumes/tx_inv/landing/transactions'
  FILEFORMAT => CSV
  FORMAT_OPTIONS ('header'='true', 'escapeQuotes'='true')
)
FILEFORMAT = CSV
COPY_OPTIONS ('mergeSchema' = 'true');
```

### Option B — Delta Live Tables (managed; gives lineage + retries for free)
```sql
CREATE OR REFRESH STREAMING TABLE tx_inv.bronze.transactions
COMMENT 'Raw transactions landed as-is from source'
AS
SELECT
  transaction_id, account_id, card_id, merchant_id, channel, amount, currency, txn_ts, status,
  _metadata.file_name       AS _source_file,
  _metadata.file_mod_time   AS _source_file_mod_time,
  current_timestamp()       AS _ingest_ts,
  'RUN-20260706-1'          AS _run_id,
  transaction_id            AS _source_record_id,
  sha2(concat_ws('|', transaction_id, account_id, card_id, merchant_id, channel, amount, currency, txn_ts, status), 256) AS _record_hash
FROM cloud_files(
  '/Volumes/tx_inv/landing/transactions',
  'csv',
  map('header', 'true', 'rescuedDataColumn', '_rescued_data'));
```

### Option C — Auto Loader (near-real-time / micro-batch)
```python
(spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "csv")
     .option("cloudFiles.schemaLocation", "/Volumes/tx_inv/_schema/transactions")
     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
     .option("cloudFiles.inferColumnTypes", "false")
     .option("rescuedDataColumn", "_rescued_data")
     .option("header", "true")
     .load("/Volumes/tx_inv/landing/transactions")
     .writeStream.format("delta")
     .option("checkpointLocation", "/Volumes/tx_inv/_checkpoints/bronze_transactions")
     .option("mergeSchema", "true")
     .trigger(availableNow=True)                 # batch-style: process new files then stop
     .toTable("tx_inv.bronze.transactions"))
```
`trigger(availableNow=True)` makes Auto Loader behave as a **batch** job (processes only newly-arrived files, then stops) — aligns with the batch mandate while keeping streaming-style idempotency.

---

## 6. Bronze pipeline picture

```
                                  Bronze layer (Databricks)
 ┌──────────────────┐   COPY INTO /    ┌─────────────────────────────────────┐
 │ Source mock files│   Auto Loader /  │  Delta tables (raw + metadata)      │
 │ (CSV/JSON)       │ ──── DLT ──────▶ │  append-only, schema-validated,     │
 │ /Volumes/.../    │   idempotent     │  _rescued_data for drift            │
 │   landing/       │                  │  all columns STRING                 │
 └──────────────────┘                  └─────────────────────────────────────┘
        tx_inv.bronze.customers · accounts · cards · merchants ·
        merchant_categories · transactions · disputes ·
        investigation_cases · investigation_notes · employees ·
        channels · case_status_types · dispute_reason_codes · fraud_types …
                                                │
                                                ▼  (next doc: Silver)
                                   dedupe · type-cast · conform enums ·
                                   RI repair · route failures to quarantine ·
                                   mask PII
```

---

## 7. Bronze conventions

- **Namespace:** `tx_inv.bronze.<table>` (table name = source file stem, singular).
- **All bronze columns are `STRING`** — type coercion is Silver's responsibility, with failures → quarantine.
- **Mandatory metadata columns on every bronze table:** `_source_file`, `_source_file_mod_time`, `_ingest_ts`, `_run_id`, `_batch_id`, `_source_record_id`, `_record_hash`, plus `_rescued_data` for malformed CSV preservation.
- **Append-only:** never `UPDATE`/`DELETE`/`OVERWRITE` a bronze table.
- **Idempotent reruns:** use `COPY INTO` or checkpointed Auto Loader so re-running the pipeline from a clean state reproduces the same bronze content.
- **Deterministic mock data:** the generator is seeded, so re-generating source files reproduces the same defects (required for reproducible tests).

---

## 8. What Bronze explicitly does NOT do (that's Silver's job)

❌ Drop/clean dirty rows · ❌ Deduplicate · ❌ Type-cast amounts/dates · ❌ Conform enum casing · ❌ Repair RI · ❌ Mask PII · ❌ Build the AI-ready context.

Bronze keeps the dirty truth; Silver turns it into trustworthy data.
