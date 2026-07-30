# Databricks metric views

This directory contains one Databricks metric-view YAML definition per coherent
business domain and fact grain. Compatible question routes share a definition
so their common measures have one implementation.

The original `docs/models/gold/questions-to-metrics.yaml` remains the canonical
use-case and routing definition and is not modified.

| Definition | Deployed metric view | Question routes |
| --- | --- | --- |
| `01_case_metrics.yaml` | `mv_case_metrics` | `case_overview`, `case_closure_trends` |
| `02_transaction_metrics.yaml` | `mv_transaction_metrics` | `transaction_activity`, `merchant_risk_exposure` |
| `03_authorization_metrics.yaml` | `mv_authorization_metrics` | `authorization_outcomes` |
| `04_dispute_metrics.yaml` | `mv_dispute_metrics` | `dispute_activity` |
| `05_chargeback_metrics.yaml` | `mv_chargeback_metrics` | `chargeback_activity` |
| `06_fraud_alert_metrics.yaml` | `mv_fraud_alert_metrics` | `fraud_alert_activity` |
| `07_safe_note_metrics.yaml` | `mv_safe_note_metrics` | `safe_notes` |
| `08_party_metrics.yaml` | `mv_party_metrics` | `party_composition` |
| `09_date_fields.yaml` | `mv_dim_date` | Calendar reference fields |
| `10_merchant_fields.yaml` | `mv_dim_merchant` | Merchant reference fields |
| `11_channel_fields.yaml` | `mv_dim_channel` | Channel reference fields |
| `12_currency_fields.yaml` | `mv_dim_currency` | Currency reference fields |
| `13_dispute_reason_fields.yaml` | `mv_dim_dispute_reason` | Dispute-reason reference fields |

`case_detail_lookup` is intentionally not converted. It is a detail route with
no metric IDs and uses `gold.investigation_context` as a direct Genie source;
its nested arrays and structs cannot be faithfully represented as metric-view
fields.

## Deploy from a Databricks Git folder

1. Push this directory to Git and pull the repository into Databricks.
2. Run the Gold pipeline and Gold validation.
3. Open `14_create_metric_views.py` as a Databricks notebook.
<<<<<<< HEAD
4. Run the first cell to create the `catalog` widget, then select the target catalog.
5. Run the second cell to create or replace the metric views.
=======
4. Select the target catalog with the `catalog` widget.
5. Run all cells.
>>>>>>> db8336989774587b6d7c075b7ab24d6683fd0014

The runner creates `<catalog>.metrics` if needed, reads all 13 YAML files,
and executes `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML`. It
replaces only the 13 metric views listed above and does not replace Gold
tables.

The compute must support metric views and YAML specification 1.1. The executing
principal needs `SELECT` on the Gold sources, `USE CATALOG`, permission to
create the `metrics` schema when it does not exist, `USE SCHEMA` and
`CREATE TABLE` on that schema, and `CAN USE` on the compute.

## Verify

```sql
SELECT
  currency_code,
  MEASURE(transaction_count) AS transaction_count,
  MEASURE(transaction_amount_total) AS total_amount
FROM g3_catalog.metrics.mv_transaction_metrics
GROUP BY ALL
ORDER BY currency_code;
```

Monetary measures must always be grouped or filtered by `currency_code`.
