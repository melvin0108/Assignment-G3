# Metric Views and Genie Agent

## Purpose

This feature adds a governed semantic layer for natural-language analytics on
the transaction-investigation Gold models. Unity Catalog metric views define
approved business measures, dimensions, relationships, descriptions, and
English/Vietnamese synonyms. A Databricks Genie Agent uses these definitions to
translate business questions into consistent SQL without requiring users to
understand the physical Gold schema.

## From YAML contracts to metric views

The implementation keeps
[`questions-to-metrics.yaml`](../docs/models/gold/questions-to-metrics.yaml) as
the canonical question-routing contract. It maps supported analytics and
case-detail intents to metric IDs, dimensions, filters, and approved source
models. The routing contract defines question coverage; the files under
`metrics_view/` are the executable Databricks semantic definitions.

The analytics routes are consolidated by business domain and fact grain into
eight Databricks metric-view definitions under
[`metrics_view/`](../metrics_view/README.md):

| Metric view | Supported analytics |
|---|---|
| `mv_case_metrics` | Case overview and closure trends |
| `mv_transaction_metrics` | Transaction activity and merchant-risk exposure |
| `mv_authorization_metrics` | Authorization attempts, approvals, and declines |
| `mv_dispute_metrics` | Dispute counts and amounts |
| `mv_chargeback_metrics` | Chargeback counts and amounts |
| `mv_fraud_alert_metrics` | Fraud-alert counts and average scores |
| `mv_safe_note_metrics` | PII-screened note counts |
| `mv_party_metrics` | Safe party-composition counts |

Each YAML definition declares a Gold source, safe dimensional joins, exposed
fields, business measures, comments, and bilingual synonyms. Shared measures
are defined once per grain, reducing inconsistent calculations across question
routes. Monetary measures explicitly require grouping or filtering by
`currency_code`.

## Automated Databricks deployment

[`09_create_metric_views.py`](../metrics_view/09_create_metric_views.py) is the
Databricks deployment notebook. After the repository is pulled into a
Databricks Git folder, the notebook:

1. reads the eight version-controlled YAML definitions;
2. validates their metric-view specification version and Gold source;
3. applies the catalog selected through the standard `catalog` widget;
4. creates the `<catalog>.metrics` schema when required; and
5. executes `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML` for each
   governed metric view.

This approach keeps semantic definitions reviewable in Git while making
deployment repeatable across the supported development, test, and assignment
catalogs. The runner changes only the named metric views and does not replace
the underlying Gold tables. After deployment, each metric result is reconciled
with the equivalent direct Gold aggregation before it is exposed to the agent.

## Genie Agent setup

After deployment and metric reconciliation, a Genie Agent is created with the
eight objects from `<catalog>.metrics` and a Serverless or Pro SQL warehouse.
The agent inherits the measures, field descriptions, synonyms, and
relationships stored in the metric views.

Unity Catalog grants restrict the agent and its users to the approved metric
views and source access. The natural-language instructions below supplement
those enforceable permissions; they are not a replacement for access control.

Agent instructions reinforce the project rules:

- never combine monetary amounts across currencies;
- use closed-date fields for closure analysis;
- treat `UNKNOWN` as unresolved enrichment;
- ask for clarification when the requested domain, period, currency, or metric
  is ambiguous;
- do not expose restricted customer, employee, account, card, PAN, contact,
  device/IP, address, tax-ID, or legal-hold information; and
- report recorded investigation activity without inferring guilt or confirmed
  fraud.

The separate `gold.investigation_context` model remains the approved source for
case-detail lookup. It is not part of the metrics-only agent unless it is
separately approved and configured; the metric views serve aggregate analytics.

## Example questions

The following questions demonstrate the supported Genie Agent experience:

| English | Vietnamese |
|---|---|
| How many cases are there by status? | Có bao nhiêu hồ sơ theo trạng thái? |
| Show closed cases by priority. | Hiển thị hồ sơ đã đóng theo mức ưu tiên. |
| What are the transaction count and total amount by currency? | Số lượng và tổng số tiền giao dịch theo loại tiền là bao nhiêu? |
| Show transaction exposure by merchant risk rating and currency. | Hiển thị giá trị giao dịch theo mức rủi ro người bán và loại tiền. |
| What is the authorization approval rate? | Tỷ lệ phê duyệt cấp phép là bao nhiêu? |
| Show disputes by reason and status. | Hiển thị tranh chấp theo lý do và trạng thái. |
| What is the total chargeback amount by stage and currency? | Tổng số tiền chargeback theo giai đoạn và loại tiền là bao nhiêu? |
| Show the average fraud-alert score by rule. | Hiển thị điểm cảnh báo gian lận trung bình theo quy tắc. |
| Count safe investigation notes by case. | Đếm ghi chú điều tra an toàn theo hồ sơ. |
| Show party counts by party type and role. | Hiển thị số bên theo loại bên và vai trò. |

Generated SQL and results should be reviewed against direct Gold aggregations,
with bilingual benchmark questions used to detect regressions as metric
definitions or agent instructions evolve.

## Outcome

The resulting feature provides a concise, governed path from documented
business questions to reusable metrics and natural-language analytics. YAML
contracts remain the source-controlled semantic specification, the Python
notebook provides repeatable Unity Catalog deployment, and the Genie Agent
provides the investigator-facing conversational interface.
