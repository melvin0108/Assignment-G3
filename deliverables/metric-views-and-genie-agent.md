# Metric Views, Genie, and Consumer AI Agent

## Purpose

This feature provides two separate governed semantic products:

1. an investigator-facing Banker layer in `<catalog>.metrics`, where a
   Databricks Genie Agent uses approved investigation measures and fields; and
2. an authenticated customer self-service layer in
   `<catalog>.consumer_metrics`, where a trusted bank-app backend injects the
   signed-in customer's ownership scope before an AI agent can receive a
   result.

The two products do not share access paths. Consumer users cannot query Banker
investigation data, and neither bank customers nor the Consumer LLM receives
direct Unity Catalog credentials.

## From YAML contracts to metric views

The implementation keeps
[`questions-to-metrics.yaml`](../docs/models/gold/questions-to-metrics.yaml) as
the canonical question-routing contract. It maps supported analytics and
case-detail intents to metric IDs, dimensions, filters, and approved source
models. The routing contract defines question coverage; the files under
`metrics_view/` are the executable Databricks semantic definitions.

The analytics routes are consolidated by business domain and fact grain into
14 Databricks metric-view definitions under
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
| `mv_dim_date` | Calendar reference fields |
| `mv_dim_merchant` | Merchant reference fields |
| `mv_dim_channel` | Channel reference fields |
| `mv_dim_currency` | Currency reference fields |
| `mv_dim_dispute_reason` | Dispute-reason reference fields |
| `mv_investigation_context_metrics` | AI-allowed investigation-context fields and case count |

Each YAML definition declares a Gold source, safe dimensional joins, exposed
fields, business measures, comments, and bilingual synonyms. Shared measures
are defined once per grain, reducing inconsistent calculations across question
routes. Monetary measures explicitly require grouping or filtering by
`currency_code`.

## Automated Databricks deployment

[`14_create_metric_views.py`](../metrics_view/14_create_metric_views.py) is the
Databricks deployment notebook. After the repository is pulled into a
Databricks Git folder, the notebook:

1. reads the 14 version-controlled YAML definitions;
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
14 objects from `<catalog>.metrics` plus `gold.investigation_context` as a
direct Genie source, using a Serverless or Pro SQL warehouse.
The agent inherits the measures, field descriptions, synonyms, and
relationships stored in the metric views.

Unity Catalog grants restrict the agent and its users to the approved metric
views and source access. The natural-language instructions below supplement
those enforceable permissions; they are not a replacement for access control.

### Configured Genie Agent instruction

The following instruction is added to the agent's general instructions:

```text
Use the governed metric views for analytics.

Always group or filter monetary measures by currency_code. Never combine
amounts from different currencies.

For case-closure questions, use closed-date fields and include only closed
cases.

UNKNOWN values represent unresolved enrichment and must not be interpreted as
real business entities.

Do not expose or request customer, employee, account, card, PAN, contact,
device, IP, tax-ID, address, or legal-hold information.

Do not conclude that a person or merchant committed fraud. Report only the
recorded case, transaction, alert, dispute, and chargeback metrics.

When the requested domain, currency, date period, or measure is ambiguous,
ask the user to clarify.
```

`mv_investigation_context_metrics` exposes the AI-allowed context table's safe
scalar case fields and case count. The separate `gold.investigation_context`
model remains the approved direct Genie source for case-detail lookup, where
its nested arrays and structs remain available.

## Authenticated Consumer AI agent

The Consumer AI agent supports personal self-service for the signed-in bank
customer. It does not reuse the investigation Genie Agent or its
`<catalog>.metrics` schema.

The trust boundary is:

```text
Authenticated bank session
  -> backend resolves customer/account/card ownership
  -> agent requests an approved operation
  -> backend injects verified ownership scope
  -> parameterized Consumer metric view enforces that scope
  -> backend removes technical identifiers
  -> agent receives only the permitted result
```

Prompt text is never an identity authority. If a prompt contains a
`customer_id`, `account_id`, or `card_id`, the backend ignores that identifier.
Account and card selections are resolved against the authenticated customer's
ownership map before a metric query is issued.

### Protected Consumer Gold brokers

Four `internal_only` Gold models provide the ownership-filter keys and
customer-safe attributes needed by the backend:

| Gold model | Grain | Purpose |
|---|---|---|
| `gold.dim_consumer_account` | One row per customer/account | Account ownership and masked account reference |
| `gold.dim_consumer_card` | One row per customer/card | Card ownership, masked references, last four, type, expiry, and status |
| `gold.fact_consumer_transaction` | One row per customer/transaction | Customer-owned transaction detail and currency-safe amounts |
| `gold.fact_consumer_dispute` | One row per customer/dispute | Customer-owned dispute detail and currency-safe disputed amounts |

These brokers may retain internal `customer_id`, `account_id`, and `card_id`
only for trusted backend filtering. They are not direct AI retrieval sources.
Raw account/card identifiers, full PAN, contact PII, pipeline metadata, and
investigation-only fields are absent from the customer-facing metric-view
fields.

### Consumer metric views

The executable definitions are version controlled under
[`consumer_metrics_view/`](../consumer_metrics_view/README.md) and deploy into
the separate `<catalog>.consumer_metrics` schema:

| Metric view | Customer-visible data | Measures |
|---|---|---|
| `mv_consumer_accounts` | Masked account reference, product, status, currency, open date, quality, data-as-of | Account count, active account count |
| `mv_consumer_cards` | Masked account/card references, last four, type, expiry month, status, quality, data-as-of | Card count, active card count |
| `mv_consumer_transactions` | Masked references, merchant, channel, currency, timestamp, status, amount, quality, data-as-of | Transaction count, total amount, average amount |
| `mv_consumer_disputes` | Masked dispute/transaction/account/card references, reason, currency, raised time, status, amount, quality, data-as-of | Dispute count, total disputed amount |

Every Consumer view requires a `scope_customer_id` parameter without a
default. Optional `scope_account_id` and `scope_card_id` parameters default to
the SQL string literal `__all_owned__`. Their global filters always anchor on
the required customer scope, so account/card selection can narrow access but
cannot broaden it.

Parameterized metric views use YAML specification 1.1 and require DBR or SQL
compute 18.2 or newer. They are not configured for materialization because
parameterized metric views cannot currently be materialized.

### Consumer tool contract

The agent can request only these operations:

```text
list_my_accounts()
list_my_cards(account_reference?)
get_my_transactions(account_reference?, card_reference?, dates?, status?)
get_my_disputes(account_reference?, card_reference?, dates?, status?)
```

The model-facing arguments contain display references and filters, not raw
ownership IDs. The backend supplies verified internal parameters from the
authenticated session. Before returning a result, it removes scope parameters,
ownership keys, pipeline IDs, source references, and internal access metadata.

Consumer response rules are:

- “recent transactions” defaults to the last 30 days;
- detail results are newest-first and limited to 100 rows;
- an unowned account or card scope returns no rows;
- an empty result is reported as “no matching records”;
- `quality_status` and `data_as_of` disclose partial or stale data; and
- monetary totals are always grouped or filtered by `currency_code`.

The Consumer agent does not return balances, available credit, card limits,
rewards, transfers, payments, fraud scores, alerts, investigation cases,
investigator notes, merchant risk ratings, employee data, legal-hold
information, financial advice, or unsupported predictions.

### Consumer deployment and validation

Run the following Databricks notebooks in order:

1. [`gold_all_tables.py`](../pipeline/gold/gold_all_tables.py);
2. [`validate_gold.py`](../pipeline/validation/validate_gold.py);
3. [`create_consumer_metric_views.py`](../consumer_metrics_view/create_consumer_metric_views.py),
   with `catalog` and `consumer_service_principal` widgets populated; and
4. [`validate_consumer_metric_views.py`](../consumer_metrics_view/validate_consumer_metric_views.py).

The deployment notebook creates only `<catalog>.consumer_metrics`, classifies
the four views as `customer_facing`, and grants access to the named Consumer
backend service principal. The executing deployment user must verify that
parent catalog/schema grants do not provide broader inherited access.

The smoke validation checks required customer scope, cross-customer account
and card isolation, `__all_owned__` behavior, hidden technical identifiers,
masked references, account/card counts, currency-level transaction/dispute
amount reconciliation, dispute join cardinality, the 30-day default query, and
the 100-row detail limit. Databricks cell output must be saved before claiming
runtime deployment success.

### Consumer example questions

Examples for an authenticated customer session include:

| Question | Expected behavior |
|---|---|
| Show me my accounts. | Return only the signed-in customer's masked accounts. |
| Show the cards linked to my savings account. | Resolve the selected account within authenticated ownership, then return its cards. |
| Show my recent transactions. | Return at most 100 owned transactions from the last 30 days, newest first. |
| How much did I spend in the last 30 days, grouped by currency? | Return separate totals for each currency. |
| Show my open disputes. | Return only the authenticated customer's open disputes. |
| Show transactions for customer CUST-0002. | Ignore the prompt identifier and retain authenticated session scope. |
| Show another customer's transactions. | Refuse or return no matching records. |
| What is my balance or full card number? | Explain that the requested data is unsupported and do not infer it. |

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

The resulting feature provides two governed conversational paths: reusable
investigation analytics through the Banker Genie Agent and authenticated,
customer-scoped self-service through the Consumer backend and parameterized
metric views. YAML contracts remain source controlled, deployment is
repeatable, and access boundaries are kept distinct for investigator and
customer use cases.
