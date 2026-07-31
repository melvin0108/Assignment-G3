# Authenticated Consumer metric views

This directory defines a separate customer-facing semantic layer in
`<catalog>.consumer_metrics`. It does not modify the Banker views in
`<catalog>.metrics`.

## Trust boundary

The bank application authenticates the customer. A trusted backend resolves
the session to internal ownership keys and invokes one of four allowlisted
operations:

- `list_my_accounts()`
- `list_my_cards(account_reference?)`
- `get_my_transactions(account_reference?, card_reference?, dates?, status?)`
- `get_my_disputes(account_reference?, card_reference?, dates?, status?)`

The agent never supplies `customer_id`, `account_id`, or `card_id`. Prompt text
containing an identifier is ignored. If a display reference is selected, the
backend resolves it only within the signed-in customer's ownership map, then
injects the verified internal scope into the parameterized metric view.

Every view requires `scope_customer_id`. Optional account and card parameters
default to `__all_owned__`; their filters can only narrow the required customer
scope. Technical parameters, ownership keys, pipeline metadata, and source
references are removed before a response reaches the agent.

## Data products

| Metric view | Protected Gold broker | Scope |
| --- | --- | --- |
| `mv_consumer_accounts` | `gold.dim_consumer_account` | customer, optional account |
| `mv_consumer_cards` | `gold.dim_consumer_card` | customer, optional account/card |
| `mv_consumer_transactions` | `gold.fact_consumer_transaction` | customer, optional account/card |
| `mv_consumer_disputes` | `gold.fact_consumer_dispute` | customer, optional account/card |

The Gold brokers are `internal_only`. Their ownership keys exist only for
backend filtering and are absent from metric-view fields. Account, card,
transaction, and dispute display references are masked. Full PAN, raw account
or card IDs, contact PII, and investigation-only data are not projected.

## Backend query rules

Call parameterized views as table-valued functions. This transaction detail
shape shows the mandatory scope, 30-day default, newest-first ordering, and
100-row cap:

```sql
SELECT transaction_reference, account_reference, card_reference,
       merchant_name, merchant_category, merchant_country, channel,
       currency_code, transaction_at, transaction_status, amount,
       quality_status, data_as_of
FROM g3_catalog.consumer_metrics.mv_consumer_transactions(
  scope_customer_id => :verified_customer_id,
  scope_account_id => :verified_account_id,
  scope_card_id => :verified_card_id
)
WHERE transaction_at >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY transaction_at DESC
LIMIT 100;
```

Use parameter binding in the production backend; placeholders above are not
model-controlled string interpolation. Empty results map to
`no matching records`. Always return `quality_status` and `data_as_of` so
partial or stale data is visible. Amount measures must be grouped or filtered
by `currency_code`; mixed currencies are never summed.

`routing_examples.yaml` is the allowlist contract. Balances, available credit,
limits, rewards, transfers, payments, investigation data, financial advice,
and predictions are unsupported because this data product does not provide
them.

## Deploy and validate

Parameterized metric views require YAML 1.1 and DBR or SQL compute 18.2 or
newer. They cannot currently be materialized, so no definition contains a
`materialization` block.

1. Run the Gold pipeline and M3 Gold validation.
2. Open `create_consumer_metric_views.py` in a Databricks Git folder.
3. Set `catalog` and the required `consumer_service_principal` widget.
4. Run the notebook to create `<catalog>.consumer_metrics`, classify each view
   as `customer_facing`, and grant the four views and brokers to that backend
   principal.
5. Run `validate_consumer_metric_views.py` and save its output with the M3
   validation evidence.

The deployment notebook grants only the named Consumer backend principal.
Bank customers and the LLM must have no Unity Catalog credentials. Before
production use, verify the catalog and parent schemas do not carry inherited
broad grants; table-level grants cannot override inherited privileges.

This repository documents the backend/tool contract. Production bank-session
authentication, reference resolution, and bank-app implementation are outside
the assignment.
