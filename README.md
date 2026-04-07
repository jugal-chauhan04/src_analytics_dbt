# src_analytics dbt Project

## Business Context

**src_analytics** is a SaaS organization offering three products across three subscription plans.

| Product | Plans Available |
|---|---|
| AutomateSRC | Free, Pro, Premium |
| CollabSRC | Free, Pro, Premium |
| InsightSRC | Free, Pro, Premium |

Plans are billed either **monthly** or **yearly**. Yearly subscribers pay upfront and receive a single invoice per year, while monthly subscribers are invoiced each month.

---

## What This Project Does

This dbt project transforms src_analytics raw transactional data into clean, reliable MRR (Monthly Recurring Revenue) models for revenue reporting and analysis.

MRR is the backbone metric for any SaaS business — it answers *"how much recurring revenue are we generating each month?"* We model three variants of MRR because each answers a slightly different version of that question:

| MRR Type | Definition | Source |
|---|---|---|
| **Gross MRR** | Revenue at full list price, before any discounts | Plan price |
| **Net MRR** | Revenue actually invoiced after discounts applied | Invoice `amount_due` |
| **Collected MRR** | Revenue actually received — successful payments only | Payments table |

Together these three let the business understand not just what revenue was expected, but how much was discounted away and how much was actually collected. The gap between net MRR and collected MRR represents **revenue at risk** — invoiced but not yet or never collected.

---

## Model Architecture

```
stg__subscriptions ─┐
stg__plans         ─┴──→ int__subscription_plan_join
                                    │
                    ┌───────────────┴───────────────┐
                    ↓                               ↓
            int__mrr_join              int__subscription_payments_join
         (gross + net amounts)         (successful payments only)
                    │                               │
          ┌─────────┴─────────┐                    ↓
          ↓                   ↓             mart__collected_mrr
    mart__gross_mrr     mart__net_mrr
```

### Staging Layer
One model per raw source table. Renames columns, casts types, and standardizes values. No business logic.

### Intermediate Layer
Three models that handle all the heavy lifting:

- **`int__subscription_plan_join`** — One row per subscription enriched with plan metadata. Only join, no calculations. Shared foundation for both downstream intermediates.
- **`int__mrr_join`** — One row per subscription per active billing month carrying both gross and net monthly amounts. Feeds both gross and net mart models.
- **`int__subscription_payments_join`** — One row per subscription per active billing month carrying collected payment amounts. Payments are pre-aggregated before joining to avoid double counting from retry attempts.

### Mart Layer
Three aggregated models — one per MRR variant. Each is materialized as a table.

- **`mart__gross_mrr`**
- **`mart__net_mrr`**
- **`mart__collected_mrr`**

All three share the same grain and structure: one row per `billing_month` per `product_id`, `plan_id`, `plan_name`, `billing_type` combination.

---

## Key Design Decisions

### Subscription-period spine, not invoice explosion

The first approach considered was exploding yearly invoices into 12 rows — one per month. This was abandoned because it struggled to correctly handle mid-year cancellations, invoice date drift, and upgrades or downgrades where two subscriptions could overlap.

Instead, the spine is built from the **subscription's own start and end dates** using `GENERATE_DATE_ARRAY` + `UNNEST`. This generates one row per active calendar month per subscription, and the invoice is treated as a lookup — consulted only for the amount, not used to drive the temporal logic.

```sql
-- For a subscription starting 2024-01-01 and ending 2024-03-31
-- GENERATE_DATE_ARRAY produces: [2024-01-01, 2024-02-01, 2024-03-01]
-- UNNEST expands that into individual rows:

subscription_id | billing_month
101             | 2024-01-01
101             | 2024-02-01
101             | 2024-03-01
```

Cancellations are handled naturally — the spine simply doesn't generate rows beyond `cancel_date`. Upgrades and downgrades are handled the same way — the old subscription's spine ends when its `end_date` is set, and the new subscription's spine begins at its own `start_date`.

### Yearly invoice anchor math

Yearly subscribers receive one invoice per year. All 12 months in a subscription year need to map to that same invoice. The join uses integer division to find which subscription year a `billing_month` belongs to, then anchors back to the invoice issued at the start of that year:

```
Subscription start: 2024-06

billing_month   maps to invoice at
2024-06     →   2024-06  (year 1 start)
2024-09     →   2024-06  (year 1, month 4)
2025-03     →   2024-06  (year 1, month 10)
2025-06     →   2025-06  (year 2 start)
2025-09     →   2025-06  (year 2, month 4)
```

The collected amount (or `amount_due`) is then divided by 12 to produce a normalized monthly figure, consistent across all three MRR variants.

### Gross and net MRR share one spine

Rather than building two separate intermediate models — one for gross, one for net — both amounts are computed in `int__mrr_join` and carried as separate columns. This means `mart__gross_mrr` and `mart__net_mrr` both read from the same model, avoiding duplicated spine logic.

### Payment pre-aggregation before joining

A single invoice can have multiple payment attempts if the first attempt fails and is retried. In `int__subscription_payments_join`, payments are aggregated to one row per invoice — summing only successful amounts — before joining to the spine. Aggregating after the join would cause double counting.

---

## Mart Output Example

After aggregation, each mart produces one row per billing month per dimension combination:

```
billing_month | product_id | plan_id | plan_name | billing_type | gross_mrr
2024-01-01    | 1          | 2       | Pro       | monthly      | 4500.00
2024-01-01    | 1          | 3       | Premium   | yearly       | 2916.67
2024-01-01    | 2          | 2       | Pro       | monthly      | 1800.00
2024-02-01    | 1          | 2       | Pro       | monthly      | 4500.00
...
```

Analysts can roll up to total company MRR by summing all rows for a given `billing_month`, or slice by product, plan, or billing type without any additional joins.