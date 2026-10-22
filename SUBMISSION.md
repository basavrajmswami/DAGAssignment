# NammaMart assignment submission

## Part 0 — Discovery and data issues

All six source CSVs are copied unchanged to `include/data/nammamart/`. Their combined source count is 5,372 rows. The source scan found:

| File | Rows | Key profile | Issues identified |
| --- | ---: | --- | --- |
| `orders.csv` | 2,412 | 2,400 distinct order IDs; 12 extra duplicate-key rows | 1 blank timestamp, 18 blank customer IDs, 164 blank delivery durations (8 delivered), 18 formula mismatches, 10 invalid quantities, 3 invalid discounts, 3 invalid statuses, 6 durations >=120 minutes, unknown customer/product/store keys, and non-standard payment modes |
| `payments.csv` | 2,228 | 2,224 distinct payment IDs; 4 extra duplicate-key rows | 301 blank gateway references, 8 payments without a matching order, and 68 REFUNDED plus 44 FAILED statuses |
| `customers.csv` | 405 | 400 distinct customer IDs; 5 extra duplicate-key rows | 6 blank cities, 12 malformed emails, 7 non-10-digit phones, 3 future signup dates, and inconsistent loyalty-tier casing |
| `products.csv` | 152 | 150 distinct product IDs; 2 extra duplicate-key rows | 3 blank categories, 1 non-positive/invalid MRP, and 5 invalid cost-price values (some issues overlap) |
| `stores.csv` | 15 | 15 distinct store IDs | 1 blank zone |
| `orders_2026-10-01_increment.csv` | 160 | 10 order IDs overlap the September file; 150 are new | Contains ten corrections and 150 new orders; no blank fields found in the source scan |

Keys are normalized for casing and whitespace before validation. The incremental file is ranked above the base extract so its ten corrections replace, rather than duplicate, their prior order rows. The source scan also found 9 orders referencing unknown product IDs, 5 referencing unknown store IDs, and 8 payments referencing unknown order IDs. Counts can overlap because a row can break multiple rules.

| File | Row key | Problem / quality dimension | Proposed severity |
| --- | --- | --- | --- |
| Orders | `order_id` | Duplicate key, missing customer, invalid timestamp/status/mode, bad amount formula, impossible quantity/duration, or unknown dimension key; uniqueness/completeness/validity/accuracy/consistency/timeliness | Critical, quarantine the rejected copy |
| Payments | `payment_id` | Duplicate key, orphan order, or invalid payment value/status; uniqueness/consistency/validity | Critical; failed or refunded payment remains visible for reconciliation |
| Customers | `customer_id` | Duplicate key and invalid signup date; uniqueness/validity | Critical for duplicate/date; email/phone format is a warning |
| Products | `product_id` | Duplicate key, blank category, or invalid price; uniqueness/completeness/accuracy | Critical |
| Stores | `store_id` | Blank/invalid zone; completeness/validity | Critical |
| Increment | `order_id` | Late status correction or new row; timeliness/uniqueness | Valid corrections upsert; reject any row that fails normal order checks |

The issue log below gives representative source keys (counts cover every matching row, not only the examples). The source CSV line number is the data-row number plus one for the header.

| File / row key examples | Column | Problem and count | Quality dimension | Proposed rule | Severity |
| --- | --- | --- | --- | --- | --- |
| `orders.csv`: `ORD001462`, `ORD001326` | `order_id` | 12 duplicated keys (24 flagged source rows) | Uniqueness | Keep one canonical row; quarantine duplicate extras | Critical |
| `orders.csv`: `ORD000362` | `order_ts` | 1 blank; malformed timestamps also fail parsing | Completeness / validity | Require a parseable timestamp | Critical |
| `orders.csv`: `ORD002255`, `ORD000701` | `customer_id` | 18 blank or unrecognized customer references | Completeness / consistency | Require a customer key present in the accepted customer dimension | Critical |
| `orders.csv`: `ORD000986`, `ORD001058` | `product_id` | 9 unknown product references | Consistency | Require a key present in the accepted product dimension | Critical |
| `orders.csv`: `ORD001799`, `ORD001046` | `store_id` | 5 unknown/invalid store references | Consistency | Require an accepted store dimension key | Critical |
| `orders.csv`: `ORD002103`, `ORD002305` | `order_amount` | 18 formula mismatches | Accuracy | Recompute and compare to two decimal places | Critical |
| `orders.csv`: `ORD000469`, `ORD001296` | `quantity` | 10 non-positive or unparseable quantities | Accuracy | Require `quantity > 0` | Critical |
| `orders.csv`: `ORD002103`, `ORD001468` | `discount_pct` | 3 values outside 0–100 | Validity | Require `0 <= discount_pct <= 100` | Critical |
| `orders.csv`: `ORD000500`, `ORD001970` | `order_status` | 3 invalid `Pending??` values | Validity | Allow DELIVERED, CANCELLED, RETURNED after case normalization | Critical |
| `orders.csv`: `ORD001812`, `ORD001414` | `payment_mode` | 2 misspelled `Cashh` values | Validity | Allow only UPI, Card, Cash on Delivery, Wallet; normalize case/spacing | Critical |
| `orders.csv`: `ORD000682`, `ORD000910` | `delivery_minutes` | 164 blanks (8 delivered orders) and 6 values >=120 | Completeness / accuracy | Delivered orders require a duration below 120 minutes | Critical when delivered; otherwise warning |
| `payments.csv`: `PAY0001651`, `PAY0000755` | `payment_id` | 4 duplicated keys (8 flagged source rows) | Uniqueness | Keep one canonical payment and quarantine duplicate extras | Critical |
| `payments.csv`: `PAY0002219`, `PAY0002220` | `order_id` | 8 unmatched payment references | Consistency | Require a source order; retain and report mismatches | Critical |
| `payments.csv`: `PAY0000133`, `PAY0001514` | `gateway_ref` | 301 blank references | Completeness | Warn; require a reference for gateway audit before production | Warning |
| `customers.csv`: `C00103`, `C00252` | `customer_id` | 5 duplicated keys (10 flagged source rows) | Uniqueness | Keep one canonical dimension row; quarantine duplicate extras | Critical |
| `customers.csv`: `C00337`, `C00307` | `email` | 12 malformed email values | Validity | Warn, mask in reporting, and request CRM correction | Warning |
| `customers.csv`: `C00151`, `C00240` | `phone` | 7 values are not 10 digits | Validity | Warn, mask in reporting, and request CRM correction | Warning |
| `customers.csv`: `C00400`, `C00398` | `signup_date` | 3 invalid or future dates | Validity / timeliness | Require a valid non-future signup date | Critical |
| `customers.csv`: `C00355`, `C00394` | `city` | 6 blank values | Completeness | Warn because city is not a required join key | Warning |
| `customers.csv`: `C00103`, `C00252` | `loyalty_tier` | Mixed case (`Bronze`, `BRONZE`, `bronze`) | Consistency | Normalize to title case | Warning |
| `products.csv`: `P0030`, `P0140` | `product_id` | 2 duplicated keys (4 flagged source rows) | Uniqueness | Keep one canonical product row; quarantine duplicate extras | Critical |
| `products.csv`: `P0051`, `P0060` | `category` | 3 blank categories | Completeness | Require a category for category-level reporting | Critical |
| `products.csv`: `P0109`, `P0019`, `P0073` | `mrp`, `cost_price` | 1 invalid MRP and 5 invalid cost values | Accuracy | Require positive MRP and `0 <= cost_price <= mrp` | Critical |
| `stores.csv`: `S07` | `zone` | 1 blank zone | Completeness / validity | Require one of North, South, East, West | Critical |

Three questions for the client: (1) Should a returned order reduce the original order-date revenue or be represented as a separate return transaction? (2) Are Cash-on-Delivery orders considered reconciled only after settlement, or is order creation sufficient? (3) Is the 120-minute delivery ceiling an absolute critical limit or an operational warning?

## Parts A and B — Pipelines and outputs

Both DAGs use `0 9 * * 1-6`, an explicit `Asia/Kolkata` start date, `catchup=False`, two retries, a two-minute retry delay, and a 30-minute task execution timeout. ETL and ELT use separate per-user DuckDB files so simultaneous 09:00 runs do not compete for one DuckDB write lock.

**ETL graph:** five parallel `extract_*` tasks → `validate` → `transform` → `load_clean` → `reconcile` → `report`. `validate` also branches to a separate `quarantine` task, which writes run-stamped CSVs with `failure_reason`, following the separate-task pattern in `meridian_pipeline.py`. A final `audit_run` follows both the report and quarantine branch. The audit determines success from the upstream tasks' return-value XComs, which are available in the Airflow 3 task context, rather than querying task instances through `DagRun`. The `load_clean` task ID matches the expected “Load clean” stage and writes `etl_fact_orders`. Reconciliation, reporting, and audit are additional distinct tasks required by Part A of the assignment.

**ELT graph:** five parallel `extract_*` tasks → `load_raw` → `transform_staging` → `transform_marts` → `audit_run`. `load_raw` appends text source rows to raw tables; SQL staging and marts are separate Airflow tasks.

ETL critical failures are quarantined; warning-level email/phone format problems are logged but retained. Accepted order keys are delete-and-insert upserted. The increment file's ten corrections supersede their base rows and its 150 new order IDs are added. ELT preserves each run in raw tables and uses SQL row precedence to materialize one current staging row per `order_id`.

**Executed Airflow runs used below:** ETL `manual__2026-10-22T15:45:18.518719+00:00` completed at 21:17 IST with status `success` (5,372 extracted, 2,147 loaded, 456 quarantined). ELT `manual__2026-10-22T14:59:18.148904+00:00` completed at 20:30 IST with status `success` (5,372 extracted, 4,908 staged rows counted, 431 failed). These are the latest successful audit rows in their respective warehouse files; they ran at different times.

### Business questions — executed SQL and results

Queries below use the generated local DuckDB files. The daily revenue query returns all 429 date/zone/store rows; a concise result sample and its monthly totals are shown. Query outputs are rounded to two decimal places for readability.

#### 1. Daily net revenue by zone and store

```sql
-- ETL
SELECT order_date, zone, store_name, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone, store_name;

-- ELT
SELECT order_date, zone, store_name, net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_name;
```

**Results (both pipelines):** 429 daily store rows; total delivered revenue **1,052,922.85**. September contributes **982,401.20** across 414 rows; October contributes **70,521.65** across 15 rows. The first result row is `2026-09-01 | East | NammaMart HSR Layout | 2,121.15`. The remaining rows are returned by the queries above.

#### 2. Revenue and gross margin by product category

```sql
-- ETL
SELECT category, revenue, gross_margin
FROM etl_category_margin ORDER BY category;

-- ELT
SELECT category, revenue, gross_margin
FROM mart_category_margin ORDER BY category;
```

| Category | ETL revenue | ETL gross margin | ELT revenue | ELT gross margin |
| --- | ---: | ---: | ---: | ---: |
| Dairy & Bakery | 169,005.00 | 44,761.99 | 169,005.00 | 44,761.99 |
| Fruits & Vegetables | 86,740.95 | 20,538.74 | 86,740.95 | 20,538.74 |
| Household | 194,112.90 | 49,547.43 | 194,112.90 | 49,547.43 |
| Personal Care | 195,893.45 | 43,799.67 | 195,893.45 | 43,799.67 |
| Snacks & Beverages | 166,393.70 | 39,115.74 | 166,393.70 | 39,115.74 |
| Staples | 240,776.85 | 61,194.48 | 240,776.85 | 61,194.48 |

#### 3. Average delivery time and within-15-minute rate by store

```sql
-- ETL
SELECT store_id, store_name, zone, avg_delivery_minutes,
       within_15_min_pct, delivered_orders
FROM etl_delivery_sla ORDER BY store_id;

-- ELT
SELECT store_id, store_name, zone, avg_delivery_minutes,
       within_15_min_pct, delivered_orders
FROM mart_delivery_sla ORDER BY store_id;
```

**Results (same in both pipelines; minutes and percentages rounded):**

| Store | Average minutes | Within 15 min | Delivered orders |
| --- | ---: | ---: | ---: |
| S01 | 20.62 | 31.21% | 141 |
| S02 | 20.99 | 30.94% | 139 |
| S03 | 20.69 | 28.99% | 138 |
| S04 | 21.59 | 26.92% | 130 |
| S05 | 21.24 | 29.10% | 134 |
| S06 | 22.33 | 22.07% | 145 |
| S08 | 20.60 | 35.48% | 155 |
| S09 | 21.14 | 27.56% | 127 |
| S10 | 21.90 | 26.95% | 141 |
| S11 | 20.97 | 27.78% | 144 |
| S12 | 21.99 | 23.89% | 113 |
| S13 | 21.80 | 27.01% | 137 |
| S14 | 21.50 | 31.48% | 162 |
| S15 | 22.33 | 20.98% | 143 |

S07 has no result because its source store record fails validation.

#### 4. Revenue by payment mode and amount that does not reconcile

```sql
-- ETL
SELECT payment_mode, revenue
FROM etl_revenue_by_payment_mode ORDER BY payment_mode;

SELECT COUNT(*) AS exception_rows,
       SUM(CASE WHEN order_amount IS NOT NULL THEN order_amount
                ELSE amount_paid END) AS exception_amount_exposure,
       SUM(ABS(COALESCE(order_amount, 0) - COALESCE(amount_paid, 0)))
           AS absolute_amount_difference
FROM etl_payment_reconciliation;

-- ELT
SELECT payment_mode, revenue
FROM mart_revenue_by_payment_mode ORDER BY payment_mode;

SELECT COUNT(*) AS exception_rows,
       SUM(CASE WHEN order_amount IS NOT NULL THEN order_amount
                ELSE amount_paid END) AS exception_amount_exposure,
       SUM(ABS(COALESCE(order_amount, 0) - COALESCE(amount_paid, 0)))
           AS absolute_amount_difference
FROM mart_payment_reconciliation;
```

| Payment mode | ETL revenue | ELT revenue |
| --- | ---: | ---: |
| Card | 216,908.85 | 216,908.85 |
| Cash on Delivery | 153,611.85 | 153,611.85 |
| UPI | 559,699.15 | 559,699.15 |
| Wallet | 122,703.00 | 122,703.00 |

| Reconciliation result | ETL | ELT |
| --- | ---: | ---: |
| Exception rows | 573 | 217 |
| Exception amount exposure* | 348,434.80 | 118,729.32 |
| Absolute amount difference** | 328,774.20 | 91,156.72 |

\* Exposure sums `order_amount` where present, otherwise `amount_paid`; it is the value associated with exception rows, not a claim of net financial loss.  
\** Absolute difference sums `ABS(order_amount - amount_paid)`, treating a missing side as zero. Exception counts differ because ETL compares payments to accepted clean orders, while ELT reconciliation checks its passed staged orders and retained payments.

#### 5. Top 10 customers by spend and loyalty tier

```sql
-- ETL
SELECT customer_id, loyalty_tier, total_spend, delivered_orders
FROM etl_top_customers ORDER BY total_spend DESC;

-- ELT
SELECT customer_id, loyalty_tier, total_spend, delivered_orders
FROM mart_top_customers ORDER BY total_spend DESC;
```

**Results (same in both pipelines):**

| Rank | Customer | Loyalty tier | Spend | Delivered orders |
| ---: | --- | --- | ---: | ---: |
| 1 | C00136 | Bronze | 8,149.10 | 8 |
| 2 | C00115 | Bronze | 7,337.70 | 7 |
| 3 | C00310 | Bronze | 7,258.95 | 8 |
| 4 | C00236 | Platinum | 7,151.60 | 10 |
| 5 | C00265 | Gold | 6,940.30 | 7 |
| 6 | C00338 | Silver | 6,696.55 | 8 |
| 7 | C00072 | Silver | 6,433.95 | 11 |
| 8 | C00283 | Bronze | 6,241.40 | 6 |
| 9 | C00129 | Bronze | 6,215.10 | 10 |
| 10 | C00202 | Silver | 6,096.75 | 7 |

#### 6. Rejected rows today, by file and reason

```sql
-- ETL: this is the file name generated by the successful run listed above.
SELECT 'orders' AS source_file, failure_reason, COUNT(*) AS rejected_rows
FROM read_csv_auto('include/quarantine/basavraj/orders_failed_manual__2026-10-22T15_45_18.518719_00_00.csv') GROUP BY failure_reason
UNION ALL
SELECT 'orders_increment', failure_reason, COUNT(*)
FROM read_csv_auto('include/quarantine/basavraj/orders_increment_failed_manual__2026-10-22T15_45_18.518719_00_00.csv') GROUP BY failure_reason
UNION ALL
SELECT 'payments', failure_reason, COUNT(*)
FROM read_csv_auto('include/quarantine/basavraj/payments_failed_manual__2026-10-22T15_45_18.518719_00_00.csv') GROUP BY failure_reason
UNION ALL
SELECT 'customers', failure_reason, COUNT(*)
FROM read_csv_auto('include/quarantine/basavraj/customers_failed_manual__2026-10-22T15_45_18.518719_00_00.csv') GROUP BY failure_reason
UNION ALL
SELECT 'products', failure_reason, COUNT(*)
FROM read_csv_auto('include/quarantine/basavraj/products_failed_manual__2026-10-22T15_45_18.518719_00_00.csv') GROUP BY failure_reason
UNION ALL
SELECT 'stores', failure_reason, COUNT(*)
FROM read_csv_auto('include/quarantine/basavraj/stores_failed_manual__2026-10-22T15_45_18.518719_00_00.csv') GROUP BY failure_reason
ORDER BY source_file, rejected_rows DESC;

-- ELT: rank raw rows with the same increment precedence used by staging to
-- report the original file for each rejected canonical row.
WITH order_source AS (
    SELECT order_id, _source_file,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY CASE WHEN _source_file LIKE '%increment%' THEN 1 ELSE 0 END DESC,
                        _loaded_at DESC, rowid DESC
           ) AS row_rank
    FROM raw_orders
), payment_source AS (
    SELECT payment_id, _source_file,
           ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC, rowid DESC) AS row_rank
    FROM raw_payments
), customer_source AS (
    SELECT customer_id, _source_file,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC, rowid DESC) AS row_rank
    FROM raw_customers
), product_source AS (
    SELECT product_id, _source_file,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC, rowid DESC) AS row_rank
    FROM raw_products
), store_source AS (
    SELECT store_id, _source_file,
           ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _loaded_at DESC, rowid DESC) AS row_rank
    FROM raw_stores
)
SELECT r._source_file AS source_file, s.failure_reason, COUNT(*) AS rejected_rows
FROM stg_orders s JOIN order_source r ON r.order_id = s.order_id AND r.row_rank = 1
WHERE s.dq_status = 'failed' GROUP BY 1, 2
UNION ALL
SELECT r._source_file, s.failure_reason, COUNT(*)
FROM stg_payments s JOIN payment_source r ON r.payment_id = s.payment_id AND r.row_rank = 1
WHERE s.dq_status = 'failed' GROUP BY 1, 2
UNION ALL
SELECT r._source_file, s.failure_reason, COUNT(*)
FROM stg_customers s JOIN customer_source r ON r.customer_id = s.customer_id AND r.row_rank = 1
WHERE s.dq_status = 'failed' GROUP BY 1, 2
UNION ALL
SELECT r._source_file, s.failure_reason, COUNT(*)
FROM stg_products s JOIN product_source r ON r.product_id = s.product_id AND r.row_rank = 1
WHERE s.dq_status = 'failed' GROUP BY 1, 2
UNION ALL
SELECT r._source_file, s.failure_reason, COUNT(*)
FROM stg_stores s JOIN store_source r ON r.store_id = s.store_id AND r.row_rank = 1
WHERE s.dq_status = 'failed' GROUP BY 1, 2
ORDER BY source_file, rejected_rows DESC;
```

**Results by file:** ETL quarantined **456** rows: `orders.csv` **396**, increment orders **20**, `payments.csv` **21**, `customers.csv` **8**, `products.csv` **10**, `stores.csv` **1**. The largest ETL base-order reason groups were unknown store (**167**), unknown product (**116**), unknown customer (**14**), duplicate order ID (**12**), and formula mismatch (**11**); combined reasons are retained as combined strings in the query output. ELT rejected **431** staged rows: `orders.csv` **383**, `orders_2026-10-01_increment.csv` **20**, `payments.csv` **16**, `customers.csv` **3**, `products.csv` **8**, `stores.csv` **1**. ELT's largest base-order reason groups were unknown store (**167**), unknown product (**124**), missing required fields (**18**), unknown customer (**16**), and formula mismatch (**15**). The SQL returns each exact file/reason/count row, including the remaining reason groups.

Both runs produced the same delivered-revenue, category, delivery, payment-mode, and top-customer results. The daily revenue marts contain **982,401.20** for September and **70,521.65** for October. Rejected ETL rows are retained in run-stamped quarantine CSVs; ELT retains raw rows and reports `failure_reason` in staging. `pipeline_runs` records one audit row per DAG run.

## Part C — Schedule

The cron expression is `0 9 * * 1-6`; this is 03:30 UTC. Leaving `start_date` in UTC while retaining this cron would run at 09:00 UTC, or 14:30 IST. `catchup=False` avoids creating a backlog of historical runs after deployment or downtime. If the laptop or Docker is off, Airflow cannot run at 09:00; with catchup disabled it will not replay that missed schedule automatically.

## Part D — Data quality dimensions

Both pipelines record critical row-level failures separately from warning-level checks. ETL sends critical failures to quarantine with `failure_reason`; ELT keeps the source in raw and marks failed staged rows with a `failure_reason`, excluding them from marts.

| Dimension | Rules applied |
| --- | --- |
| Completeness | Required entity/order/payment keys and required product/store attributes must be present; delivered orders require delivery duration. Missing optional customer contact fields are warnings. |
| Accuracy | Quantity and prices must be valid, `order_amount` must match quantity × unit price after discount, delivery duration must be below 120 minutes, and product cost must not exceed MRP. |
| Validity | Timestamps, order/payment statuses, payment modes, discount ranges, customer signup dates, and store zones must use valid values. |
| Consistency | Order customer/product/store keys must resolve to accepted dimensions; payments must reference a known order. |
| Uniqueness | Duplicate customer/product/store/order/payment keys are identified; duplicate source rows are rejected in ETL and canonical latest rows are selected in ELT staging. |
| Timeliness | Future order/signup timestamps fail validation; an order feed more than one day stale records a warning. |

## Part E — Governance

1. **Ownership:** Ops owns order definitions, Finance owns payment/reconciliation sign-off, CRM owns customer data, Merchandising owns products, and Store Operations owns store master data. Finance and the relevant domain owner approve changes to revenue or quality rules; Ops approves delivery-SLA changes.
2. **Dictionary:** core columns and policies:

| Table / column | Type | Meaning / source | Allowed values | PII |
| --- | --- | --- | --- | --- |
| `etl_fact_orders.order_id` / `stg_orders.order_id` | VARCHAR | Order key from order export | Unique, non-empty in mart | No |
| `order_ts`, `order_date` | TIMESTAMP, DATE | Source order time and derived date | Parseable; not future | No |
| `customer_id`, `product_id`, `store_id` | VARCHAR | Order foreign keys | Must resolve to accepted dimensions | Customer key is pseudonymous |
| `quantity`, `unit_price`, `discount_pct`, `order_amount` | INTEGER / DECIMAL | Order measures from order export | quantity > 0; discount 0–100; amount formula checked | No |
| `payment_mode`, `order_status` | VARCHAR | Order classifications | UPI / Card / Cash on Delivery / Wallet; DELIVERED / CANCELLED / RETURNED | No |
| `delivery_minutes`, `within_15_min` | INTEGER, BOOLEAN | Source duration and derived SLA flag | Delivered duration < 120 | No |
| `net_revenue`, `gross_margin` | DECIMAL | Derived revenue and margin | Only DELIVERED contributes revenue | No |
| `etl_dim_customer.customer_id`, `loyalty_tier` | VARCHAR | CRM key and normalized tier | Bronze / Silver / Gold / Platinum | Customer key is pseudonymous |
| `full_name`, `email`, `phone` | VARCHAR | CRM personal attributes, redacted/masked in reporting | Masked values only in reporting/staging | Yes, masked |
| `etl_dim_product.product_id`, `category`, `cost_price` | VARCHAR, VARCHAR, DECIMAL | Product catalogue attributes | Known category; cost 0–MRP | No |
| `etl_dim_store.store_id`, `zone` | VARCHAR | Store master attributes | North / South / East / West | No |
| `raw_*._source_file`, `_run_id`, `_loaded_at` | VARCHAR, VARCHAR, TIMESTAMPTZ | ELT lineage metadata | Source filename, run ID, load timestamp | No |

ELT raw business columns stay as source text; raw customer contact data remains restricted personal data.
3. **Lineage:** ETL's `ORD001913` contribution is 421.80 net revenue from `include/data/nammamart/orders.csv`, row 2 (`order_id=ORD001913`), for 24 September, store S10 (NammaMart Marathahalli, East). ETL can trace the accepted fact to its source order; ELT is easier to audit because it preserves every raw source row and `_source_file`.
4. **Personal data:** Customer `full_name`, `email`, and `phone`, and store `manager_name`, are treated as personal data. Names are redacted and email/phone are masked in reporting/staging; raw customer values are restricted to ELT raw storage for audit. India’s Digital Personal Data Protection Act, 2023 (DPDP Act) requires lawful-purpose processing, appropriate safeguards, and honoring data-principal rights and retention limits.
5. **Access:** Raw is limited to data engineering and authorized audit/compliance staff; staging to data engineering and privacy-approved analysts; marts to business users; quarantine to data engineering and the owning domain steward. Unmasked raw customer phone/email should not be available to general analysts.
6. **Retention:** Retain raw data only for the approved audit/business period, quarantine only until remediation and the audit window close, and masked customer data for the active analytical purpose. Apply documented deletion or anonymization at expiry, subject to applicable legal and finance retention requirements.
7. **Secrets:** Store credentials in Airflow Connections/Variables or a secrets backend and retrieve them at runtime. No secret is embedded in DAG code, source files, logs, or Git history.
8. **Audit:** ETL exposes the rejected row and reason in its run-stamped quarantine file and its absence from `etl_fact_orders`; ELT retains the original in `raw_orders`, stores the failure in `stg_orders.failure_reason`, and excludes `dq_status='failed'` rows from CFO-facing marts.
9. **Change history:** The increment overwrites the current order status to RETURNED, so the September restated delivered revenue excludes it. Finance should use the restated result for current-period net revenue; retain the original as-of view through append-only raw history and a dated snapshot/versioned fact or SCD history.

## Part F — Observability and validation status

Implemented signals include run-audit rows, task logs, quarantine or stage failure reasons, six DQ dimensions, stale-data warnings, a 90%-drop volume threshold, a price distribution warning, and explicit required-column references that fail on schema changes.

| Experiment | Expected | Observed locally | Where / change |
| --- | --- | --- | --- |
| Happy path | Both DAGs complete and populate tables | ETL/ELT transformation functions and SQL completed; Airflow task run not performed | Temporary DuckDBs; Graph screenshot remains |
| Re-run | ETL facts unchanged; ELT raw grows and stages remain deduplicated | ETL remained at 2,147 facts; ELT raw orders grew to 5,144 with 2,550 staged IDs | Two-run local data test; UI history remains |
| Increment | 150 new IDs and 10 corrections, no duplicate staged key | All ten corrections staged as RETURNED; staged table has 2,550 distinct IDs | Temporary DuckDB |
| Missing file | Retries then task/DAG failure, audit row marked failed | ETL extraction helper raises `FileNotFoundError` for a missing source; Airflow retry states/timing and audit behavior not observed | Direct helper test; scheduler experiment pending |
| New bad data | Critical rows carry reasons and do not enter finance marts | Five injected rows rejected by both Python validation and SQL staging with expected reasons | Temporary copies; source dataset unchanged |
| Schema change | Missing required column fails loudly | Python validation raised `ValueError`; SQL staging raised a DuckDB `BinderException` for missing `order_amount` | Temporary schema copies |
| Volume anomaly | A 50-row feed sets a warning in `dq_results` | ETL and ELT checks recorded warnings for the 50-row feed | Temporary DuckDBs |
| Distribution drift | Multiplying S01 `unit_price` by 100 warns and changes a mart metric | Both checks warned; S01 delivered revenue moved from **80,069.00** to **7,335,655.25** (**+7,255,586.25**); ETL flagged 158 rows and ELT flagged the run | Temporary modified copies; `dq_results` and daily-store mart |
| Freshness | Latest order older than one day warns; future dates fail | ETL and ELT marked a 30-day-old feed as `warning`; future signup/order timestamps were rejected | Temporary DuckDBs; DQ scorecard/stage reason |
| Recovery | Only failed task and downstream tasks rerun after Clear | Airflow recovery experiment not run | Requires running Airflow |

`astro dev parse` passed all four DAG integrity checks in an isolated Astro runtime, with no DAG import errors. This confirms parsing, not a scheduled or manually triggered Airflow run. The earlier local Astro containers exited with code 137, so UI screenshots, green scheduler runs, observed parallel timing, retry states/alerts, and Clear-based recovery remain unverified.

| Observability pillar | Coverage |
| --- | --- |
| Freshness | Current feed maximum order timestamp and future timestamps |
| Volume | Warning when the daily orders file falls below 10% of the supplied 2,412-row baseline |
| Schema | Missing required CSV columns fail the task |
| Distribution | Warning when maximum unit price exceeds ten times the median |
| Lineage | Run/source metadata in ELT raw and source files in ETL quarantine |

Lineage is strongest in ELT because raw rows retain their source file and run metadata. Distribution is weakest: a simple outlier warning is not a full historical drift model.

## Part G — Recommendation to Sameer

**Recommendation: adopt ELT with a strict, quality-gated mart boundary.** Keep the append-only raw layer restricted, preserve every source row and load version for audit, and publish only rows that pass critical staging checks to Finance marts. This addresses Finance's no-bad-row requirement without discarding the history Compliance and Analytics need. The local test produced identical September delivered revenue (982,401.20) in ETL and ELT, while the ELT raw layer retained the original feed and its October correction.

SQL staging makes rule changes and replay easier than rebuilding a one-off cleaned extract, and DuckDB is appropriate for this assignment-scale workload. At 1,000× volume, move the same layered design to a managed columnar warehouse/object store and partitioned orchestration rather than assuming a local DuckDB file will scale operationally. The open-source local setup has low direct software cost, but the team still owns operation, access controls, backups, and recovery.

Top risks: (1) raw PII exposure without strict access control and retention; (2) incorrect SQL rules silently excluding good records or publishing bad marts unless `dq_results` and reconciliations are monitored; and (3) local DuckDB/storage and laptop/Docker availability not meeting production scale or the 09:00 SLA without a durable hosted runtime.

## Assignment completion check

| Assignment part | Status |
| --- | --- |
| Part 0: profile the supplied six CSVs and record issues | Complete; source copies were kept unchanged. |
| Parts A–B: implement the ETL/ELT graphs, data handling, and business outputs | Implemented and locally exercised; Astro parse passed all four DAG integrity checks. |
| Part C: schedule at 09:00 IST Monday–Saturday | Configured as `0 9 * * 1-6` with `Asia/Kolkata`, `catchup=False`, retries, and timeout; UI schedule proof is pending. |
| Part D: six quality dimensions | Implemented in Python and SQL; bad-data, schema, volume, distribution, and freshness checks were locally exercised. |
| Part E: governance | Written in this submission. |
| Part F: ten observability experiments | Data-layer tests are recorded above; missing-file retry behavior and recovery after Airflow Clear, and all UI evidence, remain pending. |
| Part G: client recommendation | Written above. |
| Required screenshots and green Airflow runs | Pending; the project has been parsed but not run through the Airflow UI. |
