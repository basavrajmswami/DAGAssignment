CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR,
    dag_id VARCHAR,
    check_name VARCHAR,
    dimension VARCHAR,
    severity VARCHAR,
    rows_checked BIGINT,
    rows_failed BIGINT,
    status VARCHAR,
    checked_at TIMESTAMPTZ
);

CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC, rowid DESC) AS _rn
    FROM raw_customers
), normalized AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        CASE WHEN TRIM(full_name) = '' THEN '' ELSE '***' END AS full_name,
        CASE WHEN STRPOS(TRIM(email), '@') > 1
             THEN SUBSTR(TRIM(email), 1, 2) || '***' || SUBSTR(TRIM(email), STRPOS(TRIM(email), '@'))
             ELSE '***' END AS email,
        CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) >= 4
             THEN SUBSTR(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 1, 2)
                  || REPEAT('*', GREATEST(LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) - 4, 0))
                  || RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 2)
             ELSE REPEAT('*', LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'))) END AS phone,
        TRIM(city) AS city, TRIM(area) AS area, TRIM(pincode) AS pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date,
        UPPER(SUBSTR(TRIM(loyalty_tier), 1, 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2)) AS loyalty_tier,
        UPPER(TRIM(is_active)) AS is_active,
        CASE WHEN COALESCE(TRIM(customer_id), '') = '' OR COALESCE(TRIM(full_name), '') = '' THEN 'failed'
             WHEN TRY_CAST(signup_date AS DATE) IS NULL
                  OR TRY_CAST(signup_date AS DATE) > CURRENT_DATE THEN 'failed'
             ELSE 'passed' END AS dq_status,
        CASE WHEN COALESCE(TRIM(customer_id), '') = '' OR COALESCE(TRIM(full_name), '') = '' THEN 'completeness: missing required field'
             WHEN TRY_CAST(signup_date AS DATE) IS NULL THEN 'validity: invalid signup_date'
             WHEN TRY_CAST(signup_date AS DATE) > CURRENT_DATE THEN 'timeliness: signup_date is in the future'
             ELSE '' END AS failure_reason
    FROM ranked WHERE _rn = 1
)
SELECT * FROM normalized;

CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC, rowid DESC) AS _rn
    FROM raw_products
)
SELECT
    TRIM(product_id) AS product_id, TRIM(product_name) AS product_name,
    TRIM(category) AS category, TRIM(sub_category) AS sub_category, TRIM(brand) AS brand,
    TRY_CAST(mrp AS DOUBLE) AS mrp, TRY_CAST(cost_price AS DOUBLE) AS cost_price,
    TRIM(unit) AS unit, LOWER(TRIM(is_perishable)) AS is_perishable,
    TRY_CAST(launch_date AS DATE) AS launch_date,
    CASE WHEN COALESCE(TRIM(product_id), '') = '' OR COALESCE(TRIM(product_name), '') = '' OR COALESCE(TRIM(category), '') = '' THEN 'failed'
         WHEN TRY_CAST(mrp AS DOUBLE) IS NULL OR TRY_CAST(mrp AS DOUBLE) <= 0
              OR TRY_CAST(cost_price AS DOUBLE) IS NULL OR TRY_CAST(cost_price AS DOUBLE) < 0
              OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE) THEN 'failed'
         ELSE 'passed' END AS dq_status,
    CASE WHEN COALESCE(TRIM(product_id), '') = '' OR COALESCE(TRIM(product_name), '') = '' OR COALESCE(TRIM(category), '') = '' THEN 'completeness: missing required field'
         WHEN TRY_CAST(mrp AS DOUBLE) IS NULL OR TRY_CAST(mrp AS DOUBLE) <= 0
              OR TRY_CAST(cost_price AS DOUBLE) IS NULL OR TRY_CAST(cost_price AS DOUBLE) < 0
              OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE) THEN 'accuracy: invalid product price'
         ELSE '' END AS failure_reason
FROM ranked WHERE _rn = 1;

CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY _loaded_at DESC, rowid DESC) AS _rn
    FROM raw_stores
)
SELECT
    TRIM(store_id) AS store_id, TRIM(store_name) AS store_name,
    UPPER(SUBSTR(TRIM(zone), 1, 1)) || LOWER(SUBSTR(TRIM(zone), 2)) AS zone,
    TRIM(area) AS area, TRIM(pincode) AS pincode, TRY_CAST(open_date AS DATE) AS open_date,
    CASE WHEN COALESCE(TRIM(manager_name), '') = '' THEN '' ELSE '***' END AS manager_name,
    TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    CASE WHEN COALESCE(TRIM(store_id), '') = '' OR COALESCE(TRIM(store_name), '') = '' OR COALESCE(TRIM(zone), '') = '' THEN 'failed'
         WHEN UPPER(SUBSTR(TRIM(zone), 1, 1)) || LOWER(SUBSTR(TRIM(zone), 2)) NOT IN ('North', 'South', 'East', 'West') THEN 'failed' ELSE 'passed' END AS dq_status,
    CASE WHEN COALESCE(TRIM(store_id), '') = '' OR COALESCE(TRIM(store_name), '') = '' OR COALESCE(TRIM(zone), '') = '' THEN 'completeness: missing required field'
         WHEN UPPER(SUBSTR(TRIM(zone), 1, 1)) || LOWER(SUBSTR(TRIM(zone), 2)) NOT IN ('North', 'South', 'East', 'West') THEN 'validity: invalid zone' ELSE '' END AS failure_reason
FROM ranked WHERE _rn = 1;

CREATE OR REPLACE TABLE stg_payments AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC, rowid DESC) AS _rn
    FROM raw_payments
)
SELECT
    TRIM(payment_id) AS payment_id, TRIM(order_id) AS order_id,
    TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
    CASE LOWER(TRIM(payment_mode))
        WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cash on delivery' THEN 'Cash on Delivery'
        WHEN 'cod' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode) END AS payment_mode,
    TRY_CAST(amount_paid AS DOUBLE) AS amount_paid, UPPER(TRIM(payment_status)) AS payment_status,
    TRIM(gateway_ref) AS gateway_ref,
    CASE WHEN COALESCE(TRIM(payment_id), '') = '' OR COALESCE(TRIM(order_id), '') = '' THEN 'failed'
         WHEN TRY_CAST(payment_ts AS TIMESTAMP) IS NULL OR TRY_CAST(amount_paid AS DOUBLE) IS NULL THEN 'failed'
         WHEN NOT EXISTS (SELECT 1 FROM raw_orders o WHERE o.order_id = ranked.order_id) THEN 'failed'
         WHEN LOWER(TRIM(payment_mode)) NOT IN ('upi', 'card', 'cash on delivery', 'cod', 'wallet') THEN 'failed'
         WHEN UPPER(TRIM(payment_status)) NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'failed'
         WHEN TRY_CAST(amount_paid AS DOUBLE) < 0 THEN 'failed'
         ELSE 'passed' END AS dq_status,
    CASE WHEN COALESCE(TRIM(payment_id), '') = '' OR COALESCE(TRIM(order_id), '') = '' THEN 'completeness: missing required field'
         WHEN TRY_CAST(payment_ts AS TIMESTAMP) IS NULL OR TRY_CAST(amount_paid AS DOUBLE) IS NULL THEN 'validity: invalid payment field'
         WHEN NOT EXISTS (SELECT 1 FROM raw_orders o WHERE o.order_id = ranked.order_id) THEN 'consistency: unknown order_id'
         WHEN LOWER(TRIM(payment_mode)) NOT IN ('upi', 'card', 'cash on delivery', 'cod', 'wallet') THEN 'validity: invalid payment_mode'
         WHEN UPPER(TRIM(payment_status)) NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'validity: invalid payment_status'
         WHEN TRY_CAST(amount_paid AS DOUBLE) < 0 THEN 'accuracy: negative amount_paid'
         ELSE '' END AS failure_reason
FROM ranked WHERE _rn = 1;

CREATE OR REPLACE TABLE stg_orders AS
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY CASE WHEN _source_file LIKE '%increment%' THEN 1 ELSE 0 END DESC,
                 _loaded_at DESC, rowid DESC
    ) AS _rn
    FROM raw_orders
), typed AS (
    SELECT
        TRIM(order_id) AS order_id, TRY_CAST(order_ts AS TIMESTAMP) AS order_ts,
        TRIM(customer_id) AS customer_id, TRIM(store_id) AS store_id, TRIM(product_id) AS product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity, TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct, TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        CASE LOWER(TRIM(payment_mode))
            WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode) END AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status, TRIM(channel) AS channel,
        TRY_CAST(delivery_minutes AS INTEGER) AS delivery_minutes
    FROM ranked WHERE _rn = 1
), checked AS (
    SELECT t.*,
        CASE
            WHEN COALESCE(order_id, '') = '' OR COALESCE(customer_id, '') = ''
                 OR COALESCE(store_id, '') = '' OR COALESCE(product_id, '') = '' THEN 'completeness: missing required field'
            WHEN order_ts IS NULL THEN 'validity: invalid order_ts'
            WHEN order_ts > CURRENT_TIMESTAMP THEN 'timeliness: future order_ts'
            WHEN COALESCE(order_status, '') NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'validity: invalid order_status'
            WHEN COALESCE(payment_mode, '') NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'validity: invalid payment_mode'
            WHEN quantity IS NULL OR quantity <= 0 OR discount_pct IS NULL OR discount_pct NOT BETWEEN 0 AND 100 THEN 'accuracy: invalid quantity or discount'
            WHEN unit_price IS NULL OR order_amount IS NULL
                 OR ABS(order_amount - ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)) > 0.01 THEN 'accuracy: order amount formula mismatch'
            WHEN order_status = 'DELIVERED' AND (delivery_minutes IS NULL OR delivery_minutes >= 120) THEN 'completeness/accuracy: invalid delivered duration'
            WHEN NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id = t.customer_id AND c.dq_status = 'passed') THEN 'consistency: unknown or invalid customer_id'
            WHEN NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id = t.product_id AND p.dq_status = 'passed') THEN 'consistency: unknown or invalid product_id'
            WHEN NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id = t.store_id AND s.dq_status = 'passed') THEN 'consistency: unknown or invalid store_id'
            ELSE '' END AS failure_reason
    FROM typed t
)
SELECT *, CASE WHEN failure_reason = '' THEN 'passed' ELSE 'failed' END AS dq_status
FROM checked;

INSERT INTO dq_results
SELECT '{{RUN_ID}}', '{{DAG_ID}}', check_name, dimension, severity, rows_checked, rows_failed,
       CASE WHEN rows_failed = 0 THEN 'passed'
            WHEN severity = 'warning' THEN 'warning' ELSE 'failed' END, CURRENT_TIMESTAMP
FROM (
    SELECT 'orders_completeness' check_name, 'completeness' dimension, 'critical' severity,
           COUNT(*) rows_checked, COUNT(*) FILTER (WHERE failure_reason LIKE 'completeness:%') rows_failed
      FROM stg_orders WHERE order_id IN (SELECT order_id FROM raw_orders WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'orders_uniqueness', 'uniqueness', 'critical', COUNT(*),
           COUNT(*) - COUNT(DISTINCT _source_file || '|' || order_id)
      FROM raw_orders WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'orders_validity', 'validity', 'critical', COUNT(*), COUNT(*) FILTER (WHERE failure_reason LIKE 'validity:%')
      FROM stg_orders WHERE order_id IN (SELECT order_id FROM raw_orders WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'orders_accuracy', 'accuracy', 'critical', COUNT(*), COUNT(*) FILTER (WHERE failure_reason LIKE 'accuracy:%')
      FROM stg_orders WHERE order_id IN (SELECT order_id FROM raw_orders WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'orders_consistency', 'consistency', 'critical', COUNT(*), COUNT(*) FILTER (WHERE failure_reason LIKE 'consistency:%')
      FROM stg_orders WHERE order_id IN (SELECT order_id FROM raw_orders WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'orders_timeliness', 'timeliness', 'critical', COUNT(*), COUNT(*) FILTER (WHERE failure_reason LIKE 'timeliness:%')
      FROM stg_orders WHERE order_id IN (SELECT order_id FROM raw_orders WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'customers_completeness', 'completeness', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'completeness:%')
      FROM stg_customers WHERE customer_id IN (SELECT customer_id FROM raw_customers WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'customers_uniqueness', 'uniqueness', 'critical', COUNT(*),
           COUNT(*) - COUNT(DISTINCT customer_id) FROM raw_customers WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'customers_validity', 'validity', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'validity:%')
      FROM stg_customers WHERE customer_id IN (SELECT customer_id FROM raw_customers WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'customers_timeliness', 'timeliness', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'timeliness:%')
      FROM stg_customers WHERE customer_id IN (SELECT customer_id FROM raw_customers WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'customers_email_format', 'validity', 'warning', COUNT(*), COUNT(*) FILTER (WHERE NOT REGEXP_MATCHES(email, '^[^@ ]+@[^@ ]+[.][^@ ]+$'))
      FROM raw_customers WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'customers_phone_format', 'validity', 'warning', COUNT(*),
           COUNT(*) FILTER (WHERE LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) <> 10)
      FROM raw_customers WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'products_completeness', 'completeness', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'completeness:%')
      FROM stg_products WHERE product_id IN (SELECT product_id FROM raw_products WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'products_uniqueness', 'uniqueness', 'critical', COUNT(*),
           COUNT(*) - COUNT(DISTINCT product_id) FROM raw_products WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'products_accuracy', 'accuracy', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'accuracy:%')
      FROM stg_products WHERE product_id IN (SELECT product_id FROM raw_products WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'stores_completeness', 'completeness', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'completeness:%')
      FROM stg_stores WHERE store_id IN (SELECT store_id FROM raw_stores WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'stores_validity', 'validity', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'validity:%')
      FROM stg_stores WHERE store_id IN (SELECT store_id FROM raw_stores WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'payments_uniqueness', 'uniqueness', 'critical', COUNT(*),
           COUNT(*) - COUNT(DISTINCT payment_id) FROM raw_payments WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'payments_completeness', 'completeness', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE COALESCE(TRIM(payment_id), '') = '' OR COALESCE(TRIM(order_id), '') = '')
      FROM raw_payments WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'payments_validity', 'validity', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE dq_status = 'failed' AND failure_reason LIKE 'validity:%')
      FROM stg_payments WHERE payment_id IN (SELECT payment_id FROM raw_payments WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'payments_accuracy', 'accuracy', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'accuracy:%')
      FROM stg_payments WHERE payment_id IN (SELECT payment_id FROM raw_payments WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'payments_consistency', 'consistency', 'critical', COUNT(*),
           COUNT(*) FILTER (WHERE failure_reason LIKE 'consistency:%')
      FROM stg_payments WHERE payment_id IN (SELECT payment_id FROM raw_payments WHERE _run_id = '{{RUN_ID}}')
    UNION ALL SELECT 'orders_freshness', 'timeliness', 'warning', COUNT(*),
           CASE WHEN MAX(TRY_CAST(order_ts AS TIMESTAMP)) < CURRENT_TIMESTAMP - INTERVAL '1 day' THEN 1 ELSE 0 END
      FROM raw_orders WHERE _run_id = '{{RUN_ID}}'
    UNION ALL SELECT 'orders_volume_anomaly', 'accuracy', 'warning', COUNT(*),
           CASE WHEN COUNT(*) < 242 THEN 1 ELSE 0 END FROM raw_orders
      WHERE _run_id = '{{RUN_ID}}' AND _source_file = 'orders.csv'
    UNION ALL SELECT 'orders_price_distribution', 'accuracy', 'warning', COUNT(*),
           CASE WHEN MAX(TRY_CAST(unit_price AS DOUBLE)) > MEDIAN(TRY_CAST(unit_price AS DOUBLE)) * 10
                THEN 1 ELSE 0 END FROM raw_orders WHERE _run_id = '{{RUN_ID}}'
);
