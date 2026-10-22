CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(*) AS delivered_orders,
    SUM(o.order_amount) AS net_revenue,
    AVG(o.delivery_minutes) AS avg_delivery_minutes,
    100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) / NULLIF(COUNT(*), 0) AS within_15_min_pct
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'passed'
WHERE o.dq_status = 'passed' AND o.order_status = 'DELIVERED'
GROUP BY order_date, s.zone, s.store_id, s.store_name;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    SUM(o.order_amount) AS revenue,
    SUM(o.quantity * p.cost_price) AS cost_of_goods,
    SUM(o.order_amount - o.quantity * p.cost_price) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'passed'
WHERE o.dq_status = 'passed' AND o.order_status = 'DELIVERED'
GROUP BY p.category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id, s.store_name, s.zone,
    AVG(o.delivery_minutes) AS avg_delivery_minutes,
    100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) / NULLIF(COUNT(*), 0) AS within_15_min_pct,
    COUNT(*) AS delivered_orders
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'passed'
WHERE o.dq_status = 'passed' AND o.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT
    o.order_id, p.payment_id, o.order_amount, p.amount_paid, o.payment_mode AS order_payment_mode,
    p.payment_mode AS gateway_payment_mode, p.payment_status,
    CASE
        WHEN p.payment_id IS NULL THEN 'missing payment'
        WHEN p.payment_status <> 'SUCCESS' THEN 'payment not successful'
        WHEN ABS(o.order_amount - p.amount_paid) > 0.01 THEN 'amount mismatch'
        WHEN LOWER(o.payment_mode) <> LOWER(p.payment_mode) THEN 'payment mode mismatch'
    END AS reason
FROM stg_orders o
LEFT JOIN stg_payments p ON p.order_id = o.order_id
WHERE o.dq_status = 'passed' AND o.order_status = 'DELIVERED'
  AND (p.payment_id IS NULL OR p.payment_status <> 'SUCCESS'
       OR ABS(o.order_amount - p.amount_paid) > 0.01
       OR LOWER(o.payment_mode) <> LOWER(p.payment_mode))
UNION ALL
SELECT
    p.order_id, p.payment_id, NULL AS order_amount, p.amount_paid, NULL AS order_payment_mode,
    p.payment_mode AS gateway_payment_mode, p.payment_status, 'payment has no source order' AS reason
FROM stg_payments p
LEFT JOIN stg_orders o ON o.order_id = p.order_id
WHERE o.order_id IS NULL;

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    o.customer_id,
    c.loyalty_tier,
    SUM(o.order_amount) AS total_spend,
    COUNT(*) AS delivered_orders
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status = 'passed'
WHERE o.dq_status = 'passed' AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;

CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT o.payment_mode, SUM(o.order_amount) AS revenue
FROM stg_orders o
WHERE o.dq_status = 'passed' AND o.order_status = 'DELIVERED'
GROUP BY o.payment_mode;
