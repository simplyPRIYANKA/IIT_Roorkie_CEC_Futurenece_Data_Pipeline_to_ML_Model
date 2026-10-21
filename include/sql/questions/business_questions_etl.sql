-- include/sql/questions/business_questions_etl.sql
-- The six business questions answered from the ETL star schema.
-- Run in DBeaver / SQLTools against include/warehouse_priyanka.duckdb (when no DAG is running).

-- Q1a. Daily net revenue by zone (delivered orders only, after discount)
SELECT order_date, zone, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone;

-- Q1b. Daily net revenue by store
SELECT f.order_date, s.store_id, s.store_name, s.zone,
       ROUND(SUM(f.net_revenue), 2) AS net_revenue
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY f.order_date, s.store_id;

-- Q2. Revenue and gross margin by product category
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC;

-- Q3. Average delivery time per store and % of delivered orders within 15 minutes
SELECT s.store_id, s.store_name,
       ROUND(AVG(f.delivery_minutes), 1)                         AS avg_delivery_minutes,
       ROUND(100.0 * AVG(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY pct_within_15_min DESC;

-- Q4a. Revenue by payment mode
SELECT payment_mode, COUNT(*) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY ALL ORDER BY net_revenue DESC;

-- Q4b. Amount that does not reconcile with the payment gateway
SELECT mismatch_reason, COUNT(*) AS rows, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM etl_payment_reconciliation
GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason NULLS LAST;

-- Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)
SELECT c.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       ROUND(SUM(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC, c.customer_id LIMIT 10;

-- Q6. Rows rejected in the latest run, by file and by reason
-- (a row can break several rules, so it is counted once per reason)
SELECT source_file, reason, COUNT(*) AS rows
FROM (
    SELECT source_file, UNNEST(STRING_SPLIT(REPLACE(failure_reason, chr(59) || ' ', '|'), '|')) AS reason
    FROM etl_quarantine
    WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_priyanka'
                    ORDER BY start_time DESC LIMIT 1)
)
GROUP BY ALL ORDER BY source_file, rows DESC;
