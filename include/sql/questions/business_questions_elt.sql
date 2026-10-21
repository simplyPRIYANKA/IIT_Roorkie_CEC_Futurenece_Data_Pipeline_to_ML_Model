-- include/sql/questions/business_questions_elt.sql
-- The six business questions answered from the ELT marts.

-- Q1a. Daily net revenue by zone
SELECT order_date, zone, SUM(delivered_orders) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY ALL ORDER BY order_date, zone;

-- Q1b. Daily net revenue by store
-- (store-days with only cancelled/returned orders have no delivered revenue, so they are left out)
SELECT order_date, store_id, store_name, zone, net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY order_date, store_id;

-- Q2. Revenue and gross margin by product category
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC;

-- Q3. Average delivery time per store and % within 15 minutes
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC;

-- Q4a. Revenue by payment mode
SELECT payment_mode, COUNT(*) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM stg_orders WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED'
GROUP BY ALL ORDER BY net_revenue DESC;

-- Q4b. Amount that does not reconcile with the payment gateway
SELECT mismatch_reason, COUNT(*) AS rows, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM mart_payment_reconciliation
GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason NULLS LAST;

-- Q5. Top 10 customers by spend, with loyalty tier
SELECT customer_id, customer_name_masked, email_masked, phone_masked, loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY spend_rank, customer_id LIMIT 10;

-- Q6. Rows rejected in the latest run, by file and by reason
-- (a row can break several rules, so it is counted once per reason)
SELECT source_file, reason, COUNT(*) AS rows
FROM (
    SELECT source_file, UNNEST(STRING_SPLIT(REPLACE(failure_reason, chr(59) || ' ', '|'), '|')) AS reason
    FROM elt_quarantine
    WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_priyanka'
                    ORDER BY start_time DESC LIMIT 1)
)
GROUP BY ALL ORDER BY source_file, rows DESC;
