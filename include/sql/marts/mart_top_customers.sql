-- include/sql/marts/mart_top_customers.sql
-- Business Q5: customers ranked by delivered spend, with loyalty tier.
-- Only MASKED contact details are available here (no raw email / phone / name).

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT rank() OVER (ORDER BY sum(o.net_revenue) DESC)   AS spend_rank,
       c.customer_id,
       c.customer_name_masked,
       c.email_masked,
       c.phone_masked,
       c.loyalty_tier,
       count(*)                                         AS delivered_orders,
       round(sum(o.net_revenue), 2)                     AS total_spend
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAIL'
WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY spend_rank, c.customer_id;
