-- include/sql/marts/mart_category_margin.sql
-- Business Q2: revenue and gross margin by product category (delivered orders only).
-- gross margin = revenue - quantity x cost_price

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       count(*)                                                AS delivered_orders,
       sum(o.quantity)                                         AS units,
       round(sum(o.net_revenue), 2)                            AS net_revenue,
       round(sum(o.quantity * p.cost_price), 2)                AS cost_of_goods,
       round(sum(o.gross_margin), 2)                           AS gross_margin,
       round(100 * sum(o.gross_margin) / nullif(sum(o.net_revenue), 0), 1) AS margin_pct
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAIL'
WHERE o.dq_status <> 'FAIL' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY net_revenue DESC;
