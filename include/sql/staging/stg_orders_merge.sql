-- include/sql/staging/stg_orders_merge.sql
-- STAGING: check one batch of orders (orders.csv or a next-day increment file) and
-- MERGE it into stg_orders on order_id.
--   * new order_id      -> INSERT
--   * existing order_id -> UPDATE (e.g. a late status change DELIVERED -> RETURNED)
-- Every checked row (including duplicates) is also kept in stg_orders_checked, which
-- feeds dq_results and the quarantine for this run.
-- Variables: run_id, source_file, as_of

CREATE OR REPLACE TEMP TABLE orders_batch AS
WITH src AS (
    SELECT r.*,
           row_number() OVER (PARTITION BY trim(order_id) ORDER BY _source_row) AS key_seq
    FROM raw_orders r
    WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file')
),
typed AS (
    SELECT trim(s.order_id)                    AS order_id,
           to_ts(s.order_ts)                   AS order_ts,
           trim(s.customer_id)                 AS customer_id,
           trim(s.store_id)                    AS store_id,
           trim(s.product_id)                  AS product_id,
           to_num(s.quantity)                  AS quantity,
           to_num(s.unit_price)                AS unit_price,
           to_num(s.discount_pct)              AS discount_pct,
           to_num(s.order_amount)              AS order_amount,
           s.payment_mode                      AS payment_mode_raw,
           std_payment_mode(s.payment_mode)    AS payment_mode,
           s.order_status                      AS order_status_raw,
           upper(trim(s.order_status))         AS order_status,
           trim(s.channel)                     AS channel,
           to_num(s.delivery_minutes)          AS delivery_minutes,
           p.mrp, p.cost_price,
           p.product_id IS NOT NULL            AS product_ok,
           st.store_id  IS NOT NULL            AS store_ok,
           c.customer_id IS NOT NULL           AS customer_ok,
           st.capacity_orders_per_day          AS store_capacity,
           -- orders this store took on this day, within this file (vs daily capacity)
           count(*) OVER (PARTITION BY trim(s.store_id), to_ts(s.order_ts)::DATE) AS store_day_orders,
           s.key_seq, s._source_file, s._source_row, s._run_id, s._loaded_at
    FROM src s
    -- consistency: keys must exist in the VALIDATED masters (quarantined master rows don't count)
    LEFT JOIN stg_products  p  ON p.product_id   = trim(s.product_id)  AND p.dq_status  <> 'FAIL'
    LEFT JOIN stg_stores    st ON st.store_id    = trim(s.store_id)    AND st.dq_status <> 'FAIL'
    LEFT JOIN stg_customers c  ON c.customer_id  = trim(s.customer_id) AND c.dq_status  <> 'FAIL'
),
checked AS (
    SELECT *,
        list_filter([
            CASE WHEN customer_id = '' THEN 'customer_id_missing' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'delivery_minutes_missing' END,
            CASE WHEN key_seq > 1 THEN 'duplicate_order_id' END,
            CASE WHEN order_ts IS NULL THEN 'order_ts_unparseable' END,
            CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'order_status_invalid' END,
            CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode_invalid' END,
            CASE WHEN discount_pct IS NULL OR discount_pct < 0 OR discount_pct > 100 THEN 'discount_pct_out_of_range' END,
            CASE WHEN quantity IS NULL OR quantity <= 0 THEN 'quantity_not_positive' END,
            CASE WHEN quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL
                   OR abs(quantity * unit_price * (1 - discount_pct / 100) - order_amount) > 0.011
                 THEN 'order_amount_formula_mismatch' END,
            CASE WHEN delivery_minutes >= 120 THEN 'delivery_minutes_over_120' END,
            CASE WHEN unit_price > mrp THEN 'unit_price_above_mrp' END,
            CASE WHEN customer_id <> '' AND NOT customer_ok THEN 'customer_id_not_in_master' END,
            CASE WHEN NOT product_ok THEN 'product_id_not_in_master' END,
            CASE WHEN NOT store_ok THEN 'store_id_not_in_master' END,
            CASE WHEN order_ts > getvariable('as_of')::TIMESTAMP THEN 'order_ts_in_future' END
        ], lambda x: x IS NOT NULL) AS dq_errors,
        list_filter([
            CASE WHEN order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND order_status_raw <> order_status
                 THEN 'order_status_nonstandard' END,
            CASE WHEN payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') AND payment_mode_raw <> payment_mode
                 THEN 'payment_mode_nonstandard' END,
            CASE WHEN delivery_minutes < 8 OR delivery_minutes > 35 THEN 'delivery_minutes_outside_normal' END,
            -- capacity <= 0 is already flagged by stores.capacity_not_positive, so it is skipped here
            CASE WHEN order_ts IS NOT NULL AND store_capacity > 0 AND store_day_orders > store_capacity
                 THEN 'store_over_capacity' END
        ], lambda x: x IS NOT NULL) AS dq_warnings
    FROM typed
)
SELECT order_id, order_ts, order_ts::DATE AS order_date,
       customer_id, store_id, product_id,
       quantity::INTEGER AS quantity, unit_price, discount_pct, order_amount,
       payment_mode, order_status, channel,
       delivery_minutes::INTEGER AS delivery_minutes,
       -- derived business columns (only DELIVERED orders count as revenue)
       CASE WHEN order_status = 'DELIVERED' THEN round(order_amount, 2) ELSE 0 END                         AS net_revenue,
       CASE WHEN order_status = 'DELIVERED' THEN round(order_amount - quantity * cost_price, 2) ELSE 0 END AS gross_margin,
       CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NOT NULL THEN delivery_minutes <= 15 END AS within_15_min,
       dq_status_of(dq_errors, dq_warnings)      AS dq_status,
       failure_reason_of(dq_errors, dq_warnings) AS failure_reason,
       dq_errors, dq_warnings, key_seq, _source_file, _source_row, _run_id, _loaded_at
FROM checked;

-- 1) Audit log of every checked order row in every batch.
CREATE TABLE IF NOT EXISTS stg_orders_checked AS SELECT * FROM orders_batch WHERE false;
DELETE FROM stg_orders_checked
WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file');
INSERT INTO stg_orders_checked SELECT * FROM orders_batch;

-- 2) Upsert the current version of each order. Duplicate copies (key_seq > 1) never merge.
CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * EXCLUDE (key_seq) FROM orders_batch WHERE false;

MERGE INTO stg_orders AS t
USING (SELECT * EXCLUDE (key_seq) FROM orders_batch WHERE key_seq = 1) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
RETURNING merge_action;
