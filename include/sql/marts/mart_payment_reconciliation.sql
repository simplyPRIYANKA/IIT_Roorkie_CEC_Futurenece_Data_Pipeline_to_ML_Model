-- include/sql/marts/mart_payment_reconciliation.sql
-- Business Q4 / acceptance #7: one row per order/payment mismatch, with a reason.
--   expected  = order_amount for DELIVERED orders, 0 for CANCELLED / RETURNED
--   collected = amount_paid for SUCCESS payments, 0 for FAILED / REFUNDED
--   difference = collected - expected

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (
    SELECT order_id, order_status, order_amount, payment_mode
    FROM stg_orders WHERE dq_status <> 'FAIL'
),
p AS (
    SELECT payment_id, order_id, payment_status, amount_paid, payment_mode AS paid_mode
    FROM stg_payments WHERE dq_status <> 'FAIL'
),
joined AS (
    SELECT coalesce(o.order_id, p.order_id)  AS order_id,
           p.payment_id, o.order_status, p.payment_status, o.payment_mode, p.paid_mode,
           o.order_id IS NOT NULL            AS has_order,
           p.payment_id IS NOT NULL          AS has_payment,
           CASE WHEN o.order_id IS NULL THEN NULL
                WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END AS expected_amount,
           CASE WHEN p.payment_status = 'SUCCESS' THEN p.amount_paid ELSE 0 END  AS collected_amount
    FROM o FULL OUTER JOIN p ON o.order_id = p.order_id
),
classified AS (
    SELECT *,
           round(collected_amount - coalesce(expected_amount, 0), 2) AS difference,
           CASE
               WHEN NOT has_order   THEN 'payment_for_rejected_order'
               WHEN NOT has_payment THEN CASE WHEN order_status = 'DELIVERED' THEN 'no_payment_record' END
               WHEN order_status = 'DELIVERED' AND payment_status = 'FAILED'   THEN 'payment_failed'
               WHEN order_status = 'DELIVERED' AND payment_status = 'REFUNDED' THEN 'refunded_but_delivered'
               WHEN order_status = 'RETURNED'  AND payment_status = 'SUCCESS'  THEN 'returned_not_refunded'
               WHEN order_status = 'CANCELLED' AND payment_status = 'SUCCESS'  THEN 'cancelled_but_charged'
               WHEN abs(round(collected_amount - coalesce(expected_amount, 0), 2)) > 0.011 THEN 'amount_mismatch'
               WHEN payment_mode <> paid_mode  THEN 'payment_mode_mismatch'
           END AS mismatch_reason
    FROM joined
)
SELECT order_id, payment_id, order_status, payment_status, payment_mode, paid_mode,
       round(expected_amount, 2)  AS expected_amount,
       round(collected_amount, 2) AS collected_amount,
       difference, mismatch_reason
FROM classified
WHERE mismatch_reason IS NOT NULL
ORDER BY order_id;
