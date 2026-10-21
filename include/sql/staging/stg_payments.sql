-- include/sql/staging/stg_payments.sql
-- STAGING: payment gateway export with data quality flags.
-- order_id_unknown = the payment refers to an order that is in none of this run's order files.
-- Variables: run_id, as_of

CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
    SELECT r.*,
           row_number() OVER (PARTITION BY trim(payment_id) ORDER BY _source_row) AS key_seq
    FROM raw_payments r
    WHERE _run_id = getvariable('run_id')
),
known_orders AS (
    SELECT DISTINCT trim(order_id) AS order_id FROM raw_orders WHERE _run_id = getvariable('run_id')
),
typed AS (
    SELECT trim(s.payment_id)                  AS payment_id,
           trim(s.order_id)                    AS order_id,
           to_ts(s.payment_ts)                 AS payment_ts,
           s.payment_mode                      AS payment_mode_raw,
           std_payment_mode(s.payment_mode)    AS payment_mode,
           to_num(s.amount_paid)               AS amount_paid,
           upper(trim(s.payment_status))       AS payment_status,
           nullif(trim(s.gateway_ref), '')     AS gateway_ref,
           k.order_id IS NOT NULL              AS order_known,
           s.key_seq, s._source_file, s._source_row, s._run_id
    FROM src s
    LEFT JOIN known_orders k ON k.order_id = trim(s.order_id)
),
checked AS (
    SELECT *,
        list_filter([
            CASE WHEN key_seq > 1 THEN 'duplicate_payment_id' END,
            CASE WHEN payment_ts IS NULL THEN 'payment_ts_unparseable' END,
            CASE WHEN payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'payment_status_invalid' END,
            CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'payment_mode_invalid' END,
            CASE WHEN amount_paid IS NULL OR amount_paid <= 0 THEN 'amount_paid_not_positive' END,
            CASE WHEN NOT order_known THEN 'order_id_unknown' END,
            CASE WHEN payment_ts > getvariable('as_of')::TIMESTAMP THEN 'payment_ts_in_future' END
        ], lambda x: x IS NOT NULL) AS dq_errors,
        list_filter([
            CASE WHEN payment_mode <> 'Cash on Delivery' AND gateway_ref IS NULL THEN 'gateway_ref_missing' END,
            CASE WHEN payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') AND payment_mode_raw <> payment_mode
                 THEN 'payment_mode_nonstandard' END
        ], lambda x: x IS NOT NULL) AS dq_warnings
    FROM typed
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
       dq_status_of(dq_errors, dq_warnings)      AS dq_status,
       failure_reason_of(dq_errors, dq_warnings) AS failure_reason,
       dq_errors, dq_warnings, _source_file, _source_row, _run_id
FROM checked;
