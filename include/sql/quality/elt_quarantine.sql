-- include/sql/quality/elt_quarantine.sql
-- Quarantine: every row that failed a Critical check in this run, with its failure_reason
-- and the raw record exactly as received (joined back to the raw layer by file + row number).
-- Customer PII is masked in raw_record; the unmasked original stays only in raw_customers.
-- Variables: run_id, dag_id

DELETE FROM elt_quarantine WHERE run_id = getvariable('run_id') AND dag_id = getvariable('dag_id');

INSERT INTO elt_quarantine BY NAME
WITH failed AS (
    SELECT 'orders' AS file, order_id AS record_key, failure_reason, _source_file, _source_row
    FROM stg_orders_checked WHERE _run_id = getvariable('run_id') AND dq_status = 'FAIL'
    UNION ALL
    SELECT 'payments', payment_id, failure_reason, _source_file, _source_row
    FROM stg_payments WHERE _run_id = getvariable('run_id') AND dq_status = 'FAIL'
    UNION ALL
    SELECT 'customers', customer_id, failure_reason, _source_file, _source_row
    FROM stg_customers WHERE _run_id = getvariable('run_id') AND dq_status = 'FAIL'
    UNION ALL
    SELECT 'products', product_id, failure_reason, _source_file, _source_row
    FROM stg_products WHERE _run_id = getvariable('run_id') AND dq_status = 'FAIL'
    UNION ALL
    SELECT 'stores', store_id, failure_reason, _source_file, _source_row
    FROM stg_stores WHERE _run_id = getvariable('run_id') AND dq_status = 'FAIL'
),
raw_json AS (
    SELECT _source_file, _source_row,
           to_json(r)::VARCHAR AS raw_record
    FROM (SELECT * EXCLUDE (_loaded_at, _run_id) FROM raw_orders WHERE _run_id = getvariable('run_id')) r
    UNION ALL
    SELECT _source_file, _source_row, to_json(r)::VARCHAR
    FROM (SELECT * EXCLUDE (_loaded_at, _run_id) FROM raw_payments WHERE _run_id = getvariable('run_id')) r
    UNION ALL
    SELECT _source_file, _source_row, to_json(r)::VARCHAR
    FROM (SELECT * EXCLUDE (_loaded_at, _run_id)
                   REPLACE (coalesce(mask_email(email), '***') AS email,
                            mask_phone(phone) AS phone,
                            mask_name(full_name) AS full_name)
          FROM raw_customers WHERE _run_id = getvariable('run_id')) r
    UNION ALL
    SELECT _source_file, _source_row, to_json(r)::VARCHAR
    FROM (SELECT * EXCLUDE (_loaded_at, _run_id) FROM raw_products WHERE _run_id = getvariable('run_id')) r
    UNION ALL
    SELECT _source_file, _source_row, to_json(r)::VARCHAR
    FROM (SELECT * EXCLUDE (_loaded_at, _run_id) FROM raw_stores WHERE _run_id = getvariable('run_id')) r
)
SELECT getvariable('run_id')  AS run_id,
       getvariable('dag_id')  AS dag_id,
       f._source_file         AS source_file,
       f._source_row          AS source_row,
       f.record_key,
       f.failure_reason,
       j.raw_record,
       now()::TIMESTAMP       AS quarantined_at
FROM failed f
JOIN raw_json j USING (_source_file, _source_row);
