-- include/sql/quality/dq_results.sql
-- Scorecard: one row per rule in dq_check_catalog for this run, even when 0 rows failed.
-- Row-level failures come from the dq_errors / dq_warnings lists in staging;
-- dataset-level checks (freshness, volume) are computed here.
-- Variables: run_id, dag_id, as_of

DELETE FROM dq_results WHERE run_id = getvariable('run_id') AND dag_id = getvariable('dag_id');

INSERT INTO dq_results BY NAME
WITH row_flags AS (
    SELECT 'orders' AS file, unnest(list_concat(dq_errors, dq_warnings)) AS check_name
    FROM stg_orders_checked WHERE _run_id = getvariable('run_id')
    UNION ALL
    SELECT 'payments', unnest(list_concat(dq_errors, dq_warnings)) FROM stg_payments WHERE _run_id = getvariable('run_id')
    UNION ALL
    SELECT 'customers', unnest(list_concat(dq_errors, dq_warnings)) FROM stg_customers WHERE _run_id = getvariable('run_id')
    UNION ALL
    SELECT 'products', unnest(list_concat(dq_errors, dq_warnings)) FROM stg_products WHERE _run_id = getvariable('run_id')
    UNION ALL
    SELECT 'stores', unnest(list_concat(dq_errors, dq_warnings)) FROM stg_stores WHERE _run_id = getvariable('run_id')
),
row_failures AS (
    SELECT file, check_name, count(*) AS rows_failed FROM row_flags GROUP BY ALL
),
rows_checked AS (
    SELECT 'orders' AS file, count(*) AS n FROM raw_orders WHERE _run_id = getvariable('run_id')
    UNION ALL SELECT 'payments', count(*) FROM raw_payments WHERE _run_id = getvariable('run_id')
    UNION ALL SELECT 'customers', count(*) FROM raw_customers WHERE _run_id = getvariable('run_id')
    UNION ALL SELECT 'products', count(*) FROM raw_products WHERE _run_id = getvariable('run_id')
    UNION ALL SELECT 'stores', count(*) FROM raw_stores WHERE _run_id = getvariable('run_id')
),
latest_order AS (
    SELECT max(order_ts) AS ts FROM stg_orders_checked
    WHERE _run_id = getvariable('run_id') AND order_ts <= getvariable('as_of')::TIMESTAMP
),
previous_volume AS (
    SELECT rows_checked FROM dq_results
    WHERE dag_id = getvariable('dag_id') AND run_id <> getvariable('run_id')
      AND check_name = 'orders.volume_drop_vs_previous_run'
    ORDER BY checked_at DESC LIMIT 1
),
dataset_failures AS (
    -- Timeliness: newest order older than 24h at run time -> stale feed
    SELECT 'orders' AS file, 'stale_data' AS check_name,
           CASE WHEN ts IS NULL OR ts < getvariable('as_of')::TIMESTAMP - INTERVAL 24 HOUR THEN 1 ELSE 0 END AS failed
    FROM latest_order
    UNION ALL
    -- Volume: more than 90 % fewer order rows than the previous run
    SELECT 'orders', 'volume_drop_vs_previous_run',
           CASE WHEN (SELECT n FROM rows_checked WHERE file = 'orders')
                     < (SELECT rows_checked FROM previous_volume) * 0.10 THEN 1 ELSE 0 END
)
SELECT getvariable('run_id')                                    AS run_id,
       getvariable('dag_id')                                    AS dag_id,
       c.file || '.' || c.check_name                            AS check_name,
       c.dimension,
       c.severity,
       rc.n                                                     AS rows_checked,
       coalesce(CASE WHEN c.dataset_level THEN d.failed ELSE f.rows_failed END, 0) AS rows_failed,
       -- repeat the expression: the bare name rows_failed would bind to f.rows_failed (NULL when no failures)
       CASE WHEN coalesce(CASE WHEN c.dataset_level THEN d.failed ELSE f.rows_failed END, 0) = 0 THEN 'PASS'
            WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END AS status,
       now()::TIMESTAMP                                         AS checked_at
FROM dq_check_catalog c
JOIN rows_checked rc USING (file)
LEFT JOIN row_failures f USING (file, check_name)
LEFT JOIN dataset_failures d USING (file, check_name);
