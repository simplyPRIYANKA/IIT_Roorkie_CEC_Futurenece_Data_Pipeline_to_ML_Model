-- include/sql/staging/stg_stores.sql
-- STAGING: typed, standardised store master with data quality flags.
-- Source: the raw_stores rows loaded in this run (the store master is a full snapshot).
-- Variables: run_id

CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT r.*,
           row_number() OVER (PARTITION BY trim(store_id) ORDER BY _source_row) AS key_seq
    FROM raw_stores r
    WHERE _run_id = getvariable('run_id')
),
typed AS (
    SELECT trim(store_id)                         AS store_id,
           trim(store_name)                       AS store_name,
           capitalize(trim(zone))                 AS zone_std,
           trim(area)                             AS area,
           trim(pincode)                          AS pincode,
           to_date(open_date)                     AS open_date,
           trim(manager_name)                     AS manager_name,
           to_num(capacity_orders_per_day)        AS capacity,
           key_seq, _source_file, _source_row, _run_id
    FROM src
),
checked AS (
    SELECT *,
        list_filter([
            CASE WHEN key_seq > 1 THEN 'duplicate_store_id' END,
            CASE WHEN zone_std <> '' AND zone_std NOT IN ('North', 'South', 'East', 'West') THEN 'zone_invalid' END
        ], lambda x: x IS NOT NULL) AS dq_errors,
        list_filter([
            CASE WHEN zone_std = '' THEN 'zone_missing' END,
            CASE WHEN capacity IS NULL OR capacity <= 0 THEN 'capacity_not_positive' END
        ], lambda x: x IS NOT NULL) AS dq_warnings
    FROM typed
)
SELECT store_id, store_name,
       coalesce(nullif(zone_std, ''), 'Unknown')     AS zone,
       area, pincode, open_date, manager_name,
       capacity::INTEGER                            AS capacity_orders_per_day,
       dq_status_of(dq_errors, dq_warnings)         AS dq_status,
       failure_reason_of(dq_errors, dq_warnings)    AS failure_reason,
       dq_errors, dq_warnings, _source_file, _source_row, _run_id
FROM checked;
