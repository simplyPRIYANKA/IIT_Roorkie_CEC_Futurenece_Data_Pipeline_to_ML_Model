-- include/sql/staging/stg_products.sql
-- STAGING: typed product catalogue with data quality flags. First row of a duplicate
-- product_id is kept, later copies (e.g. "(old)" versions) are flagged duplicate.
-- Variables: run_id

CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT r.*,
           row_number() OVER (PARTITION BY trim(product_id) ORDER BY _source_row) AS key_seq
    FROM raw_products r
    WHERE _run_id = getvariable('run_id')
),
typed AS (
    SELECT trim(product_id)        AS product_id,
           trim(product_name)      AS product_name,
           trim(category)          AS category_raw,
           trim(sub_category)      AS sub_category,
           trim(brand)             AS brand,
           to_num(mrp)             AS mrp,
           to_num(cost_price)      AS cost_price,
           trim(unit)              AS unit,
           lower(trim(is_perishable)) = 'true' AS is_perishable,
           to_date(launch_date)    AS launch_date,
           key_seq, _source_file, _source_row, _run_id
    FROM src
),
checked AS (
    SELECT *,
        list_filter([
            CASE WHEN key_seq > 1 THEN 'duplicate_product_id' END,
            CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'mrp_not_positive' END,
            CASE WHEN cost_price > mrp THEN 'cost_price_above_mrp' END
        ], lambda x: x IS NOT NULL) AS dq_errors,
        list_filter([
            CASE WHEN category_raw = '' THEN 'category_missing' END
        ], lambda x: x IS NOT NULL) AS dq_warnings
    FROM typed
)
SELECT product_id, product_name,
       coalesce(nullif(category_raw, ''), 'Uncategorised') AS category,
       sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date,
       dq_status_of(dq_errors, dq_warnings)      AS dq_status,
       failure_reason_of(dq_errors, dq_warnings) AS failure_reason,
       dq_errors, dq_warnings, _source_file, _source_row, _run_id
FROM checked;
