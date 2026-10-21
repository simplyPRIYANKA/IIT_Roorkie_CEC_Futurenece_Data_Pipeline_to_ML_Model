-- include/sql/raw/raw_products.sql
-- RAW layer: exact copy of products.csv.
-- Every column is TEXT, nothing is trimmed, cast or filtered. Rows are only ever appended.
-- Variables: run_id, source_file, source_path

CREATE TABLE IF NOT EXISTS raw_products (
    product_id               VARCHAR,
    product_name             VARCHAR,
    category                 VARCHAR,
    sub_category             VARCHAR,
    brand                    VARCHAR,
    mrp                      VARCHAR,
    cost_price               VARCHAR,
    unit                     VARCHAR,
    is_perishable            VARCHAR,
    launch_date              VARCHAR,
    _source_row              BIGINT,      -- 1-based data row number in the file (lineage)
    _source_file             VARCHAR,
    _loaded_at               TIMESTAMP,
    _run_id                  VARCHAR
);

-- Clearing and re-running this task inside the same Airflow run must not load the file twice.
DELETE FROM raw_products
WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file');

-- BY NAME: a renamed or extra column in the CSV fails here loudly instead of loading wrong data.
INSERT INTO raw_products BY NAME
SELECT *,
       row_number() OVER ()           AS _source_row,
       getvariable('source_file')     AS _source_file,
       now()::TIMESTAMP               AS _loaded_at,
       getvariable('run_id')          AS _run_id
FROM read_csv(getvariable('source_path'),
              header = true, all_varchar = true, parallel = false,
              nullstr = '__no_null__',    -- keep empty strings as received
              delim = ',', quote = '"', escape = '"',
              strict_mode = false);       -- tolerate mixed CRLF/LF line endings;
                                          -- load_raw() verifies the row count instead
