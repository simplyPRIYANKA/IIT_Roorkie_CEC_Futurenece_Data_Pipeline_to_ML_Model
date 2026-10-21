-- include/sql/raw/raw_orders.sql
-- RAW layer: exact copy of orders.csv and of every orders_*_increment.csv file.
-- Every column is TEXT, nothing is trimmed, cast or filtered. Rows are only ever appended.
-- Variables: run_id, source_file, source_path

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id                 VARCHAR,
    order_ts                 VARCHAR,
    customer_id              VARCHAR,
    store_id                 VARCHAR,
    product_id               VARCHAR,
    quantity                 VARCHAR,
    unit_price               VARCHAR,
    discount_pct             VARCHAR,
    order_amount             VARCHAR,
    payment_mode             VARCHAR,
    order_status             VARCHAR,
    channel                  VARCHAR,
    delivery_minutes         VARCHAR,
    _source_row              BIGINT,      -- 1-based data row number in the file (lineage)
    _source_file             VARCHAR,
    _loaded_at               TIMESTAMP,
    _run_id                  VARCHAR
);

-- Clearing and re-running this task inside the same Airflow run must not load the file twice.
DELETE FROM raw_orders
WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file');

-- BY NAME: a renamed or extra column in the CSV fails here loudly instead of loading wrong data.
INSERT INTO raw_orders BY NAME
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
