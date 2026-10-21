-- include/sql/raw/raw_payments.sql
-- RAW layer: exact copy of payments.csv.
-- Every column is TEXT, nothing is trimmed, cast or filtered. Rows are only ever appended.
-- Variables: run_id, source_file, source_path

CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id               VARCHAR,
    order_id                 VARCHAR,
    payment_ts               VARCHAR,
    payment_mode             VARCHAR,
    amount_paid              VARCHAR,
    payment_status           VARCHAR,
    gateway_ref              VARCHAR,
    _source_row              BIGINT,      -- 1-based data row number in the file (lineage)
    _source_file             VARCHAR,
    _loaded_at               TIMESTAMP,
    _run_id                  VARCHAR
);

-- Clearing and re-running this task inside the same Airflow run must not load the file twice.
DELETE FROM raw_payments
WHERE _run_id = getvariable('run_id') AND _source_file = getvariable('source_file');

-- BY NAME: a renamed or extra column in the CSV fails here loudly instead of loading wrong data.
INSERT INTO raw_payments BY NAME
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
