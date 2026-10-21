-- include/sql/staging/stg_customers.sql
-- STAGING: customer master with PII MASKED. Raw email / phone / full_name never leave
-- the raw layer: staging and marts only ever see the masked versions.
-- Variables: run_id, as_of

CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT r.*,
           row_number() OVER (PARTITION BY trim(customer_id) ORDER BY _source_row) AS key_seq
    FROM raw_customers r
    WHERE _run_id = getvariable('run_id')
),
typed AS (
    SELECT trim(customer_id)                  AS customer_id,
           mask_name(full_name)               AS customer_name_masked,
           mask_email(email)                  AS email_masked,
           mask_phone(phone)                  AS phone_masked,
           email, phone,                      -- used for checks only, dropped below
           trim(city)                         AS city,
           trim(area)                         AS area,
           trim(pincode)                      AS pincode,
           to_date(signup_date)               AS signup_date,
           loyalty_tier                       AS tier_raw,
           capitalize(trim(loyalty_tier))     AS tier_std,
           upper(trim(is_active)) = 'Y'       AS is_active,
           key_seq, _source_file, _source_row, _run_id
    FROM src
),
checked AS (
    SELECT *,
        list_filter([
            CASE WHEN customer_id = '' THEN 'customer_id_missing' END,
            CASE WHEN key_seq > 1 THEN 'duplicate_customer_id' END,
            CASE WHEN signup_date IS NULL THEN 'signup_date_unparseable' END,
            CASE WHEN tier_std NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'loyalty_tier_invalid' END,
            CASE WHEN signup_date > getvariable('as_of')::TIMESTAMP::DATE THEN 'signup_date_in_future' END
        ], lambda x: x IS NOT NULL) AS dq_errors,
        list_filter([
            CASE WHEN city = '' THEN 'city_missing' END,
            CASE WHEN NOT is_valid_email(email) THEN 'email_invalid' END,
            CASE WHEN NOT regexp_full_match(trim(phone), '[6-9][0-9]{9}') THEN 'phone_invalid' END,
            CASE WHEN NOT regexp_full_match(pincode, '[0-9]{6}') THEN 'pincode_invalid' END,
            CASE WHEN tier_std IN ('Bronze', 'Silver', 'Gold', 'Platinum') AND tier_raw <> tier_std
                 THEN 'loyalty_tier_nonstandard_case' END
        ], lambda x: x IS NOT NULL) AS dq_warnings
    FROM typed
)
SELECT customer_id, customer_name_masked, email_masked, phone_masked,
       nullif(city, '')                                          AS city,
       area,
       CASE WHEN regexp_full_match(pincode, '[0-9]{6}') THEN pincode END AS pincode,
       signup_date,
       tier_std                                                  AS loyalty_tier,
       is_active,
       dq_status_of(dq_errors, dq_warnings)                      AS dq_status,
       failure_reason_of(dq_errors, dq_warnings)                 AS failure_reason,
       dq_errors, dq_warnings, _source_file, _source_row, _run_id
FROM checked;
