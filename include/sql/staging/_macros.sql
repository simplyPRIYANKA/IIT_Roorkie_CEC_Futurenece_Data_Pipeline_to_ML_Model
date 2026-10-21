-- include/sql/staging/_macros.sql
-- Shared SQL building blocks, run before every staging / mart step.
-- These mirror include/nammamart/masking.py and config.py so ETL and ELT apply identical rules.

CREATE OR REPLACE MACRO to_num(x) AS try_cast(trim(x) AS DOUBLE);
CREATE OR REPLACE MACRO to_ts(x)  AS try_strptime(trim(x), '%Y-%m-%d %H:%M:%S');
CREATE OR REPLACE MACRO to_date(x) AS try_strptime(trim(x), '%Y-%m-%d')::DATE;
CREATE OR REPLACE MACRO capitalize(x) AS upper(left(x, 1)) || lower(substr(x, 2));

-- upi / 'UPI ' / cod / Cashh / CARD -> the 4 allowed payment modes (unknown values pass through)
CREATE OR REPLACE MACRO std_payment_mode(x) AS
    CASE lower(trim(x))
        WHEN 'upi'              THEN 'UPI'
        WHEN 'card'             THEN 'Card'
        WHEN 'wallet'           THEN 'Wallet'
        WHEN 'cash on delivery' THEN 'Cash on Delivery'
        WHEN 'cod'              THEN 'Cash on Delivery'
        WHEN 'cashh'            THEN 'Cash on Delivery'
        ELSE trim(x)
    END;

CREATE OR REPLACE MACRO is_valid_email(e) AS
    regexp_full_match(trim(e), '[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}');

-- PII masking: pr***@example.com, 97******21, "Priya H."
CREATE OR REPLACE MACRO mask_email(e) AS
    CASE WHEN is_valid_email(e)
         THEN left(trim(e), 2) || '***@' || split_part(trim(e), '@', 2) END;

CREATE OR REPLACE MACRO mask_phone(p) AS
    CASE WHEN length(trim(p)) = 0 THEN NULL
         WHEN length(trim(p)) <= 4 THEN repeat('*', length(trim(p)))
         ELSE left(trim(p), 2) || repeat('*', length(trim(p)) - 4) || right(trim(p), 2) END;

CREATE OR REPLACE MACRO mask_name(n) AS
    CASE WHEN trim(n) = '' THEN NULL
         WHEN len(string_split_regex(trim(n), '\s+')) = 1 THEN trim(n)
         ELSE string_split_regex(trim(n), '\s+')[1] || ' '
              || left(string_split_regex(trim(n), '\s+')[-1], 1) || '.' END;

-- dq_status from the two failure lists
CREATE OR REPLACE MACRO dq_status_of(errors, warnings) AS
    CASE WHEN len(errors) > 0 THEN 'FAIL' WHEN len(warnings) > 0 THEN 'WARN' ELSE 'PASS' END;

CREATE OR REPLACE MACRO failure_reason_of(errors, warnings) AS
    nullif(array_to_string(list_concat(errors, warnings), '; '), '');
