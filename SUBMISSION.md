# NammaMart ETL & ELT: Submission (Priyanka)

FDAIE Module 2 · Data Engineering Foundations

| Deliverable | Where |
|---|---|
| ETL DAG | [dags/nammamart_etl_priyanka.py](logic in [include/nammamart/etl_core.py]) |
| ELT DAG | [dags/nammamart_elt_priyanka.py](SQL in [include/sql/]) |
| Shared quality rules (48 checks, 6 dimensions) | [include/nammamart/dq_rules.py] |
| PII masking | [include/nammamart/masking.py] and `mask_*` macros in [include/sql/staging/_macros.sql] |
| Profiling script + output | [include/profiling/profile_nammamart.py], [docs/profiling_output.txt] |
| Business-question SQL | [include/sql/questions/] |
| Quarantine files | [include/quarantine/priyanka/] |
| Warehouse | `include/warehouse_priyanka.duckdb` |
| Screenshots | [docs/screenshots/] |
| Business questions (SQL + result per pipeline) | [docs/business_questions_results.md] |
| Store-capacity check and DQ trend chart | [include/reports/dq_trend.svg] |

**Headline result.** Both pipelines produce identical numbers on the same data:

| | Net revenue | Delivered orders | Gross margin |
|---|---|---|---|
| September only (Sept files) | ₹11,00,259.60 | 2,008 | ₹2,64,855.09 |
| After the 1 Oct increment | ₹11,70,988.20 | 2,137 | ₹2,82,874.32 |

Both pipelines also agree on every one of the 48 `dq_results` checks, on every quarantined row (232 → 243 after the increment) and on every reconciliation row (235 → 384).

---

## Part 0: Discovery and data profiling


[include/profiling/profile_nammamart.py] reads each file. For every file it reports row counts, blanks, duplicate keys, min/max of numeric and date columns, distinct category values, referential integrity across the four relationships, and every business rule in the brief. The printed output masks PII.

| File | Rows | Distinct keys | Notes |
|---|---|---|---|
| orders.csv | 2,412 | 2,400 | `order_ts` from 2026-09-01 to **2027-01-01**; quantity −3 to 5; discount 0 to **250** |
| payments.csv | 2,228 | 2,224 | SUCCESS 2,116 / REFUNDED 68 / FAILED 44 |
| customers.csv | 405 | 400 | signup up to **2027-01-05**; 7 casing variants of `loyalty_tier` |
| products.csv | 152 | 150 | MRP down to **−203** |
| stores.csv | 15 | 15 | one blank zone, one capacity of 0 |
| orders_2026-10-01_increment.csv | 160 | 160 | clean: 150 new orders + 10 corrections (DELIVERED → RETURNED) |

### Data Issues Log

| # | File | Row key(s) | Column | Problem | Dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|---|
| 1 | orders | ORD000539, ORD000783, ORD001110 … (12 keys) | all | Exact duplicate rows | Uniqueness | `order_id` unique per file; keep first copy, quarantine the rest | Critical |
| 2 | orders | ORD002255, ORD000701 … (18) | customer_id | Blank | Completeness | `customer_id` not blank | Critical |
| 3 | orders | ORD000986 (P9455) … (9) | product_id | Not in product master | Consistency | `product_id` exists in validated master | Critical |
| 4 | orders | ORD001799 (S99), ORD001046 (S17), ORD002126 (S00) … (5) | store_id | Not in store master | Consistency | `store_id` exists in validated master | Critical |
| 5 | orders | ORD001296 (−2), ORD000469 (0) … (10) | quantity | Zero or negative | Accuracy | `quantity > 0` | Critical |
| 6 | orders | ORD002103 (250), ORD001468 (120), ORD001237 (150) | discount_pct | Over 100% | Validity | 0 ≤ `discount_pct` ≤ 100 | Critical |
| 7 | orders | ORD002305 (6757.5 vs 675.75), ORD001274 (110 vs 220) … (18) | order_amount | ≠ qty × price × (1 − disc/100): ×10, ×½, ×1.5 errors | Accuracy | Formula match within ₹0.01 | Critical |
| 8 | orders | ORD000500, ORD001970, ORD001637 | order_status | `Pending??` | Validity | Status in DELIVERED / CANCELLED / RETURNED | Critical |
| 9 | orders | ORD001066, ORD000153 | order_status | `delivered` (lower case) | Validity | Upper-case and trim, then log | Warning |
| 10 | orders | ORD000159 (`upi`), ORD001812 (`Cashh`), ORD001640 (`cod`), ORD001364 (`CARD`), ORD002356 (`UPI `) … (10) | payment_mode | Non-standard spelling | Validity | Map to UPI / Card / Cash on Delivery / Wallet, then log | Warning |
| 11 | orders | ORD001554, ORD001800 … (8) | delivery_minutes | DELIVERED but blank | Completeness | DELIVERED ⇒ value present | Critical |
| 12 | orders | ORD000455 (1440), ORD001104 (900), ORD001037 (480) … (6) | delivery_minutes | Hours instead of minutes | Accuracy | `< 120` (outside 8–35 is a Warning) | Critical |
| 13 | orders | ORD000403 (`N/A`), ORD000362 (blank), ORD001610 (`2026-13-05`), ORD002012 (`31/09/2026`) (6) | order_ts | Unparseable | Validity | Strict `YYYY-MM-DD HH:MM:SS` | Critical |
| 14 | orders | ORD002119 (2027-01-01), ORD001121 (2026-11-02), ORD001693 (2026-10-21) (7) | order_ts | In the future | Timeliness | `order_ts ≤ as-of time` | Critical |
| 15 | orders | P0109 orders (11) | unit_price | Above product MRP | Accuracy | `unit_price ≤ mrp` (selling above MRP is illegal) | Critical |
| 16 | payments | PAY0000509, PAY0000755, PAY0001651, PAY0002000 | all | Exact duplicate rows | Uniqueness | `payment_id` unique | Critical |
| 17 | payments | PAY0002217–PAY0002224 (ORD9xxxxx) | order_id | Payment for an order that does not exist | Consistency | `order_id` exists in orders | Critical |
| 18 | payments | PAY0002175 (−188.70), PAY0000116 (0.00) … (10) | amount_paid | Zero or negative | Accuracy | `amount_paid > 0` | Critical |
| 19 | payments | PAY0000127 (2027-01-01) … (7) | payment_ts | In the future | Timeliness | `payment_ts ≤ as-of time` | Critical |
| 20 | payments | PAY0000354 (455 vs 505), PAY0000909 (103 vs 203) … (14) | amount_paid | SUCCESS ≠ order amount (short by ₹25/50/100) | Consistency | Reported in reconciliation, not quarantined | Reconcile |
| 21 | payments | 44 DELIVERED orders | payment_status | Only payment is FAILED | Consistency | Reconciliation `payment_failed` | Reconcile |
| 22 | payments | `upi`, `cod`, `Cashh`, `CARD` (10) | payment_mode | Non-standard | Validity | Map, then log | Warning |
| 23 | payments | 301 rows | gateway_ref | Blank, but all are Cash on Delivery | Completeness | Only non-cash needs a ref (**not an issue**) | Warning |
| 24 | customers | C00036, C00103, C00226, C00252, C00313 | customer_id | Duplicated with a different phone / area | Uniqueness | Keep first, quarantine the rest; ask CRM which is current | Critical |
| 25 | customers | C00337, C00045 (`N/A`); C00307, C00063 (no `@` or TLD) … (12) | email | Invalid format | Validity | `name@domain.tld` | Warning |
| 26 | customers | C00151, C00290 … (7) | phone | Not 10 digits | Validity | `^[6-9][0-9]{9}$` | Warning |
| 27 | customers | C00132, C00083, C00166 | pincode | `5600XX` | Validity | 6 digits | Warning |
| 28 | customers | C00400 (2027-01-05), C00398, C00038 | signup_date | In the future | Timeliness | `signup_date ≤ as-of date` | Critical |
| 29 | customers | 8 rows | loyalty_tier | `BRONZE`, `bronze`, `SILVER` | Consistency | Capitalise, then log | Warning |
| 30 | customers | 6 rows | city | Blank | Completeness | Log, keep | Warning |
| 31 | products | P0030, P0140 | product_id | Duplicate "(old)" version with a lower MRP | Uniqueness | Keep first (current) row | Critical |
| 32 | products | P0051, P0060, P0068 | category | Blank | Completeness | Load as `Uncategorised`, log | Warning |
| 33 | products | P0109 | mrp | −203 | Accuracy | `mrp > 0` | Critical |
| 34 | products | P0019, P0073, P0074, P0117 (+P0109) | cost_price | = 1.2 × MRP (loss on every sale) | Accuracy | `cost_price ≤ mrp` | Critical |
| 35 | stores | S07 (BTM Layout) | zone | Blank | Completeness | Load as `Unknown`, log | Warning |
| 36 | stores | S12 (Rajajinagar) | capacity_orders_per_day | 0 | Accuracy | `> 0` | Warning |

**Severity principle.** A value that changes a money number (amount, quantity, cost, a key) is **Critical**. 
						A missing or ugly *label* (zone, category, casing) is a **Warning**.
						Quarantining store S07 for a blank zone would have removed 176 genuine orders from revenue, so the label is filled with `Unknown` and the problem is logged instead.

**Cascade rule.** An order whose product, store or customer failed a Critical master-data check is also quarantined (`*_not_in_master`). In the final run this blocks **104 orders worth ₹45,134.20** that are otherwise valid. Fixing 8 product rows and 3 customer rows in the source systems recovers them.

### Questions I would ask the client before building
1. **Store S07 has no zone. Its pincode 560076 (BTM Layout) suggests South. Can ops confirm?** Until then its ₹81,912.90 appears under `Unknown` in the zone report.
2. **Is a negative quantity a return or a data-entry error?** I treat it as an error, because returns arrive as status RETURNED. If ops use negative quantities for returns, the rule changes to "map to RETURNED".
3. **When a customer appears twice with different phone numbers (C00036 and four others), which record is current?** The CRM export has no `updated_at`, so I keep the first row. Can the CRM add a last-modified timestamp?
4. **Do `cod` and `Cashh` both mean Cash on Delivery?** I mapped them that way and log each mapping as a Warning.
5. **Should finance's September revenue change when a September order is returned in October?** See Part E, question 9.

---

## Part A: ETL pipeline (`nammamart_etl_priyanka`)

`extract_*` (×5, parallel) → `validate_*` (stores, products and customers in parallel, then orders and payments) → `quarantine` → `transform` → `load` → `reconcile` → `report` → `audit` (`trigger_rule="all_done"`).

| Requirement | How it is met |
|---|---|
| Parallel extract | 5 tasks start within 0.1 s of each other (Experiment 1). `extract_orders` also picks up any `orders_*_increment.csv`. Each extract checks the CSV header and fails loudly on a schema change. |
| Validate | 48 rules from `dq_rules.py`, each with a dimension and a severity. Values are standardised **before** the checks, so a casing fix does not quarantine a good row. |
| Quarantine | `include/quarantine/priyanka/<file>_failed_<run_id>.csv` with a `failure_reason` column, plus an `etl_quarantine` table. In the file name, the `:` and `+` characters of the run_id are replaced with `_`, because they are not valid in file names on the Windows-mounted folder (e.g. `orders_failed_manual_2026_10_21T18_01_07_636508_00_00.csv`). PII is masked in quarantine; `_source_file` and `_source_row` point to the untouched original. |
| Transform | Trim and standardise; deduplicate customers and products (first row wins); mask email, phone and name; join orders with customers, products and stores (`pd.merge` with `validate="many_to_one"`, so a duplicated dimension key fails loudly, and a check that every order matched all three); derive `order_date`, `net_revenue`, `gross_margin` (using `cost_price` from the product join) and `within_15_min`. When an order appears in an increment file, the latest version wins. |
| Load (star schema) | `etl_fact_orders` (+ `etl_fact_payments`) and `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`. **Idempotent:** dimensions are replaced, and the fact table is an upsert (delete this batch's `order_id`s, then insert) inside one transaction. |
| Reconcile | `etl_payment_reconciliation`: one row per mismatch, with `expected_amount`, `collected_amount`, `difference` and `mismatch_reason`. |
| Report | `etl_daily_zone_revenue` and `etl_category_margin`, printed in the task log. |

## Part B: ELT pipeline (`nammamart_elt_priyanka`)

| Layer | Tables | Implementation |
|---|---|---|
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | [include/sql/raw/]: `read_csv(all_varchar=true)`, nothing trimmed or cast, plus `_source_row`, `_source_file`, `_loaded_at` and `_run_id`. Append-only. `INSERT … BY NAME` combined with a header check means a renamed column fails loudly. A row-count check guards against parser problems. |
| Staging | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` (+ `stg_orders_checked` audit log) | [include/sql/staging/]: casting, standardising, deduplication, PII masking, the same 48 rules, `dq_status` (PASS / WARN / FAIL) and `failure_reason`. |
| Incremental | `stg_orders` | [stg_orders_merge.sql]runs once per order file: `MERGE INTO stg_orders … ON order_id WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT RETURNING merge_action`. The increment file gave **150 INSERT + 10 UPDATE**, with no duplicates. |
| Quality | `dq_results`, `elt_quarantine` | [include/sql/quality/] |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` | [include/sql/marts/]: built only from rows with `dq_status <> 'FAIL'`. |

**Do the ELT totals match ETL?** Yes, exactly. The final `compare_with_etl` task logs `net revenue ETL=1,170,988.20 ELT=1,170,988.20 match=True` (and the same for delivered orders and gross margin). They match because both pipelines apply the same rule catalogue in the same order: standardise → check → latest version wins → only non-FAIL rows count.

The only difference is in `pipeline_runs.rows_loaded` (5,119 vs 5,129). ETL counts the final fact rows. ELT counts every checked row that passed, including the 10 older order versions that the MERGE then updated.

---

## Part C: Scheduling

```python
schedule="0 9 * * 1-6",
start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
catchup=False, max_active_runs=1,
default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
              "execution_timeout": timedelta(minutes=15), "on_failure_callback": notify_failure}
```

- **What is your cron expression?** `0 9 * * 1-6`: minute 0, hour 9, Monday (1) to Saturday (6).
- **What time does the run show in UTC in the Airflow UI?** **03:30 UTC.** `airflow dags next-execution` returned `2026-10-22T03:30:00+00:00`, and the run IDs are `scheduled__…T03:30:00+00:00` (IST = UTC + 5:30). Airflow 3 uses `CronTriggerTimetable`, so the run starts *at* 09:00 IST, not at the end of a daily interval.
- **What would happen at 9 AM if `start_date` were in UTC?** The cron would be read in UTC, so the run would start at 09:00 UTC = **14:30 IST**: five hours *after* the 9:30 stand-up. Nothing would error, so it would quietly miss the SLA every day.
- **What if the laptop or Docker is off at 9 AM?** No run happens, and no alert fires, because the alerting lives inside the same Airflow. When the scheduler comes back, `catchup=False` creates **only the latest missed run**. I observed this when the DAGs were first unpaused at 22:48 IST: Airflow ran `scheduled__2026-10-21T03:30:00+00:00` once, not once per day since the 1 October start date. In production this needs an always-on scheduler (a server or a managed Airflow) and an external "no run by 09:05" heartbeat.
- **Why `catchup=False`?** Every run reads the *current* files: a full snapshot plus increments, with no date parameter. Backfilling 20 missed days would process the same files 20 times and gain nothing. It would also queue 20 runs against a single-writer DuckDB file.
- **Why these retries and timeout?** Two retries a minute apart cover a file that lands a minute late or a brief lock. A normal run takes 15 s (ETL) to 28 s (ELT), so a 15-minute timeout catches a hung task and still leaves time to alert before 09:30. `max_active_runs=1` stops a manual run and the scheduled run writing at the same time.

---

## Part D: Data quality dimensions

All 48 checks are defined once in [dq_rules.py]. They are implemented in pandas for ETL and in SQL for ELT, and both write the same check names to `dq_results` (`run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at`) on every run. Dataset-level checks (freshness, volume) report `rows_failed = 1` when the dataset fails.

**Coverage.** Every dimension has at least one check in orders, and every file has checks in 3–6 dimensions:

| Dimension | Example checks (file.check) | Severity |
|---|---|---|
| Completeness | orders.customer_id_missing, orders.delivery_minutes_missing, orders.volume_drop_vs_previous_run (W), products.category_missing (W), stores.zone_missing (W), customers.city_missing (W) | Critical / Warning |
| Uniqueness | orders.duplicate_order_id, payments.duplicate_payment_id, customers.duplicate_customer_id, products.duplicate_product_id, stores.duplicate_store_id | Critical |
| Validity | orders.order_status_invalid, orders.payment_mode_invalid, orders.order_ts_unparseable, orders.discount_pct_out_of_range, customers.email_invalid (W), customers.phone_invalid (W), customers.pincode_invalid (W) | Critical / Warning |
| Accuracy | orders.quantity_not_positive, orders.order_amount_formula_mismatch, orders.delivery_minutes_over_120, orders.unit_price_above_mrp, products.mrp_not_positive, products.cost_price_above_mrp, stores.capacity_not_positive (W) | Critical |
| Consistency | orders.{customer,product,store}_id_not_in_master, payments.order_id_unknown, orders.store_over_capacity (W, bonus), customers.loyalty_tier_nonstandard_case (W) | Critical / Warning |
| Timeliness | orders.order_ts_in_future, payments.payment_ts_in_future, customers.signup_date_in_future, orders.stale_data (W) | Critical / Warning |

**Final-run scorecard** (identical for both DAGs): **48 checks: 14 PASS, 22 FAIL (Critical), 12 WARN.** (The `dq_results.png` screenshot was taken before the bonus capacity rule was added, so it shows 47 checks with 13 PASS; the extra rule passes on clean data.)

```sql
SELECT check_name, dimension, severity, rows_checked, rows_failed, status
FROM dq_results
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_priyanka'
                ORDER BY start_time DESC LIMIT 1)
ORDER BY check_name;
```

Timeliness checks compare against an **as-of time** taken from the Airflow Variable `NAMMAMART_AS_OF` (default: the real clock). I set it to `2026-10-02 09:00:00`, the morning after the increment file, so results are reproducible. Experiment 9 shows why this matters.

---

## Business questions (identical answers from both pipelines)

SQL: [business_questions_etl.sql] and [business_questions_elt.sql]. **Each question's SQL and result, shown separately for ETL and for ELT, is in [docs/business_questions_results.md]**, generated from the warehouse after the final runs. All eight result sets (Q1a, Q1b, Q2, Q3, Q4a, Q4b, Q5, Q6) match row for row and value for value. Results are after the 1 October increment; the summary below is the same for both pipelines.

**Q1. Daily net revenue by zone and store.** 155 zone-day rows and 460 store-day rows. September + 1 October totals by zone:

| Zone | Delivered | Net revenue |
|---|---|---|
| East | 580 | ₹3,02,075.70 |
| West | 566 | ₹3,01,766.90 |
| South | 418 | ₹2,49,010.50 |
| North | 426 | ₹2,36,222.20 |
| Unknown (S07, blank zone) | 147 | ₹81,912.90 |

**Q2. Revenue and gross margin by category.**

| Category | Delivered | Net revenue | Gross margin | Margin % |
|---|---|---|---|---|
| Staples | 300 | ₹2,64,456.90 | ₹66,435.28 | 25.1 |
| Household | 360 | ₹2,10,607.95 | ₹54,168.91 | 25.7 |
| Personal Care | 319 | ₹2,08,774.70 | ₹46,709.57 | 22.4 |
| Dairy & Bakery | 402 | ₹1,77,404.85 | ₹47,200.82 | 26.6 |
| Snacks & Beverages | 340 | ₹1,74,863.60 | ₹41,398.13 | 23.7 |
| Fruits & Vegetables | 372 | ₹92,968.45 | ₹22,018.22 | 23.7 |
| Uncategorised | 44 | ₹41,911.75 | ₹4,943.39 | 11.8 |

**Q3. Delivery time per store.** The average is 20.5–22.3 minutes. No store delivers more than 36% of orders within 15 minutes. The best is S08 Whitefield (35.7%) and the worst is S15 Basaveshwaranagar (21.9%). The "10-minute promise" is not being met anywhere.

**Q4. Revenue by payment mode, and the unreconciled amount.** UPI ₹6,20,350.55 · Card ₹2,37,148.25 · Cash on Delivery ₹1,76,906.70 · Wallet ₹1,36,582.70. **Unreconciled: ₹2,33,819.48 across 384 rows.**

| Mismatch reason | Rows | Amount |
|---|---|---|
| payment_for_rejected_order (money taken for an order that failed validation) | 153 | ₹1,11,674.93 |
| no_payment_record (139 of these are 1 Oct orders whose payments file has not arrived yet) | 167 | ₹91,643.60 |
| payment_failed (DELIVERED, but the only payment FAILED) | 42 | ₹25,722.25 |
| returned_not_refunded (the 10 returns from the increment) | 10 | ₹4,193.70 |
| amount_mismatch (short by ₹25 / ₹50 / ₹100) | 12 | ₹585.00 |

**Q5. Top 10 customers** (masked contact details only): C00136 Sanjay B. (Bronze) ₹8,738.10 · C00283 Nikhil R. (Bronze) ₹7,934.60 · C00236 Sneha N. (Platinum) ₹7,831.80 · C00115 Sanjay K. (Bronze) ₹7,337.70 · C00310 Kiran P. (Bronze, email invalid so shown as NULL) ₹7,258.95 · C00265 Vikram H. (Gold) ₹7,151.30 · C00338 Bhavana I. (Silver) ₹6,848.55 · C00071 Mahesh P. (Silver) ₹6,696.15 · C00357 Arjun N. (Bronze) ₹6,637.90 · C00209 Kavya P. (Platinum) ₹6,519.40. Seven of the top 10 are Bronze or Silver, which is a loyalty-tier upgrade opportunity.

**Q6. Rows rejected in the latest run.** 243 rows: orders.csv 188, increment 11, payments.csv 29, customers.csv 8, products.csv 7, stores.csv 0. The largest reason is `product_id_not_in_master` (87 + 10), caused by the bad product master. A row can break several rules, so it is counted once per reason.

---

## Part E: Data governance

1. **Ownership.** Orders: Head of Ops (the order management system team). Payments: Finance Controller. Customers: CRM / Marketing lead (the PII data owner). Products: Category Management. Stores: Ops / Store Expansion. Warehouse tables: the data / FDE team are *custodians* (they operate the pipelines), while business owners own the meaning. A rule change such as the 120-minute delivery limit is proposed by the data team, **approved by the Ops owner** (it changes the SLA KPI) and countersigned by Finance if it affects revenue. It is then changed in one place, `config.py` / `dq_rules.py`, through a reviewed pull request, so ETL and ELT change together.

2. **Data dictionary** (fact and dimension tables; ELT `stg_*` tables carry the same columns plus `dq_status`, `failure_reason`, `dq_errors` and `dq_warnings`):

   | Table.column | Type | Meaning | Source | Allowed values | PII |
   |---|---|---|---|---|---|
   | fact_orders.order_id | VARCHAR | Order key | orders.order_id | `ORD` + 6 digits, unique | No |
   | fact_orders.order_ts / order_date | TIMESTAMP / DATE | When the order was placed (IST) | orders.order_ts | ≤ as-of time | No |
   | fact_orders.customer_id / store_id / product_id | VARCHAR | Foreign keys | orders | Exist in the dimension | Pseudonymous |
   | fact_orders.quantity | INTEGER | Units | orders.quantity | > 0 | No |
   | fact_orders.unit_price / discount_pct / order_amount | DOUBLE | Price, %, amount after discount | orders | price ≤ MRP; 0–100; matches formula | No |
   | fact_orders.payment_mode | VARCHAR | Standardised mode | orders.payment_mode | UPI / Card / Cash on Delivery / Wallet | No |
   | fact_orders.order_status | VARCHAR | Latest status (increment wins) | orders + increment | DELIVERED / CANCELLED / RETURNED | No |
   | fact_orders.delivery_minutes | INTEGER | Minutes to deliver | orders | < 120 | No |
   | fact_orders.net_revenue | DOUBLE | order_amount if DELIVERED, else 0 | derived | ≥ 0 | No |
   | fact_orders.gross_margin | DOUBLE | net_revenue − quantity × cost_price (DELIVERED only) | derived | any | No |
   | fact_orders.within_15_min | BOOLEAN | delivery_minutes ≤ 15 (DELIVERED only) | derived | true / false / null | No |
   | fact_orders._source_file / _source_row / _run_id | VARCHAR / BIGINT / VARCHAR | Lineage back to the exact CSV line and run | pipeline | n/a | No |
   | dim_customer.customer_name_masked | VARCHAR | First name + last initial | customers.full_name | "Priya H." | **Yes (masked)** |
   | dim_customer.email_masked / phone_masked | VARCHAR | `pr***@example.com` / `97******21` | customers.email / phone | NULL if invalid | **Yes (masked)** |
   | dim_customer.city / area / pincode | VARCHAR | Location (pincode NULL if invalid) | customers | 6-digit pincode | Yes (quasi-identifier) |
   | dim_customer.signup_date, loyalty_tier, is_active | DATE / VARCHAR / BOOLEAN | CRM attributes | customers | Bronze / Silver / Gold / Platinum | No |
   | dim_product.category, mrp, cost_price … | VARCHAR / DOUBLE | Catalogue (`Uncategorised` if blank) | products | mrp > 0, cost ≤ mrp | No |
   | dim_store.zone, capacity_orders_per_day, manager_name … | VARCHAR / INTEGER | Store master (`Unknown` if blank) | stores | North / South / East / West | manager_name: employee personal data |

3. **Lineage.** Take `mart_daily_store_kpis`, S01 on 2026-09-01: **₹1,410.80 from 3 delivered orders.** In ELT, `stg_orders` gives ORD000058 (₹594.00), ORD000025 (₹788.00) and ORD000013 (₹28.80), with `_source_file = orders.csv` and `_source_row` 443, 727 and 1368. Joining on `(_run_id, _source_file, _source_row)` returns the identical untouched rows in `raw_orders`. ETL can do the same because I carried `_source_file` and `_source_row` into `etl_fact_orders`, which gives the same 3 lines, but only back to *a file on disk that may have been overwritten since*. **ELT is easier and stronger**: the raw evidence is inside the warehouse, versioned by `_run_id`, so you can trace every number with SQL alone, even for last month's files.

4. **Personal data.** Customers: `full_name`, `email`, `phone`; quasi-identifiers `area` and `pincode` (plus `city`). Stores: `manager_name` (employee). Orders link to a person only through `customer_id` (pseudonymous). I **masked** rather than deleted because the business still needs to *recognise* a customer (top spenders, support), deduplicate them, and join their orders. Masking keeps that utility while hiding the identifying part. Unmasked values exist only in `include/data/` and `raw_customers` (git-ignored warehouse). Quarantine files and the profiling output are masked too. The applicable law is **India's Digital Personal Data Protection Act, 2023** (DPDP) and the DPDP Rules 2025. It expects purpose limitation and data minimisation, consent or a legitimate use, reasonable security safeguards, deletion once the purpose is served, breach notification to the Data Protection Board and to affected users, and honouring the rights to access, correct and erase.

5. **Access control.** **Raw:** the data engineering service account plus 1–2 named data engineers; no analysts, because it holds unmasked PII. **Staging:** data engineers and analytics engineers (PII is already masked). **Mart:** finance, ops, analysts and the COO's dashboard (read-only). **Quarantine:** data engineers plus the data owner of each file, so they can fix the source. **Raw unmasked phone numbers:** nobody needs them for reporting. Only the CRM / support system of record should hold them. If a phone is needed (for example to contact a customer about a failed payment), that happens in the CRM, with access logged, not from the warehouse.

6. **Retention.** Raw: 1 year for audit, then delete. Finance's statutory record is the books and invoices, not this copy. For raw customers, keep only the latest snapshot plus 90 days. Quarantine: 90 days after the issue is resolved. Masked customer data: while the customer is active plus a limited period (for example 3 years after the last order, a business decision), then delete or fully anonymise (drop `customer_id` linkage). After that, a scheduled purge job deletes by `_loaded_at`, and the job's own log is the proof of deletion. Today raw grows by about 5,400 rows per run (20,631 order rows after 11 ELT runs), so a retention job is required, not optional.

7. **Secrets.** `SLACK_WEBHOOK_URL` is an **Airflow Variable**, read at run time with `Variable.get("SLACK_WEBHOOK_URL", default=None)` in [alerts.py]. If it is not set, the alert prints to the task log (that's what Experiment 4 showed). In production it belongs in a secrets backend (Vault, AWS Secrets Manager) or an environment variable that isn't committed (`.env` is git-ignored). It must never be in a DAG file or on GitHub: anyone with repo access, or anyone who forks it, could post into the company Slack. Leaked webhooks are scraped from public repos within minutes, and removing one from git history is hard. Rotating secrets also shouldn't require a code deploy.

8. **Audit: show a rejected order exactly as received, the rule it broke, and prove it isn't in revenue.** Example: ORD000500.
   - **ELT:**
     ```sql
     SELECT source_file, source_row, failure_reason, raw_record
     FROM elt_quarantine WHERE record_key = 'ORD000500';
     ```
     returns `orders.csv` line **746**, `order_status_invalid`, and the raw JSON with `"order_status":"Pending??"`. You can re-check it with `SELECT * FROM raw_orders WHERE _source_file = 'orders.csv' AND _source_row = 746`. The proof that it isn't in revenue: `SELECT count(*) FROM stg_orders WHERE order_id = 'ORD000500' AND dq_status <> 'FAIL'` returns **0**, and every mart is built only from `dq_status <> 'FAIL'`.
   - **ETL:** `etl_quarantine` (or `orders_failed_<run>.csv`) gives the same line 746 and the same reason. `SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD000500'` returns **0**. "Exactly as received" means opening `orders.csv` at line 746, which only works while that file is still on disk.
   - **Bonus:** its ₹332 Card payment appears in the reconciliation as `payment_for_rejected_order`, so finance can see that the money was collected even though the order was rejected.

9. **Change history.** ORD000271 was DELIVERED on 4 September (₹445) and RETURNED in the 1 October increment. After the incremental load, **both pipelines overwrite it**: September revenue drops from ₹11,00,259.60 to **₹10,96,065.90** (−₹4,193.70 across the 10 returned orders). That restates a closed month. **Finance usually wants September as reported** (books closed), with the return booked as an October adjustment. To keep both views:
   - ELT already holds every version: `raw_orders` (and `stg_orders_checked`) has ORD000271 as DELIVERED from `orders.csv` and RETURNED from the increment, so an "as-reported" view can be rebuilt with SQL.
   - Make it explicit with an SCD-Type-2 `stg_orders_history` (`valid_from`, `valid_to`, `is_current`), or an append-only `fact_order_events` table (sale on 4 Sep, return on 1 Oct). Then "September as reported" and "September as now known" are both one query.
   - ETL loses the old version once it upserts. It would need a snapshot table written on every run.

---

## Part F: Observability tests

A run audit (`audit` task, `trigger_rule="all_done"`, retries 0) writes one row per run to `pipeline_runs` (`run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status, failed_tasks`). It then **fails on purpose** if any upstream task produced no result, so a broken run can't show green. `on_failure_callback` sends a Slack alert (or prints it if the Variable isn't set). All 10 experiments were run on both DAGs on 21 October 2026; times are UTC.

| # | Experiment | Expected | Observed (ETL / ELT) | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | Green, parallel extracts | Both green. ETL 15.3 s: 5 extracts started within 0.1 s, 4 validations in parallel at +1.9 s. ELT 27.8 s: 6 raw loads started together but took 1.5–6.5 s each because they **queue for the DuckDB write lock**. Rows in 5,212 → passed 4,980 / quarantined 232 (orders 2,412 → 188 quarantined, payments 2,228 → 29, customers 405 → 8, products 152 → 7, stores 15 → 0). | Graph and Gantt views; `pipeline_runs`; `[Validate]` log lines | ELT raw loads gain nothing from parallelism on one DuckDB file: either run them serially, or stage them to Parquet first |
| 2 | Re-run | Fact counts unchanged | ETL: `0 inserted, 2224 updated`, still 2,224 fact rows and ₹11,00,259.60. ELT: `raw_orders` **grew** 2,412 → 4,824 (correct: raw is an append-only receipt log); `stg_orders` stayed at 2,400; marts unchanged. Clearing a task inside the same run doesn't double-load raw (delete by `_run_id` first). | `load` log; `SELECT count(*)` on the tables | Add a raw retention / compaction job, because raw grows by about 5,400 rows per run |
| 3 | Incremental load | 150 new, 10 updated | ELT MERGE: **150 INSERT, 10 UPDATE** → 2,550 rows, no duplicate `order_id`. ETL: 10 versions superseded, 139 inserted (11 new orders quarantined: their product or customer failed master checks). ORD000017 and others are now RETURNED. Revenue ₹11,70,988.20 in both. Reconciliation shows 10 `returned_not_refunded`. | `stg_orders_merge_increments` log; `GROUP BY order_id HAVING count(*) > 1` returns 0 rows | Load the next-day payments file too: 139 `no_payment_record` mismatches are just missing 1 Oct payments |
| 4 | Missing `products.csv` | Retries, then fail | `extract_products` / `load_raw_products` attempts at 17:42:15, 17:43:17, 17:44:18 (1 min apart) → `up_for_retry` ×2 → `failed`. Every downstream task `upstream_failed`; independent branches (other extracts and validations) still succeeded. Log: `Source file missing: …/products.csv`. Alert printed (no webhook set). `pipeline_runs` = FAILED with `failed_tasks` listed. | Grid view (orange, then red); task log; `pipeline_runs` | Set the Slack Variable; add a `FileSensor` with a deadline (for example 08:45) so a late file is distinguished from a missing one |
| 5 | 5 new bad rows | Each quarantined with the right reason | Both: ORD900001 `customer_id_missing`, ORD900002 `quantity_not_positive`, ORD900003 `store_id_not_in_master`, ORD900004 `order_status_invalid` (SHIPPED), ORD900005 `order_ts_in_future`. Each check's `rows_failed` went up by exactly 1, identically in both. **The first ELT attempt failed:** the appended rows ended in LF while the export uses CRLF, and DuckDB's strict CSV sniffer rejected the mixed file. Fix: explicit dialect, `strict_mode=false`, plus a loaded-vs-file row-count assertion. Re-run green. | `etl_quarantine` / `elt_quarantine`; `dq_results`; `orders_failed_manual_…17_49_37….csv` | Keep the row-count assertion; normalise line endings at ingestion |
| 6 | `order_amount` renamed to `total` | Fail loudly | Both failed at the **first** task after 3 tries: `Schema change in orders.csv: missing=['order_amount'] unexpected=['total']`. Nothing downstream ran; no wrong data loaded. | Task log; `pipeline_runs` FAILED | Schema failures shouldn't retry (they will never self-heal): raise `AirflowFailException` to alert 2 minutes sooner |
| 7 | Only 50 orders | Volume warning | Both: `orders.volume_drop_vs_previous_run` = **WARN** (`this run=210, previous run=2577, drop>90%=True`). Marts didn't change (the upsert never deletes missing orders), so a missing day would look like a slow day. 2,199 payments were also quarantined as `order_id_unknown`, a second volume signal. | `dq_results`; `validate_orders` log | Make volume drops **Critical** (stop the publish) and compare against the same weekday's average, not just the previous run |
| 8 | `unit_price × 100` for S05 | Some check catches it | Caught (Critical) in both: `order_amount_formula_mismatch` 162, `unit_price_above_mrp` 139. Drifted orders were quarantined, so revenue was **not** inflated 100×. **S05 net revenue fell ₹82,456.15 → ₹7,491.20 (−91%)**; company total ₹11,70,988.20 → ₹10,96,023.25. No check said "S05 revenue dropped 91%". | `dq_results`; `mart_daily_store_kpis` | Add a per-store distribution check (revenue or avg price vs 4-week median ± 3σ) as a Warning |
| 9 | Latest order 30 days old | Stale warning | Both: `orders.stale_data` = **WARN** (as-of 2026-10-30 23:50, increment removed). Side effect: orders dated 2026-10-21 10:15 stopped being "future" (`order_ts_in_future` 7 → 5) and became the "latest order", which shows that wall-clock rules make results depend on *when* you run. | `dq_results`; `validate_orders` log | Pin the as-of time to the run's logical date and alert on staleness at 08:30 |
| 10 | Recovery: Clear the failed task | Only the failed and downstream tasks re-run | After restoring `products.csv`, cleared `extract_products` (or `load_raw_products`) + downstream. Only those ran (try 4); the successful extracts and validations kept **try=1 from 17:42** and reused their outputs (`include/tmp/`, kept for 3 days). Run turned green; the audit replaced its row with SUCCESS. ELT: `raw_products` = 608 = 152 × 4 successful runs (no double load). | Grid view (try numbers); `pipeline_runs` | None: this works as designed |



**Five pillars of data observability**

| Pillar | How my setup covers it | Strength |
|---|---|---|
| Freshness | `stale_data` (24 h vs as-of), `*_in_future`, 09:00 schedule, run history | Medium: a warning, not an alert; nothing fires if Airflow itself is down |
| Volume | `rows_extracted / rows_loaded / rows_quarantined` per run, `volume_drop_vs_previous_run`, ELT row-count assertion | Good |
| Schema | Header check in both pipelines + `INSERT BY NAME`; fails loudly at the first task | **Strongest** |
| Distribution | Formula, MRP and range checks (catch impossible values), quarantine counts by reason | **Weakest**: nothing watches plausible-but-shifted values (Exp 8 needed a manual comparison) |
| Lineage | `_source_file`, `_source_row`, `_run_id` on every fact and staging row; raw kept (ELT) | Good in ELT, partial in ETL |

---

## Part G: Recommendation to Sameer (CTO)



**Recommendation: adopt ELT, with ETL's quality gate built into the staging layer.** That is the pipeline I built as `nammamart_elt_priyanka`. Finance keeps its guarantee that no bad row reaches the CFO's numbers, and analytics and compliance get every raw record.

**What I observed running both side by side on your data (21 Oct 2026):**

- **Data-quality guarantee: a tie.** Both pipelines applied the same 48 rules and produced *identical* revenue (₹11,70,988.20), margin, quarantine (243 rows) and reconciliation (₹2,33,819.48 unreconciled). In ELT, the CFO's marts read only rows that passed every Critical check, the same guarantee ETL gives. I tested this with 5 injected bad rows and a 100× price error: none reached revenue in either pipeline.
- **Auditability: ELT wins clearly.** Finance asked to see a rejected order exactly as received. ELT answers with one SQL query (order ORD000500, line 746, "Pending??", rule broken, absent from revenue). ETL can only point to a file on disk that tomorrow's export may overwrite. When the 1 October file turned 10 September sales into returns, both pipelines updated September revenue (₹11,00,259.60 → ₹10,96,065.90), but **only ELT still holds the original version**, so finance can see "as reported" and "as now known" side by side.
- **Ease of change: ELT wins.** Rules and reports are plain `.sql` files that an analyst can read and review. New questions don't need a new pipeline, because the raw data is already there. ETL changes need Python changes and a re-extract.
- **Performance at 1000× (about 2.4 million orders a month).** ETL's pandas steps hold whole files in memory and would need rework. ELT pushes the work into the database. *But* DuckDB in a single file allows one writer at a time: our 6 parallel loads already queued, and one analyst's open DBeaver session blocked an entire run. At that scale, move the same SQL to a server database (PostgreSQL, or DuckDB / ClickHouse over partitioned Parquet). The SQL carries over largely unchanged.
- **Cost: about ₹0 in licences for both** (all open source). ELT stores more, because raw grows about 5,400 rows per run with today's full-snapshot exports. That's a few MB a day, negligible against the half-day a week finance spends reconciling today.

**What changes for the business tomorrow:** the numbers are ready by 09:00 IST, Monday to Saturday, with a scorecard (`dq_results`) and a run log (`pipeline_runs`) showing whether today's run worked and why not. Two findings need owners now:
1. **Fix 8 product rows and 3 customer rows** in the catalogue and CRM. They block 104 otherwise-valid orders (₹45,134 of revenue).
2. **₹1.12 lakh of payments were collected for orders that failed validation**, and 42 delivered orders have only a failed payment. Finance should work through `mart_payment_reconciliation` daily.

**Top 3 risks of the recommended design**
1. **PII in raw.** Raw keeps unmasked names, emails and phones forever unless we act. Mitigation: restrict raw to the pipeline service account, set a 1-year purge job (DPDP Act: storage limitation and security safeguards), and keep the warehouse file out of git (done).
2. **Single point of failure.** One DuckDB file on one laptop: if the laptop is off, or a SQL client holds a lock, there's no 9 AM report and no alert. Mitigation: run Airflow on an always-on server, add an external "report not ready by 09:05" heartbeat, give BI users a read-only replica, and plan the move to a server database before volume grows 10×.
3. **Quality rules are business decisions held in code.** The 120-minute limit, the cascade rule and the 90% volume threshold change what counts as revenue. If they drift without sign-off, ETL-style trust is lost. Mitigation: named owners per rule (Part E1), changes only through reviewed pull requests, and the `dq_results` trend reviewed weekly with Ops and Finance.

---

## Bonus

| Bonus item | Status | Where |
|---|---|---|
| Slack / email alert on failure using an Airflow Variable | DONE: `on_failure_callback` reads `SLACK_WEBHOOK_URL` with `Variable.get()`. Fired in Experiments 4 and 6; no webhook is configured, so the alert printed to the task log. | [include/nammamart/alerts.py] |
| Store-capacity check (daily orders vs `capacity_orders_per_day`) | DONE: in both pipelines and tested | Rule `orders.store_over_capacity`; [etl_core.py] `validate_orders`; [stg_orders_merge.sql] |
| Data-quality trend chart built from `dq_results` | DONE: rebuilt on every run of either DAG | [include/reports/dq_trend.svg], [dq_trend.html], task `dq_trend_chart`, table `dq_trend`, code [dq_trend.py] |


### Store-capacity check

- **Rule.** For each source file, count the orders each store took on each day and compare the count with that store's `capacity_orders_per_day` from the validated store master. Every order on an over-capacity store-day is flagged `store_over_capacity`. Dimension: **Consistency** (orders must agree with the store master). Severity: **Warning**, because a store being overloaded is an ops alert, not bad data, so revenue is never blocked. Stores with capacity ≤ 0 (S12) are skipped, because `stores.capacity_not_positive` already flags them.
- **On the real data it passes.** The busiest store-day is S11 with 18 orders against a capacity of 300, so peak utilisation across all stores is ≤ 6%. The exports are a sample of NammaMart's volume, or capacity is set far above demand: a point to confirm with ops.
- **Experiment.** In a copy of `stores.csv` I set S11's capacity to 10 and triggered both DAGs. Both reported `orders.store_over_capacity` = **18 rows, WARN**: the 18 S11 orders on 2026-10-01, the store's only day above 10 orders. Revenue stayed at ₹11,70,988.20, because Warnings never block rows. After restoring the file, both pipelines returned to 0.

### Data-quality trend chart

![Rows failing Critical data-quality rules per run, ETL and ELT]

- **What it shows.** One stacked column per run, split by source file, with ETL and ELT on the same y-scale. The measure is the sum of `rows_failed` over Critical checks, so a row that breaks two rules counts twice. Runs that failed wrote no `dq_results` and are marked "✕ failed" instead of being silently missing.
- **The experiments are visible:** the steady baseline (246 before the increment, 257 after); +5 for the injected bad rows (262); "✕ failed" for the schema change (and ELT's first Experiment 5 attempt, the mixed line endings); a spike to 2,221 for the 50-order volume test (2,199 payments became orphans); and 540 for the ×100 price drift.
- **How it's built.** The `dq_trend_chart` task runs before `audit` in both DAGs. It rebuilds the `dq_trend` table and writes the SVG (which renders on GitHub in light and dark mode) and an HTML version with hover tooltips and a table view of the same numbers. It's plain Python with no plotting library, so the Airflow image needed no new package. Colours are the validated categorical palette in fixed order, one per file. The palette passed a colour-blind-safety validator in light and dark mode, and the totals labels plus the table view cover its three low-contrast light-mode colours.

## Screenshots

Saved in [docs/screenshots/]:
1. `etl_graph_green.png` and `elt_graph_green.png`
2. `schedule_ist.png`
3. `quarantine_file.png`
4. `dq_results.png`
5.`pipeline_runs.png`
6.`payment_reconciliation.png`

