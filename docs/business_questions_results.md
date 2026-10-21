# Business questions: SQL and results from both pipelines

Generated read-only from `include/warehouse_priyanka.duckdb` after the final runs (ETL `manual__2026-10-21T18:01:07.636508+00:00`, ELT `manual__2026-10-21T18:01:26.417482+00:00`): September data plus the 1 October increment. Source SQL: [business_questions_etl.sql](../include/sql/questions/business_questions_etl.sql) and [business_questions_elt.sql](../include/sql/questions/business_questions_elt.sql). Each section shows the ETL query and result, then the ELT query and result, then whether every value matches.

## Q1a. Daily net revenue by zone (delivered orders only, after discount)

**ETL** (first 10 of 155 rows)

```sql
SELECT order_date, zone, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, zone;
```

| order_date | zone | delivered_orders | net_revenue |
|---|---|---|---|
| 2026-09-01 00:00:00 | East | 18 | 9868.05 |
| 2026-09-01 00:00:00 | North | 15 | 8380.15 |
| 2026-09-01 00:00:00 | South | 9 | 4718.3 |
| 2026-09-01 00:00:00 | Unknown | 9 | 5844.75 |
| 2026-09-01 00:00:00 | West | 13 | 5790.15 |
| 2026-09-02 00:00:00 | East | 16 | 7984.6 |
| 2026-09-02 00:00:00 | North | 9 | 3573.8 |
| 2026-09-02 00:00:00 | South | 16 | 12423.0 |
| 2026-09-02 00:00:00 | Unknown | 3 | 824.1 |
| 2026-09-02 00:00:00 | West | 16 | 10423.35 |

**ELT** (first 10 of 155 rows)

```sql
SELECT order_date, zone, SUM(delivered_orders) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY ALL ORDER BY order_date, zone;
```

| order_date | zone | delivered_orders | net_revenue |
|---|---|---|---|
| 2026-09-01 00:00:00 | East | 18.0 | 9868.05 |
| 2026-09-01 00:00:00 | North | 15.0 | 8380.15 |
| 2026-09-01 00:00:00 | South | 9.0 | 4718.3 |
| 2026-09-01 00:00:00 | Unknown | 9.0 | 5844.75 |
| 2026-09-01 00:00:00 | West | 13.0 | 5790.15 |
| 2026-09-02 00:00:00 | East | 16.0 | 7984.6 |
| 2026-09-02 00:00:00 | North | 9.0 | 3573.8 |
| 2026-09-02 00:00:00 | South | 16.0 | 12423.0 |
| 2026-09-02 00:00:00 | Unknown | 3.0 | 824.1 |
| 2026-09-02 00:00:00 | West | 16.0 | 10423.35 |

_ETL and ELT results identical (all 155 rows, every value): **yes**._

## Q1b. Daily net revenue by store

**ETL** (first 10 of 460 rows)

```sql
SELECT f.order_date, s.store_id, s.store_name, s.zone,
       ROUND(SUM(f.net_revenue), 2) AS net_revenue
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY f.order_date, s.store_id;
```

| order_date | store_id | store_name | zone | net_revenue |
|---|---|---|---|---|
| 2026-09-01 00:00:00 | S01 | NammaMart Hebbal | North | 1410.8 |
| 2026-09-01 00:00:00 | S02 | NammaMart Yelahanka | North | 2385.55 |
| 2026-09-01 00:00:00 | S03 | NammaMart RT Nagar | North | 4583.8 |
| 2026-09-01 00:00:00 | S04 | NammaMart Jayanagar | South | 1103.3 |
| 2026-09-01 00:00:00 | S05 | NammaMart JP Nagar | South | 803.0 |
| 2026-09-01 00:00:00 | S06 | NammaMart Banashankari | South | 2812.0 |
| 2026-09-01 00:00:00 | S07 | NammaMart BTM Layout | Unknown | 5844.75 |
| 2026-09-01 00:00:00 | S08 | NammaMart Whitefield | East | 2984.7 |
| 2026-09-01 00:00:00 | S09 | NammaMart Indiranagar | East | 2620.6 |
| 2026-09-01 00:00:00 | S10 | NammaMart Marathahalli | East | 2141.6 |

**ELT** (first 10 of 460 rows)

```sql
SELECT order_date, store_id, store_name, zone, net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY order_date, store_id;
```

| order_date | store_id | store_name | zone | net_revenue |
|---|---|---|---|---|
| 2026-09-01 00:00:00 | S01 | NammaMart Hebbal | North | 1410.8 |
| 2026-09-01 00:00:00 | S02 | NammaMart Yelahanka | North | 2385.55 |
| 2026-09-01 00:00:00 | S03 | NammaMart RT Nagar | North | 4583.8 |
| 2026-09-01 00:00:00 | S04 | NammaMart Jayanagar | South | 1103.3 |
| 2026-09-01 00:00:00 | S05 | NammaMart JP Nagar | South | 803.0 |
| 2026-09-01 00:00:00 | S06 | NammaMart Banashankari | South | 2812.0 |
| 2026-09-01 00:00:00 | S07 | NammaMart BTM Layout | Unknown | 5844.75 |
| 2026-09-01 00:00:00 | S08 | NammaMart Whitefield | East | 2984.7 |
| 2026-09-01 00:00:00 | S09 | NammaMart Indiranagar | East | 2620.6 |
| 2026-09-01 00:00:00 | S10 | NammaMart Marathahalli | East | 2141.6 |

_ETL and ELT results identical (all 460 rows, every value): **yes**._

## Q2. Revenue and gross margin by product category

**ETL** (7 rows)

```sql
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC;
```

| category | delivered_orders | net_revenue | gross_margin | margin_pct |
|---|---|---|---|---|
| Staples | 300 | 264456.9 | 66435.28 | 25.1 |
| Household | 360 | 210607.95 | 54168.91 | 25.7 |
| Personal Care | 319 | 208774.7 | 46709.57 | 22.4 |
| Dairy & Bakery | 402 | 177404.85 | 47200.82 | 26.6 |
| Snacks & Beverages | 340 | 174863.6 | 41398.13 | 23.7 |
| Fruits & Vegetables | 372 | 92968.45 | 22018.22 | 23.7 |
| Uncategorised | 44 | 41911.75 | 4943.39 | 11.8 |

**ELT** (7 rows)

```sql
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC;
```

| category | delivered_orders | net_revenue | gross_margin | margin_pct |
|---|---|---|---|---|
| Staples | 300 | 264456.9 | 66435.28 | 25.1 |
| Household | 360 | 210607.95 | 54168.91 | 25.7 |
| Personal Care | 319 | 208774.7 | 46709.57 | 22.4 |
| Dairy & Bakery | 402 | 177404.85 | 47200.82 | 26.6 |
| Snacks & Beverages | 340 | 174863.6 | 41398.13 | 23.7 |
| Fruits & Vegetables | 372 | 92968.45 | 22018.22 | 23.7 |
| Uncategorised | 44 | 41911.75 | 4943.39 | 11.8 |

_ETL and ELT results identical (all 7 rows, every value): **yes**._

## Q3. Average delivery time per store and % of delivered orders within 15 minutes

**ETL** (15 rows)

```sql
SELECT s.store_id, s.store_name,
       ROUND(AVG(f.delivery_minutes), 1)                         AS avg_delivery_minutes,
       ROUND(100.0 * AVG(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 1) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY pct_within_15_min DESC;
```

| store_id | store_name | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|
| S08 | NammaMart Whitefield | 20.5 | 35.7 |
| S07 | NammaMart BTM Layout | 21.1 | 33.3 |
| S01 | NammaMart Hebbal | 20.5 | 32.4 |
| S14 | NammaMart Vijayanagar | 21.5 | 31.3 |
| S02 | NammaMart Yelahanka | 21.2 | 30.3 |
| S05 | NammaMart JP Nagar | 21.2 | 29.6 |
| S03 | NammaMart RT Nagar | 20.7 | 29.5 |
| S11 | NammaMart HSR Layout | 20.7 | 28.7 |
| S09 | NammaMart Indiranagar | 21.2 | 27.3 |
| S13 | NammaMart Malleshwaram | 21.9 | 27.3 |
| S04 | NammaMart Jayanagar | 21.5 | 26.9 |
| S10 | NammaMart Marathahalli | 21.9 | 26.2 |
| S12 | NammaMart Rajajinagar | 22.0 | 23.7 |
| S06 | NammaMart Banashankari | 22.3 | 22.1 |
| S15 | NammaMart Basaveshwaranagar | 22.2 | 21.9 |

**ELT** (15 rows)

```sql
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC;
```

| store_id | store_name | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|
| S08 | NammaMart Whitefield | 20.5 | 35.7 |
| S07 | NammaMart BTM Layout | 21.1 | 33.3 |
| S01 | NammaMart Hebbal | 20.5 | 32.4 |
| S14 | NammaMart Vijayanagar | 21.5 | 31.3 |
| S02 | NammaMart Yelahanka | 21.2 | 30.3 |
| S05 | NammaMart JP Nagar | 21.2 | 29.6 |
| S03 | NammaMart RT Nagar | 20.7 | 29.5 |
| S11 | NammaMart HSR Layout | 20.7 | 28.7 |
| S13 | NammaMart Malleshwaram | 21.9 | 27.3 |
| S09 | NammaMart Indiranagar | 21.2 | 27.3 |
| S04 | NammaMart Jayanagar | 21.5 | 26.9 |
| S10 | NammaMart Marathahalli | 21.9 | 26.2 |
| S12 | NammaMart Rajajinagar | 22.0 | 23.7 |
| S06 | NammaMart Banashankari | 22.3 | 22.1 |
| S15 | NammaMart Basaveshwaranagar | 22.2 | 21.9 |

_ETL and ELT results identical (all 15 rows, every value): **yes**._

## Q4a. Revenue by payment mode

**ETL** (4 rows)

```sql
SELECT payment_mode, COUNT(*) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY ALL ORDER BY net_revenue DESC;
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| UPI | 1158 | 620350.55 |
| Card | 432 | 237148.25 |
| Cash on Delivery | 307 | 176906.7 |
| Wallet | 240 | 136582.7 |

**ELT** (4 rows)

```sql
SELECT payment_mode, COUNT(*) AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
FROM stg_orders WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED'
GROUP BY ALL ORDER BY net_revenue DESC;
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| UPI | 1158 | 620350.55 |
| Card | 432 | 237148.25 |
| Cash on Delivery | 307 | 176906.7 |
| Wallet | 240 | 136582.7 |

_ETL and ELT results identical (all 4 rows, every value): **yes**._

## Q4b. Amount that does not reconcile with the payment gateway

**ETL** (6 rows)

```sql
SELECT mismatch_reason, COUNT(*) AS rows, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM etl_payment_reconciliation
GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason NULLS LAST;
```

| mismatch_reason | rows | unreconciled_amount |
|---|---|---|
| amount_mismatch | 12 | 585.0 |
| no_payment_record | 167 | 91643.6 |
| payment_failed | 42 | 25722.25 |
| payment_for_rejected_order | 153 | 111674.93 |
| returned_not_refunded | 10 | 4193.7 |
|  | 384 | 233819.48 |

**ELT** (6 rows)

```sql
SELECT mismatch_reason, COUNT(*) AS rows, ROUND(SUM(ABS(difference)), 2) AS unreconciled_amount
FROM mart_payment_reconciliation
GROUP BY ROLLUP (mismatch_reason) ORDER BY mismatch_reason NULLS LAST;
```

| mismatch_reason | rows | unreconciled_amount |
|---|---|---|
| amount_mismatch | 12 | 585.0 |
| no_payment_record | 167 | 91643.6 |
| payment_failed | 42 | 25722.25 |
| payment_for_rejected_order | 153 | 111674.93 |
| returned_not_refunded | 10 | 4193.7 |
|  | 384 | 233819.48 |

_ETL and ELT results identical (all 6 rows, every value): **yes**._

## Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)

**ETL** (10 rows)

```sql
SELECT c.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       ROUND(SUM(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC, c.customer_id LIMIT 10;
```

| customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | total_spend |
|---|---|---|---|---|---|
| C00136 | Sanjay B. | sa***@example.com | 96******88 | Bronze | 8738.1 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | Bronze | 7934.6 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | Platinum | 7831.8 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | Bronze | 7337.7 |
| C00310 | Kiran P. |  | 91******35 | Bronze | 7258.95 |
| C00265 | Vikram H. | vi***@example.com | 91******69 | Gold | 7151.3 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | Silver | 6848.55 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | Silver | 6696.15 |
| C00357 | Arjun N. | ar***@example.com | 93*****12 | Bronze | 6637.9 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | Platinum | 6519.4 |

**ELT** (10 rows)

```sql
SELECT customer_id, customer_name_masked, email_masked, phone_masked, loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY spend_rank, customer_id LIMIT 10;
```

| customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | total_spend |
|---|---|---|---|---|---|
| C00136 | Sanjay B. | sa***@example.com | 96******88 | Bronze | 8738.1 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | Bronze | 7934.6 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | Platinum | 7831.8 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | Bronze | 7337.7 |
| C00310 | Kiran P. |  | 91******35 | Bronze | 7258.95 |
| C00265 | Vikram H. | vi***@example.com | 91******69 | Gold | 7151.3 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | Silver | 6848.55 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | Silver | 6696.15 |
| C00357 | Arjun N. | ar***@example.com | 93*****12 | Bronze | 6637.9 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | Platinum | 6519.4 |

_ETL and ELT results identical (all 10 rows, every value): **yes**._

## Q6. Rows rejected in the latest run, by file and by reason

**ETL** (26 rows)

```sql
SELECT source_file, reason, COUNT(*) AS rows
FROM (
    SELECT source_file, UNNEST(STRING_SPLIT(REPLACE(failure_reason, chr(59) || ' ', '|'), '|')) AS reason
    FROM etl_quarantine
    WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_priyanka'
                    ORDER BY start_time DESC LIMIT 1)
)
GROUP BY ALL ORDER BY source_file, rows DESC;
```

| source_file | reason | rows |
|---|---|---|
| customers.csv | duplicate_customer_id | 5 |
| customers.csv | signup_date_in_future | 3 |
| orders.csv | product_id_not_in_master | 87 |
| orders.csv | customer_id_missing | 18 |
| orders.csv | order_amount_formula_mismatch | 18 |
| orders.csv | customer_id_not_in_master | 18 |
| orders.csv | duplicate_order_id | 12 |
| orders.csv | quantity_not_positive | 10 |
| orders.csv | delivery_minutes_missing | 8 |
| orders.csv | order_ts_in_future | 7 |
| orders.csv | order_ts_unparseable | 6 |
| orders.csv | delivery_minutes_over_120 | 6 |
| orders.csv | delivery_minutes_outside_normal | 6 |
| orders.csv | store_id_not_in_master | 5 |
| orders.csv | order_status_invalid | 3 |
| orders.csv | discount_pct_out_of_range | 3 |
| orders.csv | payment_mode_nonstandard | 1 |
| orders_2026-10-01_increment.csv | product_id_not_in_master | 10 |
| orders_2026-10-01_increment.csv | customer_id_not_in_master | 1 |
| payments.csv | amount_paid_not_positive | 10 |
| payments.csv | order_id_unknown | 8 |
| payments.csv | payment_ts_in_future | 7 |
| payments.csv | duplicate_payment_id | 4 |
| products.csv | cost_price_above_mrp | 5 |
| products.csv | duplicate_product_id | 2 |
| products.csv | mrp_not_positive | 1 |

**ELT** (26 rows)

```sql
SELECT source_file, reason, COUNT(*) AS rows
FROM (
    SELECT source_file, UNNEST(STRING_SPLIT(REPLACE(failure_reason, chr(59) || ' ', '|'), '|')) AS reason
    FROM elt_quarantine
    WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_elt_priyanka'
                    ORDER BY start_time DESC LIMIT 1)
)
GROUP BY ALL ORDER BY source_file, rows DESC;
```

| source_file | reason | rows |
|---|---|---|
| customers.csv | duplicate_customer_id | 5 |
| customers.csv | signup_date_in_future | 3 |
| orders.csv | product_id_not_in_master | 87 |
| orders.csv | customer_id_not_in_master | 18 |
| orders.csv | order_amount_formula_mismatch | 18 |
| orders.csv | customer_id_missing | 18 |
| orders.csv | duplicate_order_id | 12 |
| orders.csv | quantity_not_positive | 10 |
| orders.csv | delivery_minutes_missing | 8 |
| orders.csv | order_ts_in_future | 7 |
| orders.csv | delivery_minutes_outside_normal | 6 |
| orders.csv | order_ts_unparseable | 6 |
| orders.csv | delivery_minutes_over_120 | 6 |
| orders.csv | store_id_not_in_master | 5 |
| orders.csv | order_status_invalid | 3 |
| orders.csv | discount_pct_out_of_range | 3 |
| orders.csv | payment_mode_nonstandard | 1 |
| orders_2026-10-01_increment.csv | product_id_not_in_master | 10 |
| orders_2026-10-01_increment.csv | customer_id_not_in_master | 1 |
| payments.csv | amount_paid_not_positive | 10 |
| payments.csv | order_id_unknown | 8 |
| payments.csv | payment_ts_in_future | 7 |
| payments.csv | duplicate_payment_id | 4 |
| products.csv | cost_price_above_mrp | 5 |
| products.csv | duplicate_product_id | 2 |
| products.csv | mrp_not_positive | 1 |

ETL and ELT results identical (all 26 rows, every value): **yes**.

