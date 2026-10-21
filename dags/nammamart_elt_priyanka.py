"""
dags/nammamart_elt_priyanka.py
------------------------------
NammaMart ELT pipeline: Extract -> Load raw -> Transform in DuckDB with SQL.
Every raw row is kept exactly as received; bad rows are filtered out in staging.

  RAW (parallel)           STAGING (SQL)                       QUALITY        MARTS (parallel)
  load_raw_stores ───────► stg_stores ────┐
  load_raw_products ─────► stg_products ──┼─► stg_orders_merge ─► stg_orders_merge_increments ─┐
  load_raw_customers ────► stg_customers ─┘        ▲                       ▲                    │
  load_raw_orders ─────────────────────────────────┘                       │                    ├─► dq_checks ─► mart_* x5
  load_raw_order_increments ───────────────────────────────────────────────┘                    │      ─► compare_with_etl
  load_raw_payments ───────────────────────────────────────────────► stg_payments ──────────────┘      ─► dq_trend_chart
                                                                                                         ─► audit

Schedule: 09:00 IST, Monday-Saturday. SQL lives in include/sql/{raw,staging,quality,marts}.
Chart: include/reports/dq_trend.svg / .html (rebuilt from dq_results on every run).
"""

import sys
sys.path.insert(0, "/usr/local/airflow/include")

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context

from nammamart import elt_core
from nammamart.alerts import audit_run, notify_failure
from nammamart.config import get_as_of

DAG_ID = "nammamart_elt_priyanka"
MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers"]

default_args = {
    "owner": "priyanka",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": notify_failure,
}


def _run_id():
    return get_current_context()["run_id"]


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                   # 09:00 Mon-Sat ...
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),  # ... in IST, not UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=__doc__,
    tags=["nammamart", "elt", "priyanka"],
)
def nammamart_elt_priyanka():

    # ── EXTRACT + LOAD RAW (parallel, all text, append-only) ─────────────────
    @task
    def load_raw_stores():
        return elt_core.load_raw("stores", _run_id())

    @task
    def load_raw_products():
        return elt_core.load_raw("products", _run_id())

    @task
    def load_raw_customers():
        return elt_core.load_raw("customers", _run_id())

    @task
    def load_raw_orders():
        return elt_core.load_raw("orders", _run_id())

    @task
    def load_raw_order_increments():
        return elt_core.load_raw_increments(_run_id())

    @task
    def load_raw_payments():
        return elt_core.load_raw("payments", _run_id())

    # ── STAGING (SQL) ────────────────────────────────────────────────────────
    @task
    def stg_stores(_raw):
        return elt_core.build_staging("stg_stores", _run_id(), get_as_of())

    @task
    def stg_products(_raw):
        return elt_core.build_staging("stg_products", _run_id(), get_as_of())

    @task
    def stg_customers(_raw):
        return elt_core.build_staging("stg_customers", _run_id(), get_as_of())

    @task
    def stg_orders_merge(_raw, _stores, _products, _customers):
        return elt_core.merge_orders(_run_id(), get_as_of(), "orders.csv")

    @task
    def stg_orders_merge_increments(_raw_increments, _merged):
        return elt_core.merge_order_increments(_run_id(), get_as_of())

    @task
    def stg_payments(_raw, _raw_orders, _raw_increments):
        return elt_core.build_staging("stg_payments", _run_id(), get_as_of())

    # ── QUALITY: dq_results + elt_quarantine ─────────────────────────────────
    @task
    def dq_checks(_orders, _payments):
        return elt_core.quality(_run_id(), DAG_ID, get_as_of())

    # ── MARTS (SQL, parallel) ────────────────────────────────────────────────
    @task
    def build_mart(table, _dq):
        return elt_core.build_mart(table)

    @task
    def compare_with_etl(_marts):
        return elt_core.compare_with_etl()

    @task
    def dq_trend_chart(_compared):
        from nammamart.dq_trend import build_dq_trend
        return build_dq_trend()

    # ── RUN AUDIT ────────────────────────────────────────────────────────────
    @task(trigger_rule="all_done", retries=0)
    def audit(upstream_ids):
        run_id = _run_id()

        def rows_extracted(r):
            raw = [r[t] for t in upstream_ids if t.startswith("load_raw_") and r.get(t)]
            return sum(x["rows"] if isinstance(x, dict) else sum(i["rows"] for i in x) for x in raw)

        def rows_loaded(r):
            return elt_core.rows_passed_this_run(run_id) if r.get("dq_checks") else 0

        return audit_run(
            get_current_context(), upstream_ids, rows_extracted, rows_loaded,
            rows_quarantined=lambda r: r["dq_checks"]["rows_quarantined"] if r.get("dq_checks") else 0,
        )

    # ── WIRING ───────────────────────────────────────────────────────────────
    r_st, r_pr, r_cu = load_raw_stores(), load_raw_products(), load_raw_customers()
    r_od, r_inc, r_pa = load_raw_orders(), load_raw_order_increments(), load_raw_payments()

    s_st, s_pr, s_cu = stg_stores(r_st), stg_products(r_pr), stg_customers(r_cu)
    merged = stg_orders_merge(r_od, s_st, s_pr, s_cu)
    merged_inc = stg_orders_merge_increments(r_inc, merged)
    s_pa = stg_payments(r_pa, r_od, r_inc)

    dq = dq_checks(merged_inc, s_pa)
    marts = [build_mart.override(task_id=table)(table, dq) for table in MARTS]
    compared = compare_with_etl(marts)
    charted = dq_trend_chart(compared)

    upstream = ["load_raw_stores", "load_raw_products", "load_raw_customers", "load_raw_orders",
                "load_raw_order_increments", "load_raw_payments", "stg_stores", "stg_products",
                "stg_customers", "stg_orders_merge", "stg_orders_merge_increments", "stg_payments",
                "dq_checks", *MARTS, "compare_with_etl", "dq_trend_chart"]
    audit_task = audit(upstream)
    [r_st, r_pr, r_cu, r_od, r_inc, r_pa, s_st, s_pr, s_cu, merged, merged_inc, s_pa, dq, compared, charted] >> audit_task


nammamart_elt_priyanka()
