"""
dags/nammamart_etl_priyanka.py
------------------------------
NammaMart ETL pipeline: Extract -> Validate -> Transform -> Load (pandas -> DuckDB).
Only rows that pass every Critical check reach the finance tables.

  extract_stores ────► validate_stores ─────┐
  extract_products ──► validate_products ───┼──► validate_orders ──┐
  extract_customers ─► validate_customers ──┘          ▲           │
  extract_orders ──────────────────────────────────────┤           ├──► quarantine ─┐
  extract_payments ─────────────────────► validate_payments ───────┘                │
                                                                    transform ◄─────┘
                                                                        │
                         load ──► reconcile ──► report ──► dq_trend_chart ──► audit

Schedule: 09:00 IST, Monday-Saturday. Logic lives in include/nammamart/etl_core.py.
Tables: etl_fact_orders, etl_fact_payments, etl_dim_customer, etl_dim_product,
etl_dim_store, etl_payment_reconciliation, etl_daily_zone_revenue,
etl_category_margin, etl_quarantine, dq_results, dq_trend, pipeline_runs.
Chart: include/reports/dq_trend.svg / .html (rebuilt from dq_results on every run).
"""

import sys
sys.path.insert(0, "/usr/local/airflow/include")

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, get_current_context

from nammamart import etl_core
from nammamart.alerts import audit_run, notify_failure
from nammamart.config import get_as_of

DAG_ID = "nammamart_etl_priyanka"

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
    tags=["nammamart", "etl", "priyanka"],
)
def nammamart_etl_priyanka():

    # ── EXTRACT (5 in parallel) ──────────────────────────────────────────────
    @task
    def extract_stores():
        return etl_core.extract("stores", _run_id())

    @task
    def extract_products():
        return etl_core.extract("products", _run_id())

    @task
    def extract_customers():
        return etl_core.extract("customers", _run_id())

    @task
    def extract_orders():
        return etl_core.extract("orders", _run_id())

    @task
    def extract_payments():
        return etl_core.extract("payments", _run_id())

    # ── VALIDATE (data quality rules, Python) ────────────────────────────────
    @task
    def validate_stores(meta):
        return etl_core.validate_stores(meta, _run_id())

    @task
    def validate_products(meta):
        return etl_core.validate_products(meta, _run_id())

    @task
    def validate_customers(meta):
        return etl_core.validate_customers(meta, _run_id(), get_as_of())

    @task
    def validate_orders(meta, stores_v, products_v, customers_v):
        return etl_core.validate_orders(meta, stores_v, products_v, customers_v, _run_id(), DAG_ID, get_as_of())

    @task
    def validate_payments(meta, orders_meta):
        return etl_core.validate_payments(meta, orders_meta, _run_id(), get_as_of())

    # ── QUARANTINE + TRANSFORM + LOAD ────────────────────────────────────────
    @task
    def quarantine(stores_v, products_v, customers_v, orders_v, payments_v):
        validated = {"stores": stores_v, "products": products_v, "customers": customers_v,
                     "orders": orders_v, "payments": payments_v}
        result = etl_core.quarantine(validated, _run_id(), DAG_ID)
        return {**result, "validated": validated}

    @task
    def transform(q):
        return etl_core.transform(q["validated"], _run_id())

    @task
    def load(paths):
        return etl_core.load(paths)

    @task
    def reconcile(_loaded):
        return etl_core.reconcile()

    @task
    def report(_reconciled):
        return etl_core.report()

    @task
    def dq_trend_chart(_reported):
        from nammamart.dq_trend import build_dq_trend
        return build_dq_trend()

    # ── RUN AUDIT: always runs, even when something upstream failed ──────────
    @task(trigger_rule="all_done", retries=0)
    def audit(upstream_ids):
        return audit_run(
            get_current_context(), upstream_ids,
            rows_extracted=lambda r: sum(r[t]["rows"] for t in r if t.startswith("extract_") and r[t]),
            rows_loaded=lambda r: r["load"]["rows_loaded"] if r.get("load") else 0,
            rows_quarantined=lambda r: r["quarantine"]["rows_quarantined"] if r.get("quarantine") else 0,
        )

    # ── WIRING ───────────────────────────────────────────────────────────────
    st, pr, cu, od, pa = extract_stores(), extract_products(), extract_customers(), extract_orders(), extract_payments()
    st_v, pr_v, cu_v = validate_stores(st), validate_products(pr), validate_customers(cu)
    od_v = validate_orders(od, st_v, pr_v, cu_v)
    pa_v = validate_payments(pa, od)
    q = quarantine(st_v, pr_v, cu_v, od_v, pa_v)
    final = dq_trend_chart(report(reconcile(load(transform(q)))))

    upstream = ["extract_stores", "extract_products", "extract_customers", "extract_orders", "extract_payments",
                "validate_stores", "validate_products", "validate_customers", "validate_orders",
                "validate_payments", "quarantine", "transform", "load", "reconcile", "report", "dq_trend_chart"]
    audit_task = audit(upstream)
    # all_done: the audit waits for every task to finish (success, failed or upstream_failed)
    [st, pr, cu, od, pa, st_v, pr_v, cu_v, od_v, pa_v, q, final] >> audit_task


nammamart_etl_priyanka()
