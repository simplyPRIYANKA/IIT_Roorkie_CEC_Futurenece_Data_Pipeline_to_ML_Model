"""
include/nammamart/db.py
-----------------------
DuckDB helpers shared by both pipelines: connection handling, the audit tables
(dq_results, pipeline_runs, dq_check_catalog) and the quarantine tables.

DuckDB allows ONE writing process per database file. Airflow runs parallel tasks
as separate processes (and both DAGs are scheduled for 09:00), so connect()
retries for a while when the file is locked instead of failing the task.
"""

import contextlib
import random
import time

import duckdb
import pandas as pd

from nammamart.config import WAREHOUSE_DB
from nammamart.dq_rules import catalog_rows

LOCK_TIMEOUT_SECONDS = 180


@contextlib.contextmanager
def connect(db_path=WAREHOUSE_DB):
    deadline = time.time() + LOCK_TIMEOUT_SECONDS
    while True:
        try:
            con = duckdb.connect(db_path)
            break
        except duckdb.IOException as e:
            if "lock" not in str(e).lower() or time.time() > deadline:
                raise
            print("[DuckDB] Warehouse is locked by another task, waiting...")
            time.sleep(1 + random.random() * 2)
    try:
        yield con
    finally:
        con.close()


AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,     -- PASS / WARN / FAIL
    checked_at    TIMESTAMP
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR,  -- SUCCESS / FAILED
    failed_tasks      VARCHAR
);
"""

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    run_id          VARCHAR,
    dag_id          VARCHAR,
    source_file     VARCHAR,
    source_row      BIGINT,     -- 1-based data row number in the source file
    record_key      VARCHAR,
    failure_reason  VARCHAR,
    raw_record      VARCHAR,    -- JSON of the row as received (PII masked)
    quarantined_at  TIMESTAMP
)
"""


def ensure_audit_tables(con):
    con.execute(AUDIT_DDL)
    catalog = pd.DataFrame(catalog_rows())
    con.execute("CREATE OR REPLACE TABLE dq_check_catalog AS SELECT * FROM catalog")


def status_for(severity, rows_failed):
    if rows_failed == 0:
        return "PASS"
    return "FAIL" if severity == "Critical" else "WARN"


def write_dq_results(con, run_id, dag_id, results):
    """results: list of dicts with check_name, dimension, severity, rows_checked, rows_failed."""
    ensure_audit_tables(con)
    df = pd.DataFrame(results)
    df["run_id"] = run_id
    df["dag_id"] = dag_id
    df["status"] = [status_for(s, f) for s, f in zip(df["severity"], df["rows_failed"])]
    df["checked_at"] = pd.Timestamp.utcnow().tz_localize(None)
    # Re-running (Clear) the same run replaces its results instead of duplicating them.
    con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
    con.execute("INSERT INTO dq_results BY NAME SELECT * FROM df")


def previous_rows_checked(con, dag_id, run_id, check_name):
    """Rows checked by the same check in the latest earlier run of this DAG (for volume checks)."""
    con.execute(AUDIT_DDL)
    row = con.execute(
        """SELECT rows_checked FROM dq_results
           WHERE dag_id = ? AND run_id <> ? AND check_name = ?
           ORDER BY checked_at DESC LIMIT 1""",
        [dag_id, run_id, check_name],
    ).fetchone()
    return row[0] if row else None


def write_pipeline_run(con, row: dict):
    con.execute(AUDIT_DDL)
    df = pd.DataFrame([row])
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [row["run_id"], row["dag_id"]])
    con.execute("INSERT INTO pipeline_runs BY NAME SELECT * FROM df")
