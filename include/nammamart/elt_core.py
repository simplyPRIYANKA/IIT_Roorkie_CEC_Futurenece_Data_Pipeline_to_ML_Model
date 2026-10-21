"""
include/nammamart/elt_core.py
-----------------------------
ELT pipeline runner: Extract -> Load raw -> Transform in DuckDB with SQL.
Python here only checks the file exists, sets SQL variables and runs the .sql
files in include/sql/. Every transformation is SQL.
"""

import os

from nammamart import config as cfg
from nammamart.db import QUARANTINE_DDL, connect, ensure_audit_tables

MACROS = os.path.join(cfg.SQL_DIR, "staging", "_macros.sql")


def run_sql(relative_path, **variables):
    """Execute a .sql file with DuckDB variables (read in SQL via getvariable('name'))."""
    path = os.path.join(cfg.SQL_DIR, relative_path)
    with open(path, encoding="utf-8") as f:
        sql = f.read()
    with connect() as con:
        for name, value in variables.items():
            con.execute(f"SET VARIABLE {name} = ?", [value])
        with open(MACROS, encoding="utf-8") as f:
            con.execute(f.read())
        print(f"[SQL] running {relative_path} with {variables}")
        result = con.execute(sql)
        try:
            return result.fetchall()
        except Exception:
            return None


def _check_header(path, expected):
    """Fail loudly on a schema change before anything is appended to raw."""
    with open(path, encoding="utf-8") as f:
        header = f.readline().strip().split(",")
    missing = [c for c in expected if c not in header]
    unexpected = [c for c in header if c not in expected]
    if missing or unexpected:
        raise ValueError(f"[Load raw] Schema change in {os.path.basename(path)}: "
                         f"missing={missing} unexpected={unexpected}")


def load_raw(file_key, run_id, source_file=None):
    """Append one source file to raw_<file_key>, all columns as text."""
    fname, _, expected = cfg.SOURCES[file_key]
    source_file = source_file or fname
    path = os.path.join(cfg.DATA_DIR, source_file)
    if not os.path.exists(path):
        raise FileNotFoundError(f"[Load raw] Source file missing: {path}")
    _check_header(path, expected)
    run_sql(f"raw/raw_{file_key}.sql", run_id=run_id, source_file=source_file, source_path=path)
    with connect() as con:
        n = con.execute(f"SELECT count(*) FROM raw_{file_key} WHERE _run_id = ? AND _source_file = ?",
                        [run_id, source_file]).fetchone()[0]
        total = con.execute(f"SELECT count(*) FROM raw_{file_key}").fetchone()[0]
    with open(path, encoding="utf-8", newline=None) as f:   # universal newlines: CRLF or LF
        lines = sum(1 for line in f if line.strip()) - 1
    if n != lines:
        raise ValueError(f"[Load raw] {source_file}: file has {lines} data lines but {n} rows loaded")
    print(f"[Load raw] {source_file}: {n} rows appended to raw_{file_key} ({total} rows in table)")
    return {"file": source_file, "rows": n}


def load_raw_increments(run_id):
    """Incremental feed: append every next-day order file present in the data folder."""
    files = cfg.increment_files()
    if not files:
        print("[Load raw] No increment files present.")
    return [load_raw("orders", run_id, f) for f in files]


def build_staging(table, run_id, as_of):
    run_sql(f"staging/{table}.sql", run_id=run_id, as_of=as_of)
    with connect() as con:
        counts = dict(con.execute(f"SELECT dq_status, count(*) FROM {table} GROUP BY 1").fetchall())
    print(f"[Staging] {table}: {counts}")
    return {"table": table, **counts}


def merge_orders(run_id, as_of, source_file):
    """MERGE one order file into stg_orders (orders.csv first, then increments in date order)."""
    actions = run_sql("staging/stg_orders_merge.sql", run_id=run_id, as_of=as_of, source_file=source_file) or []
    inserted = sum(1 for a in actions if a[0] == "INSERT")
    updated = sum(1 for a in actions if a[0] == "UPDATE")
    with connect() as con:
        checked = dict(con.execute(
            "SELECT dq_status, count(*) FROM stg_orders_checked WHERE _run_id = ? AND _source_file = ? GROUP BY 1",
            [run_id, source_file]).fetchall())
        total = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
    print(f"[Staging] MERGE {source_file} into stg_orders: {inserted} inserted, {updated} updated "
          f"-> {total} rows. Batch check result: {checked}")
    return {"file": source_file, "inserted": inserted, "updated": updated, "checked": checked}


def merge_order_increments(run_id, as_of):
    return [merge_orders(run_id, as_of, f) for f in cfg.increment_files()]


def quality(run_id, dag_id, as_of):
    """Write dq_results + elt_quarantine for this run."""
    with connect() as con:
        ensure_audit_tables(con)
        con.execute(QUARANTINE_DDL.format(table="elt_quarantine"))
    run_sql("quality/dq_results.sql", run_id=run_id, dag_id=dag_id, as_of=as_of)
    run_sql("quality/elt_quarantine.sql", run_id=run_id, dag_id=dag_id)
    with connect() as con:
        issues = con.execute(
            """SELECT check_name, severity, rows_checked, rows_failed, status FROM dq_results
               WHERE run_id = ? AND dag_id = ? AND rows_failed > 0 ORDER BY check_name""",
            [run_id, dag_id]).df()
        quarantined = con.execute(
            "SELECT source_file, count(*) AS n FROM elt_quarantine WHERE run_id = ? AND dag_id = ? GROUP BY 1",
            [run_id, dag_id]).fetchall()
    print("[Quality] Checks with failures:\n" + issues.to_string(index=False))
    print(f"[Quality] Quarantined rows by file: {dict(quarantined)}")
    return {"rows_quarantined": sum(n for _, n in quarantined)}


def build_mart(table):
    run_sql(f"marts/{table}.sql")
    with connect() as con:
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    print(f"[Mart] {table}: {n} rows")
    return {"table": table, "rows": n}


def compare_with_etl():
    """Part B4: ELT mart totals must match the ETL fact table."""
    with connect() as con:
        elt = con.execute("""SELECT round(sum(net_revenue), 2), sum(delivered_orders), round(sum(gross_margin), 2)
                             FROM mart_daily_store_kpis""").fetchone()
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if "etl_fact_orders" not in tables:
            print("[Compare] ETL tables not built yet, nothing to compare.")
            return {"elt_net_revenue": elt[0]}
        etl = con.execute("""SELECT round(sum(net_revenue), 2), count(*) FILTER (WHERE order_status = 'DELIVERED'),
                                    round(sum(gross_margin), 2)
                             FROM etl_fact_orders""").fetchone()
    print(f"[Compare] net revenue  ETL={etl[0]:,.2f}  ELT={elt[0]:,.2f}  match={etl[0] == elt[0]}")
    print(f"[Compare] delivered    ETL={etl[1]}  ELT={elt[1]}  match={etl[1] == elt[1]}")
    print(f"[Compare] gross margin ETL={etl[2]:,.2f}  ELT={elt[2]:,.2f}  match={etl[2] == elt[2]}")
    return {"etl_net_revenue": etl[0], "elt_net_revenue": elt[0], "match": etl[0] == elt[0] and etl[1] == elt[1]}


def rows_passed_this_run(run_id):
    with connect() as con:
        return con.execute("""
            SELECT (SELECT count(*) FROM stg_orders_checked WHERE _run_id = $r AND dq_status <> 'FAIL')
                 + (SELECT count(*) FROM stg_payments  WHERE _run_id = $r AND dq_status <> 'FAIL')
                 + (SELECT count(*) FROM stg_customers WHERE _run_id = $r AND dq_status <> 'FAIL')
                 + (SELECT count(*) FROM stg_products  WHERE _run_id = $r AND dq_status <> 'FAIL')
                 + (SELECT count(*) FROM stg_stores    WHERE _run_id = $r AND dq_status <> 'FAIL')
        """, {"r": run_id}).fetchone()[0]
