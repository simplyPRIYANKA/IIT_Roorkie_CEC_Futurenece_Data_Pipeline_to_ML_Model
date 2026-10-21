"""
include/nammamart/etl_core.py
-----------------------------
ETL pipeline logic: Extract -> Validate -> Transform -> Load, all in pandas.
Bad rows are rejected BEFORE anything reaches the warehouse.

The DAG (dags/nammamart_etl_priyanka.py) calls these functions; they can also be
run locally (see include/nammamart/run_local.py). Intermediate data is passed
between tasks as pickle files under include/tmp/<run>/, XCom only carries metadata.
"""

import json
import os

import pandas as pd

from nammamart import config as cfg
from nammamart.db import connect, previous_rows_checked, write_dq_results, QUARANTINE_DDL
from nammamart.dq_rules import RULES, SEVERITY, CRITICAL
from nammamart.masking import EMAIL_RE, PHONE_RE, PINCODE_RE, mask_email, mask_name, mask_phone, mask_record

FILE_ORDER = ["stores", "products", "customers", "orders", "payments"]


# ── helpers ─────────────────────────────────────────────────────────────────

def _run_dir(run_id):
    path = os.path.join(cfg.TMP_DIR, f"etl_{cfg.LEARNER}", cfg.safe_run_id(run_id))
    os.makedirs(path, exist_ok=True)
    return path


def _save(df, run_id, name):
    path = os.path.join(_run_dir(run_id), f"{name}.pkl")
    df.to_pickle(path)
    return path


def _txt(df, col):
    return df[col].fillna("").astype(str).str.strip()


def _num(df, col):
    return pd.to_numeric(_txt(df, col), errors="coerce")


def _ts(df, col, fmt=cfg.TS_FORMAT):
    return pd.to_datetime(_txt(df, col), format=fmt, errors="coerce")


def _std_payment_mode(df):
    raw = _txt(df, "payment_mode")
    return raw.str.lower().map(cfg.PAYMENT_MODE_MAP).fillna(raw)


def _first_occurrence_dupes(df, key):
    """True for the 2nd, 3rd... copy of a key within the same source file (first row is kept)."""
    k = _txt(df, key)
    return df.assign(_k=k).duplicated(["_source_file", "_k"], keep="first")


def _apply_checks(df, file_key, masks):
    """Turn {check_name: boolean mask} into dq_errors / dq_warnings / dq_status / failure_reason."""
    errors = [[] for _ in range(len(df))]
    warnings = [[] for _ in range(len(df))]
    for name, mask in masks.items():
        target = errors if SEVERITY[(file_key, name)] == CRITICAL else warnings
        for i in mask.fillna(False).to_numpy().nonzero()[0]:
            target[i].append(name)
    df = df.copy()
    df["dq_errors"] = errors
    df["dq_warnings"] = warnings
    df["dq_status"] = ["FAIL" if e else ("WARN" if w else "PASS") for e, w in zip(errors, warnings)]
    df["failure_reason"] = ["; ".join(e + w) or None for e, w in zip(errors, warnings)]
    return df


def _check_results(file_key, masks, rows_checked, dataset_failures=None):
    """One dq_results row per rule in the catalogue for this file (0 failures included)."""
    dataset_failures = dataset_failures or {}
    out = []
    for f, name, dim, sev, _, dataset_level in RULES:
        if f != file_key:
            continue
        failed = dataset_failures.get(name, 0) if dataset_level else int(masks[name].fillna(False).sum())
        out.append({"check_name": f"{f}.{name}", "dimension": dim, "severity": sev,
                    "rows_checked": int(rows_checked), "rows_failed": int(failed)})
    return out


def _split(df, file_key, run_id, masks, rows_checked, dataset_failures=None):
    checked = _apply_checks(df, file_key, masks)
    passed = checked[checked["dq_status"] != "FAIL"]
    failed = checked[checked["dq_status"] == "FAIL"]
    results = _check_results(file_key, masks, rows_checked, dataset_failures)
    print(f"[Validate] {file_key}: {len(df)} rows -> {len(passed)} passed "
          f"({(checked['dq_status'] == 'WARN').sum()} with warnings), {len(failed)} failed")
    for r in results:
        if r["rows_failed"]:
            print(f"    {r['severity']:<8} {r['check_name']:<45} {r['rows_failed']}")
    return {
        "file": file_key,
        "checked_path": _save(checked, run_id, f"checked_{file_key}"),
        "rows_in": len(df), "rows_passed": len(passed), "rows_failed": len(failed),
        "checks": results,
    }


# ── EXTRACT ─────────────────────────────────────────────────────────────────

def extract(file_key, run_id):
    """Read a source CSV exactly as text. orders also picks up next-day increment files."""
    fname, _, expected = cfg.SOURCES[file_key]
    files = [fname] + (cfg.increment_files() if file_key == "orders" else [])
    frames = []
    for seq, f in enumerate(files):
        path = os.path.join(cfg.DATA_DIR, f)
        if not os.path.exists(path):
            raise FileNotFoundError(f"[Extract] Source file missing: {path}")
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        missing = [c for c in expected if c not in df.columns]
        unexpected = [c for c in df.columns if c not in expected]
        if missing or unexpected:
            # Fail loudly: a renamed column must never load as silently-empty data.
            raise ValueError(f"[Extract] Schema change in {f}: missing={missing} unexpected={unexpected}")
        df = df[expected]
        df["_source_file"] = f
        df["_source_row"] = range(1, len(df) + 1)
        df["_file_seq"] = seq
        frames.append(df)
        print(f"[Extract] {f}: {len(df)} rows")
    df = pd.concat(frames, ignore_index=True)
    return {"file": file_key, "path": _save(df, run_id, f"extract_{file_key}"), "rows": len(df),
            "files": {f: int((df["_source_file"] == f).sum()) for f in files}}


# ── VALIDATE ────────────────────────────────────────────────────────────────

def validate_stores(meta, run_id):
    df = pd.read_pickle(meta["path"])
    zone = _txt(df, "zone").str.capitalize()
    cap = _num(df, "capacity_orders_per_day")
    masks = {
        "zone_missing": zone == "",
        "duplicate_store_id": _first_occurrence_dupes(df, "store_id"),
        "zone_invalid": (zone != "") & ~zone.isin(cfg.ZONES),
        "capacity_not_positive": cap.isna() | (cap <= 0),
    }
    return _split(df, "stores", run_id, masks, len(df))


def validate_products(meta, run_id):
    df = pd.read_pickle(meta["path"])
    mrp, cost = _num(df, "mrp"), _num(df, "cost_price")
    masks = {
        "category_missing": _txt(df, "category") == "",
        "duplicate_product_id": _first_occurrence_dupes(df, "product_id"),
        "mrp_not_positive": mrp.isna() | (mrp <= 0),
        "cost_price_above_mrp": cost > mrp,
    }
    return _split(df, "products", run_id, masks, len(df))


def validate_customers(meta, run_id, as_of):
    df = pd.read_pickle(meta["path"])
    tier_raw = df["loyalty_tier"].fillna("")  # untrimmed: "Gold " counts as nonstandard
    tier = tier_raw.str.strip().str.capitalize()
    signup = _ts(df, "signup_date", cfg.DATE_FORMAT)
    masks = {
        "customer_id_missing": _txt(df, "customer_id") == "",
        "city_missing": _txt(df, "city") == "",
        "duplicate_customer_id": _first_occurrence_dupes(df, "customer_id"),
        "email_invalid": ~_txt(df, "email").str.match(EMAIL_RE.pattern),
        "phone_invalid": ~_txt(df, "phone").str.match(PHONE_RE.pattern),
        "pincode_invalid": ~_txt(df, "pincode").str.match(PINCODE_RE.pattern),
        "signup_date_unparseable": signup.isna(),
        "loyalty_tier_invalid": ~tier.isin(cfg.LOYALTY_TIERS),
        "loyalty_tier_nonstandard_case": tier.isin(cfg.LOYALTY_TIERS) & (tier_raw != tier),
        "signup_date_in_future": signup > pd.Timestamp(as_of).normalize(),
    }
    return _split(df, "customers", run_id, masks, len(df))


def _valid_keys(meta, key):
    df = pd.read_pickle(meta["checked_path"])
    return df[df["dq_status"] != "FAIL"].assign(_k=_txt(df, key))


def validate_orders(meta, stores_v, products_v, customers_v, run_id, dag_id, as_of):
    df = pd.read_pickle(meta["path"])
    stores = _valid_keys(stores_v, "store_id")
    valid_stores = set(stores["_k"])
    capacity_by_store = dict(zip(stores["_k"], _num(stores, "capacity_orders_per_day")))
    valid_customers = set(_valid_keys(customers_v, "customer_id")["_k"])
    products = _valid_keys(products_v, "product_id")
    mrp_by_product = dict(zip(products["_k"], _num(products, "mrp")))

    cust, prod, store = _txt(df, "customer_id"), _txt(df, "product_id"), _txt(df, "store_id")
    ts = _ts(df, "order_ts")
    status_raw = df["order_status"].fillna("")
    status = status_raw.str.strip().str.upper()
    mode_raw = df["payment_mode"].fillna("")
    mode = _std_payment_mode(df)
    q, up, dp, amt, dm = (_num(df, c) for c in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"])
    expected = q * up * (1 - dp / 100)
    lo, hi = cfg.NORMAL_DELIVERY_RANGE
    as_of_ts = pd.Timestamp(as_of)
    # Orders per store per day within each file, vs that store's daily capacity.
    # Capacity <= 0 is already flagged by stores.capacity_not_positive, so it is skipped here.
    store_day_orders = (df.assign(_s=store, _d=ts.dt.date)
                        .groupby(["_source_file", "_s", "_d"], dropna=True)["_s"].transform("size"))
    capacity = store.map(capacity_by_store)

    masks = {
        "customer_id_missing": cust == "",
        "delivery_minutes_missing": (status == "DELIVERED") & dm.isna(),
        "duplicate_order_id": _first_occurrence_dupes(df, "order_id"),
        "order_ts_unparseable": ts.isna(),
        "order_status_invalid": ~status.isin(cfg.ORDER_STATUSES),
        "payment_mode_invalid": ~mode.isin(cfg.PAYMENT_MODES),
        "discount_pct_out_of_range": dp.isna() | (dp < 0) | (dp > 100),
        "order_status_nonstandard": status.isin(cfg.ORDER_STATUSES) & (status_raw != status),
        "payment_mode_nonstandard": mode.isin(cfg.PAYMENT_MODES) & (mode_raw != mode),
        "quantity_not_positive": q.isna() | (q <= 0),
        "order_amount_formula_mismatch": expected.isna() | amt.isna() | ((expected - amt).abs() > cfg.AMOUNT_TOLERANCE),
        "delivery_minutes_over_120": dm >= cfg.MAX_DELIVERY_MINUTES,
        "delivery_minutes_outside_normal": dm.notna() & ((dm < lo) | (dm > hi)),
        "unit_price_above_mrp": up > prod.map(mrp_by_product),
        "customer_id_not_in_master": (cust != "") & ~cust.isin(valid_customers),
        "product_id_not_in_master": ~prod.isin(mrp_by_product.keys()),
        "store_id_not_in_master": ~store.isin(valid_stores),
        "store_over_capacity": (capacity > 0) & (store_day_orders > capacity),
        "order_ts_in_future": ts > as_of_ts,
    }

    # Dataset-level checks: freshness and volume.
    latest = ts[ts <= as_of_ts].max()
    stale = pd.isna(latest) or (as_of_ts - latest) > pd.Timedelta(hours=cfg.STALE_AFTER_HOURS)
    with connect() as con:
        prev = previous_rows_checked(con, dag_id, run_id, "orders.volume_drop_vs_previous_run")
    dropped = prev is not None and len(df) < prev * (1 - cfg.VOLUME_DROP_THRESHOLD)
    print(f"[Validate] orders freshness: latest order_ts={latest}, as_of={as_of}, stale={stale}")
    print(f"[Validate] orders volume: this run={len(df)}, previous run={prev}, drop>90%={dropped}")

    return _split(df, "orders", run_id, masks, len(df),
                  {"stale_data": int(stale), "volume_drop_vs_previous_run": int(dropped)})


def validate_payments(meta, orders_meta, run_id, as_of):
    df = pd.read_pickle(meta["path"])
    known_orders = set(_txt(pd.read_pickle(orders_meta["path"]), "order_id"))
    ts = _ts(df, "payment_ts")
    mode_raw = df["payment_mode"].fillna("")
    mode = _std_payment_mode(df)
    amount = _num(df, "amount_paid")
    masks = {
        "gateway_ref_missing": (mode != "Cash on Delivery") & (_txt(df, "gateway_ref") == ""),
        "duplicate_payment_id": _first_occurrence_dupes(df, "payment_id"),
        "payment_ts_unparseable": ts.isna(),
        "payment_status_invalid": ~_txt(df, "payment_status").str.upper().isin(cfg.PAYMENT_STATUSES),
        "payment_mode_invalid": ~mode.isin(cfg.PAYMENT_MODES),
        "payment_mode_nonstandard": mode.isin(cfg.PAYMENT_MODES) & (mode_raw != mode),
        "amount_paid_not_positive": amount.isna() | (amount <= 0),
        "order_id_unknown": ~_txt(df, "order_id").isin(known_orders),
        "payment_ts_in_future": ts > pd.Timestamp(as_of),
    }
    return _split(df, "payments", run_id, masks, len(df))


# ── QUARANTINE ──────────────────────────────────────────────────────────────

def quarantine(validated: dict, run_id, dag_id):
    """Write failed rows to CSV (+ etl_quarantine table) with a failure_reason, and store dq_results."""
    os.makedirs(cfg.QUARANTINE_DIR, exist_ok=True)
    tag = cfg.safe_run_id(run_id)
    frames, written = [], {}
    for file_key in FILE_ORDER:
        meta = validated[file_key]
        checked = pd.read_pickle(meta["checked_path"])
        failed = checked[checked["dq_status"] == "FAIL"]
        if failed.empty:
            continue
        _, key, cols = cfg.SOURCES[file_key]
        # PII is masked even here: quarantine files are shared with ops, the
        # untouched original is always reachable via _source_file + _source_row.
        masked = pd.DataFrame([mask_record(r) for r in failed[cols].to_dict("records")], index=failed.index)
        out = masked.assign(failure_reason=failed["failure_reason"],
                            _source_file=failed["_source_file"], _source_row=failed["_source_row"])
        path = os.path.join(cfg.QUARANTINE_DIR, f"{file_key}_failed_{tag}.csv")
        out.to_csv(path, index=False)
        written[file_key] = path
        print(f"[Quarantine] {len(out)} {file_key} rows -> {path}")
        frames.append(pd.DataFrame({
            "run_id": run_id, "dag_id": dag_id,
            "source_file": failed["_source_file"], "source_row": failed["_source_row"],
            "record_key": failed[key], "failure_reason": failed["failure_reason"],
            "raw_record": [json.dumps(r) for r in masked.to_dict("records")],
            "quarantined_at": pd.Timestamp.utcnow().tz_localize(None),
        }))

    results = [r for f in FILE_ORDER for r in validated[f]["checks"]]
    with connect() as con:
        con.execute(QUARANTINE_DDL.format(table="etl_quarantine"))
        con.execute("DELETE FROM etl_quarantine WHERE run_id = ? AND dag_id = ?", [run_id, dag_id])
        if frames:
            q = pd.concat(frames, ignore_index=True)
            con.execute("INSERT INTO etl_quarantine BY NAME SELECT * FROM q")
        write_dq_results(con, run_id, dag_id, results)
    total = sum(validated[f]["rows_failed"] for f in FILE_ORDER)
    print(f"[Quarantine] total quarantined rows: {total}; dq_results rows written: {len(results)}")
    return {"files": written, "rows_quarantined": total}


# ── TRANSFORM ───────────────────────────────────────────────────────────────

def _passed(meta):
    df = pd.read_pickle(meta["checked_path"])
    return df[df["dq_status"] != "FAIL"].copy()


def transform(validated: dict, run_id):
    # Dimensions: standardise, (already de-duplicated by validation), mask PII.
    st = _passed(validated["stores"])
    dim_store = pd.DataFrame({
        "store_id": _txt(st, "store_id"), "store_name": _txt(st, "store_name"),
        "zone": _txt(st, "zone").str.capitalize().replace("", "Unknown"),
        "area": _txt(st, "area"), "pincode": _txt(st, "pincode"),
        "open_date": _ts(st, "open_date", cfg.DATE_FORMAT).dt.date,
        "manager_name": _txt(st, "manager_name"),
        "capacity_orders_per_day": _num(st, "capacity_orders_per_day").astype("Int64"),
        "dq_warnings": st["failure_reason"],
    })

    pr = _passed(validated["products"])
    dim_product = pd.DataFrame({
        "product_id": _txt(pr, "product_id"), "product_name": _txt(pr, "product_name"),
        "category": _txt(pr, "category").replace("", "Uncategorised"),
        "sub_category": _txt(pr, "sub_category"), "brand": _txt(pr, "brand"),
        "mrp": _num(pr, "mrp"), "cost_price": _num(pr, "cost_price"), "unit": _txt(pr, "unit"),
        "is_perishable": _txt(pr, "is_perishable").str.lower() == "true",
        "launch_date": _ts(pr, "launch_date", cfg.DATE_FORMAT).dt.date,
        "dq_warnings": pr["failure_reason"],
    })

    cu = _passed(validated["customers"])
    dim_customer = pd.DataFrame({
        "customer_id": _txt(cu, "customer_id"),
        "customer_name_masked": cu["full_name"].map(mask_name),
        "email_masked": cu["email"].map(mask_email),
        "phone_masked": cu["phone"].map(mask_phone),
        "city": _txt(cu, "city").replace("", None), "area": _txt(cu, "area"),
        "pincode": _txt(cu, "pincode").where(_txt(cu, "pincode").str.match(PINCODE_RE.pattern)),
        "signup_date": _ts(cu, "signup_date", cfg.DATE_FORMAT).dt.date,
        "loyalty_tier": _txt(cu, "loyalty_tier").str.capitalize(),
        "is_active": _txt(cu, "is_active").str.upper() == "Y",
        "dq_warnings": cu["failure_reason"],
    })

    # Orders: the latest version of each order wins (increment files override orders.csv).
    od = pd.read_pickle(validated["orders"]["checked_path"])
    od = od[~od["dq_errors"].map(lambda e: "duplicate_order_id" in e)]
    od = od.assign(_k=_txt(od, "order_id")).sort_values(["_file_seq", "_source_row"])
    latest = od.drop_duplicates("_k", keep="last")
    batch_order_ids = sorted(latest["_k"])
    good = latest[latest["dq_status"] != "FAIL"].copy()
    superseded = len(od) - len(latest)

    status = _txt(good, "order_status").str.upper()
    q, amt, dm = _num(good, "quantity"), _num(good, "order_amount"), _num(good, "delivery_minutes")
    ts = _ts(good, "order_ts")
    # Join orders with the three clean dimensions. validate="many_to_one" fails loudly if a
    # dimension key is duplicated; the inner-join count proves every order has all three.
    keys = pd.DataFrame({c: _txt(good, c) for c in ["customer_id", "product_id", "store_id"]}, index=good.index)
    joined = (keys.reset_index(names="_row")
              .merge(dim_customer[["customer_id", "loyalty_tier"]], on="customer_id", how="inner", validate="many_to_one")
              .merge(dim_product[["product_id", "category", "cost_price"]], on="product_id", how="inner", validate="many_to_one")
              .merge(dim_store[["store_id", "zone"]], on="store_id", how="inner", validate="many_to_one")
              .set_index("_row").reindex(good.index))
    if joined["store_id"].isna().any():
        raise ValueError(f"[Transform] {joined['store_id'].isna().sum()} orders did not join to all dimensions")
    cost = joined["cost_price"]
    delivered = status == "DELIVERED"
    fact = pd.DataFrame({
        "order_id": good["_k"], "order_ts": ts, "order_date": ts.dt.date,
        "customer_id": _txt(good, "customer_id"), "store_id": _txt(good, "store_id"),
        "product_id": _txt(good, "product_id"), "quantity": q.astype("Int64"),
        "unit_price": _num(good, "unit_price"), "discount_pct": _num(good, "discount_pct"),
        "order_amount": amt, "payment_mode": _std_payment_mode(good), "order_status": status,
        "channel": _txt(good, "channel"), "delivery_minutes": dm.astype("Int64"),
        "net_revenue": amt.where(delivered, 0.0).round(2),
        "gross_margin": (amt - q * cost).where(delivered, 0.0).round(2),
        "within_15_min": (dm <= cfg.SLA_MINUTES).where(delivered & dm.notna()).astype("boolean"),
        "dq_warnings": good["failure_reason"],
        "_source_file": good["_source_file"], "_source_row": good["_source_row"], "_run_id": run_id,
    })

    pa = _passed(validated["payments"])
    fact_payments = pd.DataFrame({
        "payment_id": _txt(pa, "payment_id"), "order_id": _txt(pa, "order_id"),
        "payment_ts": _ts(pa, "payment_ts"), "payment_mode": _std_payment_mode(pa),
        "amount_paid": _num(pa, "amount_paid"), "payment_status": _txt(pa, "payment_status").str.upper(),
        "gateway_ref": _txt(pa, "gateway_ref").replace("", None),
        "_source_file": pa["_source_file"], "_source_row": pa["_source_row"],
    })

    print(f"[Transform] fact rows={len(fact)} (from {len(latest)} distinct orders, {superseded} older versions "
          f"superseded by increment files), payments={len(fact_payments)}, customers={len(dim_customer)}, "
          f"products={len(dim_product)}, stores={len(dim_store)}")
    return {
        "fact_orders": _save(fact, run_id, "fact_orders"),
        "fact_payments": _save(fact_payments, run_id, "fact_payments"),
        "dim_customer": _save(dim_customer, run_id, "dim_customer"),
        "dim_product": _save(dim_product, run_id, "dim_product"),
        "dim_store": _save(dim_store, run_id, "dim_store"),
        "batch_order_ids": _save(pd.DataFrame({"order_id": batch_order_ids}), run_id, "batch_order_ids"),
    }


# ── LOAD ────────────────────────────────────────────────────────────────────

FACT_DDL = """
CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id VARCHAR PRIMARY KEY, order_ts TIMESTAMP, order_date DATE,
    customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity INTEGER, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes INTEGER,
    net_revenue DOUBLE, gross_margin DOUBLE, within_15_min BOOLEAN, dq_warnings VARCHAR,
    _source_file VARCHAR, _source_row BIGINT, _run_id VARCHAR, _loaded_at TIMESTAMP
)
"""


def load(paths: dict):
    """Idempotent load: dimensions are replaced, the fact table is upserted on order_id."""
    fact = pd.read_pickle(paths["fact_orders"])
    fact["_loaded_at"] = pd.Timestamp.utcnow().tz_localize(None)
    batch_ids = pd.read_pickle(paths["batch_order_ids"])
    dims = {name: pd.read_pickle(paths[name]) for name in ["dim_customer", "dim_product", "dim_store"]}
    payments = pd.read_pickle(paths["fact_payments"])

    with connect() as con:
        con.execute(FACT_DDL)
        con.execute("BEGIN TRANSACTION")
        try:
            for name, df in dims.items():
                con.register("df_dim", df)
                con.execute(f"CREATE OR REPLACE TABLE etl_{name} AS SELECT * FROM df_dim")
                con.unregister("df_dim")
            con.execute("CREATE OR REPLACE TABLE etl_fact_payments AS SELECT * FROM payments")

            before = set(r[0] for r in con.execute("SELECT order_id FROM etl_fact_orders").fetchall())
            batch = set(batch_ids["order_id"])
            loaded = set(fact["order_id"])
            # Upsert = delete every order in this batch, then insert the clean latest version.
            # Orders whose latest version failed validation are removed (they are in quarantine).
            con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM batch_ids)")
            con.execute("INSERT INTO etl_fact_orders BY NAME SELECT * FROM fact")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        total = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]

    stats = {
        "inserted": len(loaded - before), "updated": len(loaded & before),
        "removed_now_invalid": len((batch - loaded) & before), "fact_rows_total": total,
        "rows_loaded": len(fact) + len(payments) + sum(len(d) for d in dims.values()),
    }
    print(f"[Load] etl_fact_orders: {stats['inserted']} inserted, {stats['updated']} updated, "
          f"{stats['removed_now_invalid']} removed (latest version invalid) -> {total} rows total")
    print(f"[Load] etl_dim_customer={len(dims['dim_customer'])}, etl_dim_product={len(dims['dim_product'])}, "
          f"etl_dim_store={len(dims['dim_store'])}, etl_fact_payments={len(payments)}")
    return stats


# ── RECONCILE ───────────────────────────────────────────────────────────────

def reconcile():
    """One row per order/payment mismatch between clean orders and clean payments."""
    with connect() as con:
        orders = con.execute("SELECT order_id, order_status, order_amount, payment_mode FROM etl_fact_orders").df()
        payments = con.execute(
            "SELECT payment_id, order_id, payment_status, amount_paid, payment_mode AS paid_mode FROM etl_fact_payments"
        ).df()
        m = orders.merge(payments, on="order_id", how="outer", indicator=True)
        has_order = m["_merge"] != "right_only"
        has_payment = m["_merge"] != "left_only"
        delivered = m["order_status"] == "DELIVERED"
        expected = m["order_amount"].where(delivered, 0.0).where(has_order)
        collected = m["amount_paid"].where(m["payment_status"] == "SUCCESS", 0.0).fillna(0.0)
        m["expected_amount"] = expected.round(2)
        m["collected_amount"] = collected.round(2)
        m["difference"] = (collected - expected.fillna(0.0)).round(2)
        diff = m["difference"].abs() > cfg.AMOUNT_TOLERANCE

        def reason(r, d, ho, hp):
            if not ho:
                return "payment_for_rejected_order"
            if not hp:
                return "no_payment_record" if r.order_status == "DELIVERED" else None
            if r.order_status == "DELIVERED" and r.payment_status == "FAILED":
                return "payment_failed"
            if r.order_status == "DELIVERED" and r.payment_status == "REFUNDED":
                return "refunded_but_delivered"
            if r.order_status == "RETURNED" and r.payment_status == "SUCCESS":
                return "returned_not_refunded"
            if r.order_status == "CANCELLED" and r.payment_status == "SUCCESS":
                return "cancelled_but_charged"
            if d:
                return "amount_mismatch"
            if r.payment_mode != r.paid_mode:
                return "payment_mode_mismatch"
            return None

        m["mismatch_reason"] = [reason(r, d, ho, hp) for r, d, ho, hp in
                                zip(m.itertuples(), diff, has_order, has_payment)]
        rec = m[m["mismatch_reason"].notna()][[
            "order_id", "payment_id", "order_status", "payment_status", "payment_mode", "paid_mode",
            "expected_amount", "collected_amount", "difference", "mismatch_reason"]].sort_values("order_id")
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM rec")

    summary = rec.groupby("mismatch_reason").agg(rows=("order_id", "size"), amount=("difference", lambda s: s.abs().sum()))
    print(f"[Reconcile] {len(rec)} mismatches, unreconciled amount = Rs {rec['difference'].abs().sum():,.2f}")
    print(summary.to_string())
    return {"mismatches": len(rec), "unreconciled_amount": round(float(rec["difference"].abs().sum()), 2)}


# ── REPORT ──────────────────────────────────────────────────────────────────

def report():
    with connect() as con:
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT f.order_date, s.zone,
                   COUNT(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
                   ROUND(SUM(f.net_revenue), 2)  AS net_revenue,
                   ROUND(SUM(f.gross_margin), 2) AS gross_margin
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
            GROUP BY ALL ORDER BY f.order_date, s.zone
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category,
                   COUNT(*)                                    AS delivered_orders,
                   SUM(f.quantity)                             AS units,
                   ROUND(SUM(f.net_revenue), 2)                AS net_revenue,
                   ROUND(SUM(f.quantity * p.cost_price), 2)    AS cost_of_goods,
                   ROUND(SUM(f.gross_margin), 2)               AS gross_margin,
                   ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue), 0), 1) AS margin_pct
            FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY ALL ORDER BY net_revenue DESC
        """)
        zone = con.execute("""SELECT zone, SUM(delivered_orders) AS delivered_orders, SUM(net_revenue) AS net_revenue
                              FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC""").df()
        cat = con.execute("SELECT * FROM etl_category_margin").df()
        total = con.execute("SELECT ROUND(SUM(net_revenue), 2), COUNT(*) FILTER (WHERE order_status='DELIVERED') FROM etl_fact_orders").fetchone()
    print("[Report] Net revenue by zone:\n" + zone.to_string(index=False))
    print("[Report] Category margin:\n" + cat.to_string(index=False))
    print(f"[Report] TOTAL delivered orders={total[1]}, net revenue=Rs {total[0]:,.2f}")
    return {"net_revenue": float(total[0]), "delivered_orders": int(total[1])}
