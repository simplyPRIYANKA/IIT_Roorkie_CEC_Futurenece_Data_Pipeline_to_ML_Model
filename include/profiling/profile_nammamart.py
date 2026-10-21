"""
include/profiling/profile_nammamart.py
--------------------------------------
Part 0 — Discovery & data profiling for the NammaMart dataset.

Reads every CSV as TEXT (so nothing is silently coerced) and prints:
  1. Column profile: nulls, distinct counts, duplicate keys, min/max, category values
  2. Referential integrity across the 4 relationships
  3. Business-rule checks (order_amount formula, delivery minutes, payments vs orders)

Run locally:   python include/profiling/profile_nammamart.py
"""

import os
import re
import sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from nammamart.masking import mask_record  # noqa: E402  (never print raw PII)
DATA_DIR = os.path.join(HERE, "..", "data", "nammamart")

FILES = {
    "orders":    ("orders.csv", "order_id"),
    "payments":  ("payments.csv", "payment_id"),
    "customers": ("customers.csv", "customer_id"),
    "products":  ("products.csv", "product_id"),
    "stores":    ("stores.csv", "store_id"),
    "increment": ("orders_2026-10-01_increment.csv", "order_id"),
}
NUMERIC = {"quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes",
           "amount_paid", "mrp", "cost_price", "capacity_orders_per_day"}
DATES = {"order_ts", "payment_ts", "signup_date", "launch_date", "open_date"}
CATEGORICAL = {"order_status", "payment_mode", "channel", "payment_status", "loyalty_tier",
               "is_active", "city", "zone", "category", "sub_category", "unit", "is_perishable"}


def load(name):
    fname, _ = FILES[name]
    return pd.read_csv(os.path.join(DATA_DIR, fname), dtype=str, keep_default_na=False)


def section(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def profile(name, df, key):
    section(f"PROFILE: {name}  ({len(df)} rows, {df.shape[1]} cols)")
    dup = df[df.duplicated(key, keep=False)].sort_values(key)
    print(f"Duplicate {key}: {dup[key].nunique()} keys / {len(dup)} rows")
    if len(dup):
        print(pd.DataFrame([mask_record(r) for r in dup.head(12).to_dict("records")]).to_string(index=False))
    full_dup = df.duplicated(keep=False).sum()
    print(f"Fully identical rows: {full_dup}")

    for col in df.columns:
        s = df[col]
        blank = (s.str.strip() == "").sum()
        padded = ((s != s.str.strip()) & (s.str.strip() != "")).sum()
        line = f"  {col:<24} blank={blank:<4} distinct={s.nunique():<5} padded={padded}"
        if col in NUMERIC:
            n = pd.to_numeric(s.str.strip(), errors="coerce")
            bad = s[(n.isna()) & (s.str.strip() != "")].unique()[:5]
            line += f"  min={n.min()} max={n.max()} non_numeric={list(bad)}"
        if col in DATES:
            d = pd.to_datetime(s.str.strip(), errors="coerce", format="mixed")
            bad = s[(d.isna()) & (s.str.strip() != "")].unique()[:5]
            line += f"  min={d.min()} max={d.max()} unparseable={list(bad)}"
        print(line)
        if col in CATEGORICAL:
            print(f"      values: {s.value_counts().to_dict()}")


def main():
    dfs = {n: load(n) for n in FILES}
    for n, (_, key) in FILES.items():
        profile(n, dfs[n], key)

    o, p, c, pr, st = (dfs[k] for k in ["orders", "payments", "customers", "products", "stores"])

    # ── Referential integrity ────────────────────────────────────────────────
    section("REFERENTIAL INTEGRITY")
    for fk, dim, dkey in [("customer_id", c, "customer_id"), ("product_id", pr, "product_id"),
                          ("store_id", st, "store_id")]:
        for label, frame in [("orders", o), ("increment", dfs["increment"])]:
            miss = frame[~frame[fk].str.strip().isin(dim[dkey].str.strip())]
            print(f"{label}.{fk} not in master: {len(miss)} -> {miss[['order_id', fk]].values.tolist()[:10]}")
    orphan = p[~p["order_id"].isin(o["order_id"])]
    print(f"payments.order_id not in orders: {len(orphan)} -> {orphan[['payment_id','order_id']].values.tolist()}")

    # ── Business rules: orders ───────────────────────────────────────────────
    section("BUSINESS RULES: orders (+ increment)")
    for label, frame in [("orders", o), ("increment", dfs["increment"])]:
        q = pd.to_numeric(frame["quantity"], errors="coerce")
        up = pd.to_numeric(frame["unit_price"], errors="coerce")
        dp = pd.to_numeric(frame["discount_pct"], errors="coerce")
        amt = pd.to_numeric(frame["order_amount"], errors="coerce")
        dm = pd.to_numeric(frame["delivery_minutes"], errors="coerce")
        ts = pd.to_datetime(frame["order_ts"], errors="coerce", format="mixed")
        exp = (q * up * (1 - dp / 100)).round(2)
        status = frame["order_status"].str.strip().str.upper()
        checks = {
            "blank customer_id": frame["customer_id"].str.strip() == "",
            "quantity <= 0": q <= 0,
            "discount outside 0-100": (dp < 0) | (dp > 100),
            "order_amount != formula": (exp - amt).abs() > 0.01,
            "status not in allowed (raw)": ~frame["order_status"].isin(["DELIVERED", "CANCELLED", "RETURNED"]),
            "status not in allowed (normalised)": ~status.isin(["DELIVERED", "CANCELLED", "RETURNED"]),
            "payment_mode not in allowed (raw)": ~frame["payment_mode"].isin(["UPI", "Card", "Cash on Delivery", "Wallet"]),
            "DELIVERED without delivery_minutes": (status == "DELIVERED") & dm.isna(),
            "delivery_minutes >= 120": dm >= 120,
            "delivery_minutes outside 8-35": (dm < 8) | (dm > 35),
            "order_ts unparseable": ts.isna(),
            "order_ts in future (> 2026-10-01)": ts > pd.Timestamp("2026-10-02"),
            "order_ts before Sept 2026": ts < pd.Timestamp("2026-09-01"),
            "test-looking row": frame.apply(lambda r: bool(re.search(r"test", " ".join(r.values), re.I)), axis=1),
        }
        for name, mask in checks.items():
            rows = frame[mask]
            cols = ["order_id", "order_ts", "customer_id", "store_id", "quantity", "unit_price",
                    "discount_pct", "order_amount", "payment_mode", "order_status", "delivery_minutes"]
            print(f"\n[{label}] {name}: {len(rows)}")
            if 0 < len(rows):
                print(rows[cols].head(15).to_string(index=False))

    # increment overlap
    inc = dfs["increment"]
    overlap = inc[inc["order_id"].isin(o["order_id"])]
    print(f"\nIncrement rows whose order_id already exists: {len(overlap)}; new: {len(inc) - len(overlap)}")
    m = overlap.merge(o, on="order_id", suffixes=("_new", "_old"))
    print(m[["order_id", "order_status_old", "order_status_new"]].to_string(index=False))

    # ── Business rules: payments vs orders ───────────────────────────────────
    section("BUSINESS RULES: payments vs orders")
    od = o.drop_duplicates("order_id")
    pm = p.merge(od[["order_id", "order_amount", "order_status", "payment_mode"]], on="order_id",
                 how="left", suffixes=("", "_order"))
    paid = pd.to_numeric(pm["amount_paid"], errors="coerce")
    oamt = pd.to_numeric(pm["order_amount"], errors="coerce")
    succ = pm["payment_status"].str.strip().str.upper() == "SUCCESS"
    print("SUCCESS amount != order_amount:", (succ & ((paid - oamt).abs() > 0.01)).sum())
    print(pm[succ & ((paid - oamt).abs() > 0.01)][["payment_id", "order_id", "amount_paid", "order_amount", "order_status"]].head(20).to_string(index=False))
    print("\npayment for CANCELLED order:", (pm["order_status"] == "CANCELLED").sum())
    print("payment_mode differs from order:", (pm["payment_mode"].str.strip() != pm["payment_mode_order"].str.strip()).sum())
    print("blank gateway_ref:", (p["gateway_ref"].str.strip() == "").sum())
    print("amount_paid <= 0:", (paid <= 0).sum())
    multi = p[p.duplicated("order_id", keep=False)].sort_values("order_id")
    print(f"orders with >1 payment rows: {multi['order_id'].nunique()}")
    print(multi.head(12).to_string(index=False))
    nonc = od[od["order_status"].str.strip().str.upper().isin(["DELIVERED", "RETURNED"])]
    nopay = nonc[~nonc["order_id"].isin(p["order_id"])]
    print(f"DELIVERED/RETURNED orders with no payment: {len(nopay)}")

    # ── Dimension rules ──────────────────────────────────────────────────────
    section("BUSINESS RULES: customers / products / stores")
    email_ok = c["email"].str.strip().str.match(r"^[\w.+-]+@[\w-]+\.[\w.]+$")
    phone_ok = c["phone"].str.strip().str.match(r"^[6-9]\d{9}$")
    pin_ok = c["pincode"].str.strip().str.match(r"^\d{6}$")
    signup = pd.to_datetime(c["signup_date"], errors="coerce", format="mixed")
    # PII: show only a masked shape of the bad value, never the value itself
    shape = lambda s: s.str.replace(r"[A-Za-z]", "a", regex=True).str.replace(r"[0-9]", "9", regex=True)
    print("bad email (shape only):\n", c[~email_ok].assign(email=shape(c["email"]))[["customer_id", "email"]].to_string(index=False))
    print("bad phone (shape only):\n", c[~phone_ok].assign(phone=shape(c["phone"]))[["customer_id", "phone"]].to_string(index=False))
    print("bad pincode:\n", c[~pin_ok][["customer_id", "pincode"]].to_string(index=False))
    print("future signup:\n", c[signup > pd.Timestamp("2026-10-01")][["customer_id", "signup_date"]].to_string(index=False))
    mrp = pd.to_numeric(pr["mrp"], errors="coerce")
    cost = pd.to_numeric(pr["cost_price"], errors="coerce")
    print("\nproducts mrp<=0 or cost>mrp or blank category:\n",
          pr[(mrp <= 0) | (cost > mrp) | (pr["category"].str.strip() == "") | mrp.isna() | cost.isna()].to_string(index=False))
    print("\nstores:\n", st.to_string(index=False))


if __name__ == "__main__":
    main()
