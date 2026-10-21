"""
include/nammamart/config.py
---------------------------
Single place for paths, file specs and business constants used by BOTH pipelines.
Paths are derived from this file's location, so the same code runs inside the
Astro container (/usr/local/airflow/include) and on a laptop for local testing.
"""

import glob
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

LEARNER = "priyanka"

INCLUDE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(INCLUDE_DIR, "data", "nammamart")
SQL_DIR = os.path.join(INCLUDE_DIR, "sql")
# Env overrides exist only so local tests can use a scratch warehouse.
WAREHOUSE_DB = os.getenv("NAMMAMART_WAREHOUSE", os.path.join(INCLUDE_DIR, f"warehouse_{LEARNER}.duckdb"))
QUARANTINE_DIR = os.getenv("NAMMAMART_QUARANTINE_DIR", os.path.join(INCLUDE_DIR, "quarantine", LEARNER))
TMP_DIR = os.getenv("NAMMAMART_TMP_DIR", os.path.join(INCLUDE_DIR, "tmp"))

TZ = "Asia/Kolkata"

# file key -> (csv name, primary key, expected columns in order)
SOURCES = {
    "stores": ("stores.csv", "store_id", [
        "store_id", "store_name", "zone", "area", "pincode", "open_date",
        "manager_name", "capacity_orders_per_day"]),
    "products": ("products.csv", "product_id", [
        "product_id", "product_name", "category", "sub_category", "brand", "mrp",
        "cost_price", "unit", "is_perishable", "launch_date"]),
    "customers": ("customers.csv", "customer_id", [
        "customer_id", "full_name", "email", "phone", "city", "area", "pincode",
        "signup_date", "loyalty_tier", "is_active"]),
    "orders": ("orders.csv", "order_id", [
        "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
        "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
        "channel", "delivery_minutes"]),
    "payments": ("payments.csv", "payment_id", [
        "payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
        "payment_status", "gateway_ref"]),
}
INCREMENT_GLOB = "orders_*_increment.csv"

# ── Business rules agreed with the client ───────────────────────────────────
ORDER_STATUSES = ["DELIVERED", "CANCELLED", "RETURNED"]
PAYMENT_STATUSES = ["SUCCESS", "FAILED", "REFUNDED"]
PAYMENT_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
# lower-cased, trimmed raw value -> standard value. 'cod' and 'cashh' agreed as cash.
PAYMENT_MODE_MAP = {
    "upi": "UPI", "card": "Card", "wallet": "Wallet",
    "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery", "cashh": "Cash on Delivery",
}
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
ZONES = ["North", "South", "East", "West"]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
MAX_DELIVERY_MINUTES = 120          # above this the value is not believable
NORMAL_DELIVERY_RANGE = (8, 35)     # outside this: warning only
SLA_MINUTES = 15                    # "within 15 minutes" KPI
AMOUNT_TOLERANCE = 0.011            # rupees; formula vs stated amount
VOLUME_DROP_THRESHOLD = 0.90        # warn if rows drop by more than 90 % vs previous run
STALE_AFTER_HOURS = 24


def increment_files():
    """Next-day order files present in the data folder, oldest first."""
    return sorted(os.path.basename(p) for p in glob.glob(os.path.join(DATA_DIR, INCREMENT_GLOB)))


def safe_run_id(run_id: str) -> str:
    """run_ids contain ':' and '+', which are not valid in Windows-mounted file names."""
    return re.sub(r"[^A-Za-z0-9]+", "_", run_id).strip("_")


def get_as_of() -> str:
    """
    Reference 'now' (IST, naive 'YYYY-MM-DD HH:MM:SS') for timeliness checks.
    Set the Airflow Variable NAMMAMART_AS_OF to replay a business day reproducibly;
    otherwise the real clock is used.
    """
    value = None
    try:
        from airflow.sdk import Variable
        value = Variable.get("NAMMAMART_AS_OF", default=None)
    except Exception:
        value = os.getenv("NAMMAMART_AS_OF")
    if value:
        return datetime.fromisoformat(value.strip()).strftime(TS_FORMAT)
    return datetime.now(ZoneInfo(TZ)).strftime(TS_FORMAT)
