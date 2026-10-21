"""
include/nammamart/dq_rules.py
-----------------------------
The data quality rule catalogue shared by the ETL (pandas) and ELT (SQL) pipelines.

Each rule has a name, the file it applies to, a quality dimension and a severity:
  Critical -> the row is quarantined and never reaches reporting tables
  Warning  -> the row loads, but the problem is logged in dq_results / dq_warnings

ETL implements every rule in include/nammamart/etl_core.py, ELT implements the same
rule names in include/sql/staging/*.sql, so dq_results is directly comparable.
Rules marked dataset=True are evaluated once per file, not per row.
"""

CRITICAL = "Critical"
WARNING = "Warning"

# (file, check_name, dimension, severity, description, dataset_level)
RULES = [
    # ── orders ──────────────────────────────────────────────────────────────
    ("orders", "customer_id_missing", "Completeness", CRITICAL, "customer_id is blank", False),
    ("orders", "delivery_minutes_missing", "Completeness", CRITICAL, "DELIVERED order has no delivery_minutes", False),
    ("orders", "volume_drop_vs_previous_run", "Completeness", WARNING, "order rows dropped >90% vs previous run", True),
    ("orders", "duplicate_order_id", "Uniqueness", CRITICAL, "order_id appears more than once in the same file", False),
    ("orders", "order_ts_unparseable", "Validity", CRITICAL, "order_ts is not a valid YYYY-MM-DD HH:MM:SS timestamp", False),
    ("orders", "order_status_invalid", "Validity", CRITICAL, "order_status not in DELIVERED/CANCELLED/RETURNED", False),
    ("orders", "payment_mode_invalid", "Validity", CRITICAL, "payment_mode not in UPI/Card/Cash on Delivery/Wallet", False),
    ("orders", "discount_pct_out_of_range", "Validity", CRITICAL, "discount_pct not between 0 and 100", False),
    ("orders", "order_status_nonstandard", "Validity", WARNING, "order_status needed trimming/casing fix", False),
    ("orders", "payment_mode_nonstandard", "Validity", WARNING, "payment_mode needed mapping (upi, cod, Cashh...)", False),
    ("orders", "quantity_not_positive", "Accuracy", CRITICAL, "quantity is zero, negative or not a number", False),
    ("orders", "order_amount_formula_mismatch", "Accuracy", CRITICAL, "order_amount != quantity x unit_price x (1 - discount/100)", False),
    ("orders", "delivery_minutes_over_120", "Accuracy", CRITICAL, "delivery_minutes >= 120 (not believable)", False),
    ("orders", "delivery_minutes_outside_normal", "Accuracy", WARNING, "delivery_minutes outside the normal 8-35 range", False),
    ("orders", "unit_price_above_mrp", "Accuracy", CRITICAL, "unit_price higher than the product MRP (illegal in India)", False),
    ("orders", "customer_id_not_in_master", "Consistency", CRITICAL, "customer_id missing from the validated customer master", False),
    ("orders", "product_id_not_in_master", "Consistency", CRITICAL, "product_id missing from the validated product master", False),
    ("orders", "store_id_not_in_master", "Consistency", CRITICAL, "store_id missing from the validated store master", False),
    ("orders", "store_over_capacity", "Consistency", WARNING, "store took more orders that day than capacity_orders_per_day", False),
    ("orders", "order_ts_in_future", "Timeliness", CRITICAL, "order_ts is later than the run's as-of time", False),
    ("orders", "stale_data", "Timeliness", WARNING, "latest order_ts is more than 24h older than the as-of time", True),
    # ── payments ────────────────────────────────────────────────────────────
    ("payments", "gateway_ref_missing", "Completeness", WARNING, "non-cash payment has no gateway_ref", False),
    ("payments", "duplicate_payment_id", "Uniqueness", CRITICAL, "payment_id appears more than once", False),
    ("payments", "payment_ts_unparseable", "Validity", CRITICAL, "payment_ts is not a valid timestamp", False),
    ("payments", "payment_status_invalid", "Validity", CRITICAL, "payment_status not in SUCCESS/FAILED/REFUNDED", False),
    ("payments", "payment_mode_invalid", "Validity", CRITICAL, "payment_mode not in the allowed list", False),
    ("payments", "payment_mode_nonstandard", "Validity", WARNING, "payment_mode needed mapping (upi, cod, Cashh...)", False),
    ("payments", "amount_paid_not_positive", "Accuracy", CRITICAL, "amount_paid is zero, negative or not a number", False),
    ("payments", "order_id_unknown", "Consistency", CRITICAL, "payment refers to an order_id that does not exist", False),
    ("payments", "payment_ts_in_future", "Timeliness", CRITICAL, "payment_ts is later than the run's as-of time", False),
    # ── customers ───────────────────────────────────────────────────────────
    ("customers", "customer_id_missing", "Completeness", CRITICAL, "customer_id is blank", False),
    ("customers", "city_missing", "Completeness", WARNING, "city is blank", False),
    ("customers", "duplicate_customer_id", "Uniqueness", CRITICAL, "customer_id appears more than once (first row kept)", False),
    ("customers", "email_invalid", "Validity", WARNING, "email is not name@domain.tld", False),
    ("customers", "phone_invalid", "Validity", WARNING, "phone is not a 10-digit Indian mobile number", False),
    ("customers", "pincode_invalid", "Validity", WARNING, "pincode is not 6 digits", False),
    ("customers", "signup_date_unparseable", "Validity", CRITICAL, "signup_date is not a valid date", False),
    ("customers", "loyalty_tier_invalid", "Validity", CRITICAL, "loyalty_tier not in Bronze/Silver/Gold/Platinum", False),
    ("customers", "loyalty_tier_nonstandard_case", "Consistency", WARNING, "loyalty_tier uses a different casing (BRONZE, bronze)", False),
    ("customers", "signup_date_in_future", "Timeliness", CRITICAL, "signup_date is later than the run's as-of date", False),
    # ── products ────────────────────────────────────────────────────────────
    ("products", "category_missing", "Completeness", WARNING, "category is blank (loaded as 'Uncategorised')", False),
    ("products", "duplicate_product_id", "Uniqueness", CRITICAL, "product_id appears more than once (first row kept)", False),
    ("products", "mrp_not_positive", "Accuracy", CRITICAL, "mrp is zero, negative or not a number", False),
    ("products", "cost_price_above_mrp", "Accuracy", CRITICAL, "cost_price is higher than mrp (margin would be wrong)", False),
    # ── stores ──────────────────────────────────────────────────────────────
    ("stores", "zone_missing", "Completeness", WARNING, "zone is blank (loaded as 'Unknown')", False),
    ("stores", "duplicate_store_id", "Uniqueness", CRITICAL, "store_id appears more than once", False),
    ("stores", "zone_invalid", "Validity", CRITICAL, "zone not in North/South/East/West", False),
    ("stores", "capacity_not_positive", "Accuracy", WARNING, "capacity_orders_per_day is zero or negative", False),
]

SEVERITY = {(f, n): s for f, n, _, s, _, _ in RULES}


def rules_for(file_key):
    return [r for r in RULES if r[0] == file_key]


def catalog_rows():
    """Rows for the dq_check_catalog table (used by the ELT SQL)."""
    return [
        {"file": f, "check_name": n, "dimension": d, "severity": s, "description": desc, "dataset_level": ds}
        for f, n, d, s, desc, ds in RULES
    ]
