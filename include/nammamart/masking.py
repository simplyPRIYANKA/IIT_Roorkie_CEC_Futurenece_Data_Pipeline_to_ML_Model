"""
include/nammamart/masking.py
----------------------------
PII masking used by the ETL pipeline. The ELT pipeline applies the same rules in
SQL (include/sql/staging/stg_customers.sql) so both produce identical output.

  mask_email("asha.rao@example.com")     -> "as***@example.com"
  mask_phone("9876543210")               -> "98******10"
  mask_name("Asha Rao")                  -> "Asha R."
"""

import re

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}$")
PHONE_RE = re.compile(r"^[6-9][0-9]{9}$")
PINCODE_RE = re.compile(r"^[0-9]{6}$")


def mask_email(email):
    """Keep the first 2 characters and the domain. Invalid emails are not shown at all."""
    email = (email or "").strip()
    if not EMAIL_RE.match(email):
        return None
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(phone):
    """Keep the first 2 and last 2 digits, star the rest."""
    phone = (phone or "").strip()
    if len(phone) <= 4:
        return "*" * len(phone) or None
    return phone[:2] + "*" * (len(phone) - 4) + phone[-2:]


def mask_name(full_name):
    """First name + initial of the last name."""
    parts = (full_name or "").split()
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else f"{parts[0]} {parts[-1][0]}."


def mask_record(record: dict) -> dict:
    """Mask PII columns in a raw record before it is written to a quarantine file."""
    out = dict(record)
    if "email" in out:
        out["email"] = mask_email(out["email"]) or "***"
    if "phone" in out:
        out["phone"] = mask_phone(out["phone"])
    if "full_name" in out:
        out["full_name"] = mask_name(out["full_name"])
    return out
