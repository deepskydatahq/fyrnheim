"""Generate sample parquet data for the timo-data-stack example.

Creates small but representative datasets for all 9 entities.
Shared emails across sources ensure the person identity graph can match records.
Two email domains (personal + business) enable account aggregation testing.

Usage:
    python generate_sample_data.py
"""

from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# Shared identity data — these emails appear across multiple sources
# ---------------------------------------------------------------------------

SHARED_PEOPLE = [
    {"email": "alice@acme.com", "name": "Alice Anderson"},
    {"email": "bob@acme.com", "name": "Bob Baker"},
    {"email": "carol@gmail.com", "name": "Carol Chen"},
    {"email": "dave@example.org", "name": "Dave Davis"},
    {"email": "eve@gmail.com", "name": "Eve Evans"},
    {"email": "frank@acme.com", "name": "Frank Foster"},
    {"email": "grace@gmail.com", "name": "Grace Green"},
    {"email": "hank@bigcorp.io", "name": "Hank Hill"},
]


def _write(df: pd.DataFrame, subdir: str) -> None:
    """Write DataFrame to parquet in a subdirectory."""
    out = DATA_DIR / subdir
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "sample.parquet", index=False)
    print(f"  {subdir}/sample.parquet: {len(df)} rows")


# ---------------------------------------------------------------------------
# 1. ghost_members (ghost_person entity)
# ---------------------------------------------------------------------------

def generate_ghost_members() -> None:
    df = pd.DataFrame([
        {"id": "gh-001", "email": "alice@acme.com", "status": "paid", "name": "Alice Anderson", "created_at": pd.Timestamp("2024-01-15"), "email_disabled": False},
        {"id": "gh-002", "email": "bob@acme.com", "status": "free", "name": "Bob Baker", "created_at": pd.Timestamp("2024-02-01"), "email_disabled": False},
        {"id": "gh-003", "email": "carol@gmail.com", "status": "free", "name": "Carol Chen", "created_at": pd.Timestamp("2024-02-10"), "email_disabled": False},
        {"id": "gh-004", "email": "dave@example.org", "status": "paid", "name": "Dave Davis", "created_at": pd.Timestamp("2024-03-01"), "email_disabled": True},
        {"id": "gh-005", "email": "eve@gmail.com", "status": "comped", "name": "Eve Evans", "created_at": pd.Timestamp("2024-03-15"), "email_disabled": False},
        {"id": "gh-006", "email": "frank@acme.com", "status": "free", "name": "Frank Foster", "created_at": pd.Timestamp("2024-04-01"), "email_disabled": False},
        {"id": "gh-007", "email": "grace@gmail.com", "status": "paid", "name": "Grace Green", "created_at": pd.Timestamp("2024-04-15"), "email_disabled": False},
        {"id": "gh-008", "email": "hank@bigcorp.io", "status": "free", "name": "Hank Hill", "created_at": pd.Timestamp("2024-05-01"), "email_disabled": False},
        {"id": "gh-009", "email": "iris@solo.dev", "status": "free", "name": "Iris Ing", "created_at": pd.Timestamp("2024-05-15"), "email_disabled": True},
        {"id": "gh-010", "email": "jack@gmail.com", "status": "paid", "name": "Jack Jones", "created_at": pd.Timestamp("2024-06-01"), "email_disabled": False},
    ])
    _write(df, "ghost_members")


# ---------------------------------------------------------------------------
# 2. mailerlite_subscribers (mailerlite_person entity)
# ---------------------------------------------------------------------------

def generate_mailerlite_subscribers() -> None:
    df = pd.DataFrame([
        {"id": "ml-001", "email": "alice@acme.com", "status": "active", "subscribed_at": pd.Timestamp("2024-01-10"), "source": "website"},
        {"id": "ml-002", "email": "bob@acme.com", "status": "active", "subscribed_at": pd.Timestamp("2024-01-20"), "source": "webinar"},
        {"id": "ml-003", "email": "carol@gmail.com", "status": "unsubscribed", "subscribed_at": pd.Timestamp("2024-02-05"), "source": "website"},
        {"id": "ml-004", "email": "eve@gmail.com", "status": "active", "subscribed_at": pd.Timestamp("2024-03-10"), "source": "lead_magnet"},
        {"id": "ml-005", "email": "frank@acme.com", "status": "active", "subscribed_at": pd.Timestamp("2024-03-20"), "source": "website"},
        {"id": "ml-006", "email": "grace@gmail.com", "status": "active", "subscribed_at": pd.Timestamp("2024-04-10"), "source": "referral"},
        {"id": "ml-007", "email": "hank@bigcorp.io", "status": "bounced", "subscribed_at": pd.Timestamp("2024-04-25"), "source": "website"},
        {"id": "ml-008", "email": "kate@startup.co", "status": "active", "subscribed_at": pd.Timestamp("2024-05-01"), "source": "webinar"},
    ])
    _write(df, "mailerlite_subscribers")


# ---------------------------------------------------------------------------
# 3. transactions (transactions entity)
# ---------------------------------------------------------------------------

def generate_transactions() -> None:
    df = pd.DataFrame([
        {"id": "tx-001", "store_id": 1001, "identifier": "ORD-001", "order_number": 1, "status": "paid", "customer_id": 5001, "customer_name": "Alice Anderson", "customer_email": "alice@acme.com", "total": 9900, "subtotal": 9900, "currency": "USD", "refunded": False, "created_at": pd.Timestamp("2024-02-01"), "updated_at": pd.Timestamp("2024-02-01")},
        {"id": "tx-002", "store_id": 1001, "identifier": "ORD-002", "order_number": 2, "status": "paid", "customer_id": 5002, "customer_name": "Bob Baker", "customer_email": "bob@acme.com", "total": 4900, "subtotal": 4900, "currency": "USD", "refunded": False, "created_at": pd.Timestamp("2024-02-15"), "updated_at": pd.Timestamp("2024-02-15")},
        {"id": "tx-003", "store_id": 1001, "identifier": "ORD-003", "order_number": 3, "status": "refunded", "customer_id": 5003, "customer_name": "Carol Chen", "customer_email": "carol@gmail.com", "total": 9900, "subtotal": 9900, "currency": "USD", "refunded": True, "created_at": pd.Timestamp("2024-03-01"), "updated_at": pd.Timestamp("2024-03-05")},
        {"id": "tx-004", "store_id": 1001, "identifier": "ORD-004", "order_number": 4, "status": "paid", "customer_id": 5001, "customer_name": "Alice Anderson", "customer_email": "alice@acme.com", "total": 19900, "subtotal": 19900, "currency": "USD", "refunded": False, "created_at": pd.Timestamp("2024-04-01"), "updated_at": pd.Timestamp("2024-04-01")},
        {"id": "tx-005", "store_id": 1001, "identifier": "ORD-005", "order_number": 5, "status": "paid", "customer_id": 5004, "customer_name": "Dave Davis", "customer_email": "dave@example.org", "total": 4900, "subtotal": 4900, "currency": "USD", "refunded": False, "created_at": pd.Timestamp("2024-04-15"), "updated_at": pd.Timestamp("2024-04-15")},
        {"id": "tx-006", "store_id": 1001, "identifier": "ORD-006", "order_number": 6, "status": "paid", "customer_id": 5005, "customer_name": "Eve Evans", "customer_email": "eve@gmail.com", "total": 9900, "subtotal": 9900, "currency": "USD", "refunded": False, "created_at": pd.Timestamp("2024-05-01"), "updated_at": pd.Timestamp("2024-05-01")},
        {"id": "tx-007", "store_id": 1001, "identifier": "ORD-007", "order_number": 7, "status": "pending", "customer_id": 5006, "customer_name": "Frank Foster", "customer_email": "frank@acme.com", "total": 14900, "subtotal": 14900, "currency": "USD", "refunded": False, "created_at": pd.Timestamp("2024-05-15"), "updated_at": pd.Timestamp("2024-05-15")},
        {"id": "tx-008", "store_id": 1001, "identifier": "ORD-008", "order_number": 8, "status": "paid", "customer_id": 5008, "customer_name": "Hank Hill", "customer_email": "hank@bigcorp.io", "total": 29900, "subtotal": 29900, "currency": "USD", "refunded": False, "created_at": pd.Timestamp("2024-06-01"), "updated_at": pd.Timestamp("2024-06-01")},
    ])
    _write(df, "transactions")


# ---------------------------------------------------------------------------
# 4. subscriptions (subscriptions entity)
# ---------------------------------------------------------------------------

def generate_subscriptions() -> None:
    df = pd.DataFrame([
        {"id": "sub-001", "store_id": 1001, "product_id": 2001, "variant_id": 3001, "status": "active", "user_email": "alice@acme.com", "user_name": "Alice Anderson", "renews_at": pd.Timestamp("2025-02-01"), "ends_at": pd.NaT, "cancelled": False, "billing_anchor": 1, "card_brand": "visa", "created_at": pd.Timestamp("2024-02-01"), "updated_at": pd.Timestamp("2024-02-01")},
        {"id": "sub-002", "store_id": 1001, "product_id": 2001, "variant_id": 3001, "status": "cancelled", "user_email": "carol@gmail.com", "user_name": "Carol Chen", "renews_at": pd.NaT, "ends_at": pd.Timestamp("2024-06-01"), "cancelled": True, "billing_anchor": 1, "card_brand": "mastercard", "created_at": pd.Timestamp("2024-03-01"), "updated_at": pd.Timestamp("2024-06-01")},
        {"id": "sub-003", "store_id": 1001, "product_id": 2002, "variant_id": 3002, "status": "active", "user_email": "dave@example.org", "user_name": "Dave Davis", "renews_at": pd.Timestamp("2025-05-01"), "ends_at": pd.NaT, "cancelled": False, "billing_anchor": 15, "card_brand": "visa", "created_at": pd.Timestamp("2024-04-15"), "updated_at": pd.Timestamp("2024-04-15")},
        {"id": "sub-004", "store_id": 1001, "product_id": 2001, "variant_id": 3001, "status": "on_trial", "user_email": "eve@gmail.com", "user_name": "Eve Evans", "renews_at": pd.Timestamp("2025-06-01"), "ends_at": pd.NaT, "cancelled": False, "billing_anchor": 1, "card_brand": "amex", "created_at": pd.Timestamp("2024-05-01"), "updated_at": pd.Timestamp("2024-05-01")},
        {"id": "sub-005", "store_id": 1001, "product_id": 2002, "variant_id": 3002, "status": "expired", "user_email": "bob@acme.com", "user_name": "Bob Baker", "renews_at": pd.NaT, "ends_at": pd.Timestamp("2024-08-01"), "cancelled": False, "billing_anchor": 15, "card_brand": "visa", "created_at": pd.Timestamp("2024-02-15"), "updated_at": pd.Timestamp("2024-08-01")},
        {"id": "sub-006", "store_id": 1001, "product_id": 2001, "variant_id": 3001, "status": "active", "user_email": "hank@bigcorp.io", "user_name": "Hank Hill", "renews_at": pd.Timestamp("2025-07-01"), "ends_at": pd.NaT, "cancelled": False, "billing_anchor": 1, "card_brand": "visa", "created_at": pd.Timestamp("2024-06-01"), "updated_at": pd.Timestamp("2024-06-01")},
    ])
    _write(df, "subscriptions")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating sample data...")
    generate_ghost_members()
    generate_mailerlite_subscribers()
    generate_transactions()
    generate_subscriptions()
    print("Done.")
