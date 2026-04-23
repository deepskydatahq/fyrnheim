"""Generate synthetic Zeffy-like data for the attribution PoC.

Produces parquet files mimicking Zeffy's Snowflake schema:
- amplitude/events/*.parquet  (~200 events across ~30 orgs)
- amplitude/merge_ids/*.parquet (~50 merge records)
- organizations/*.parquet (~30 orgs)
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

SEED = 42
random.seed(SEED)

DATA_DIR = Path(__file__).parent / "data"

# --- Organizations -----------------------------------------------------------

NUM_ORGS = 30
CATEGORIES = ["nonprofit", "charity", "foundation", "association", "social_enterprise"]
CAUSES = [
    "education", "health", "environment", "arts", "community",
    "animal_welfare", "hunger", "housing", "youth", "elderly",
]
COUNTRIES = ["CA", "US", "CA", "CA", "US", "CA", "US", "CA", "CA", "CA"]
REGIONS = ["QC", "NY", "ON", "BC", "CA", "AB", "TX", "QC", "ON", "MB"]
HOW_HEARD = [
    "Google search", "Friend referral", "Social media", "Blog post",
    "Conference", "Email", None, None,
]
ORG_TYPES = ["registered_charity", "nonprofit", "social_enterprise"]


def generate_organizations() -> pd.DataFrame:
    rows = []
    base_date = datetime(2023, 1, 1)
    for i in range(NUM_ORGS):
        org_id = str(uuid.UUID(int=i + 1000))
        rows.append(
            {
                "id": org_id,
                "name": f"Org {i + 1:03d}",
                "country": COUNTRIES[i % len(COUNTRIES)],
                "region": REGIONS[i % len(REGIONS)],
                "category": random.choice(CATEGORIES),
                "cause": random.choice(CAUSES),
                "created_at_utc": base_date + timedelta(days=random.randint(0, 365)),
                "how_did_you_hear_about_simplyk_question": random.choice(HOW_HEARD),
                "type": random.choice(ORG_TYPES),
                "website": f"https://org{i + 1:03d}.example.com",
            }
        )
    return pd.DataFrame(rows)


# --- Amplitude events --------------------------------------------------------

EVENT_TYPES = [
    "Page Viewed",
    "Donor Form Viewed",
    "Donor Form Submitted",
    "Thank You Page Viewed",
    "Form Step Submitted",
]
PLATFORMS = ["Web", "iOS Web", "Android Web"]
REFERRING_DOMAINS = [
    "google.com", "facebook.com", "instagram.com", "linkedin.com",
    "twitter.com", "reddit.com", "blog.example.com",
]
UTM_SOURCES = ["google", "facebook", "newsletter", "partner", "twitter"]
UTM_MEDIUMS_PAID = ["cpc", "ppc", "paid", "paid_social"]
UTM_MEDIUMS_ORGANIC = ["email", "social", "referral", "organic"]
UTM_CAMPAIGNS = [
    "spring_2024", "year_end_giving", "awareness_week",
    "donor_retention", "new_donor_acq",
]
FORM_TYPES = ["DonationForm", "TicketingForm", "CustomForm", "MembershipForm"]

NUM_EVENTS = 200
NUM_AMPLITUDE_IDS = 60  # more amplitude IDs than orgs (multiple sessions)


def _make_event_properties(channel_type: str) -> str:
    """Build a JSON string of event_properties based on channel type."""
    props: dict = {
        "form_type": random.choice(FORM_TYPES),
    }

    if channel_type == "paid_search_google":
        props["gclid"] = f"gclid_{uuid.uuid4().hex[:12]}"
    elif channel_type == "paid_social_meta":
        props["fbclid"] = f"fbclid_{uuid.uuid4().hex[:12]}"
    elif channel_type == "paid_other":
        props["utm_source"] = random.choice(UTM_SOURCES)
        props["utm_medium"] = random.choice(UTM_MEDIUMS_PAID)
        props["utm_campaign"] = random.choice(UTM_CAMPAIGNS)
    elif channel_type == "organic_campaign":
        props["utm_source"] = random.choice(UTM_SOURCES)
        props["utm_medium"] = random.choice(UTM_MEDIUMS_ORGANIC)
        props["utm_campaign"] = random.choice(UTM_CAMPAIGNS)
    elif channel_type == "organic_referral":
        props["referring_domain"] = random.choice(REFERRING_DOMAINS)
    # direct_or_unknown: no attribution signals

    return json.dumps(props)


def generate_events(org_ids: list[str]) -> pd.DataFrame:
    # Create amplitude IDs and assign some to orgs
    amplitude_ids = [f"amp_{i:04d}" for i in range(NUM_AMPLITUDE_IDS)]

    # Map amplitude IDs to org IDs (some IDs share an org)
    amp_to_org = {}
    for amp_id in amplitude_ids:
        amp_to_org[amp_id] = random.choice(org_ids)

    # Channel distribution: ~20% gclid, ~15% fbclid, ~15% utm, ~10% referral, ~40% direct
    channel_weights = [
        ("paid_search_google", 0.20),
        ("paid_social_meta", 0.15),
        ("paid_other", 0.08),
        ("organic_campaign", 0.07),
        ("organic_referral", 0.10),
        ("direct_or_unknown", 0.40),
    ]
    channels = [c for c, _ in channel_weights]
    weights = [w for _, w in channel_weights]

    base_date = datetime(2024, 1, 1)
    rows = []
    for _ in range(NUM_EVENTS):
        amp_id = random.choice(amplitude_ids)
        org_id = amp_to_org[amp_id]
        channel = random.choices(channels, weights=weights, k=1)[0]
        event_time = base_date + timedelta(
            days=random.randint(0, 180),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        user_props = json.dumps({"Organization": org_id})

        rows.append(
            {
                "amplitude_id": amp_id,
                "event_type": random.choice(EVENT_TYPES),
                "event_time": event_time,
                "event_properties": _make_event_properties(channel),
                "user_properties": user_props,
                "user_id": f"user_{random.randint(1, 100):03d}",
                "device_id": f"device_{random.randint(1, 80):03d}",
                "session_id": random.randint(1_000_000, 9_999_999),
                "country": random.choice(COUNTRIES),
                "region": random.choice(REGIONS),
                "platform": random.choice(PLATFORMS),
            }
        )
    return pd.DataFrame(rows)


# --- Merge IDs ---------------------------------------------------------------

NUM_MERGES = 50


def generate_merge_ids(amplitude_ids: list[str]) -> pd.DataFrame:
    base_date = datetime(2024, 1, 1)
    rows = []
    for _ in range(NUM_MERGES):
        primary = random.choice(amplitude_ids)
        merged = random.choice(amplitude_ids)
        # Ensure they are different
        while merged == primary:
            merged = random.choice(amplitude_ids)
        merge_time = base_date + timedelta(
            days=random.randint(0, 180),
            hours=random.randint(0, 23),
        )
        rows.append(
            {
                "amplitude_id": primary,
                "merged_amplitude_id": merged,
                "merge_event_time": merge_time,
                "merge_server_time": merge_time + timedelta(seconds=random.randint(0, 60)),
            }
        )
    return pd.DataFrame(rows)


# --- Main --------------------------------------------------------------------


def main() -> None:
    # Organizations
    orgs_df = generate_organizations()
    org_path = DATA_DIR / "organizations"
    org_path.mkdir(parents=True, exist_ok=True)
    orgs_df.to_parquet(org_path / "organizations.parquet", index=False)
    print(f"Organizations: {len(orgs_df)} rows -> {org_path / 'organizations.parquet'}")

    # Events
    events_df = generate_events(orgs_df["id"].tolist())
    events_path = DATA_DIR / "amplitude" / "events"
    events_path.mkdir(parents=True, exist_ok=True)
    events_df.to_parquet(events_path / "events.parquet", index=False)
    print(f"Events: {len(events_df)} rows -> {events_path / 'events.parquet'}")

    # Merge IDs
    amplitude_ids = events_df["amplitude_id"].unique().tolist()
    merge_df = generate_merge_ids(amplitude_ids)
    merge_path = DATA_DIR / "amplitude" / "merge_ids"
    merge_path.mkdir(parents=True, exist_ok=True)
    merge_df.to_parquet(merge_path / "merge_ids.parquet", index=False)
    print(f"Merge IDs: {len(merge_df)} rows -> {merge_path / 'merge_ids.parquet'}")


if __name__ == "__main__":
    main()
