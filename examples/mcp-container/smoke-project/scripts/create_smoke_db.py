"""Create the DuckDB table used by the container MCP smoke project."""

from __future__ import annotations

from pathlib import Path

import ibis
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "smoke.duckdb"


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = ibis.duckdb.connect(str(DB_PATH))
    conn.create_table(
        "content_scores",
        pd.DataFrame(
            [
                {
                    "content_id": "post-001",
                    "title": "Launch notes",
                    "channel": "linkedin",
                    "impressions": 1200,
                    "total_engagement": 84,
                    "engagement_rate_pct": 7.0,
                    "performance_tier": "high",
                },
                {
                    "content_id": "post-002",
                    "title": "Workshop recap",
                    "channel": "linkedin",
                    "impressions": 800,
                    "total_engagement": 32,
                    "engagement_rate_pct": 4.0,
                    "performance_tier": "medium",
                },
                {
                    "content_id": "email-001",
                    "title": "Newsletter feature",
                    "channel": "email",
                    "impressions": 2500,
                    "total_engagement": 125,
                    "engagement_rate_pct": 5.0,
                    "performance_tier": "high",
                },
            ]
        ),
        overwrite=True,
    )
    print(f"Wrote {DB_PATH}")


if __name__ == "__main__":
    main()
