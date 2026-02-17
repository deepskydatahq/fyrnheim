"""Generate sample customer data for the typedata example.

Run: python examples/data/generate_sample_data.py
Output: examples/data/customers.parquet
"""

from pathlib import Path

import pandas as pd

data = {
    "id": list(range(1, 13)),
    "email": [
        "alice@acme.com",
        "bob@gmail.com",
        "carol@bigcorp.io",
        "dave@example.com",
        "eve@startup.dev",
        "frank@gmail.com",
        "grace@enterprise.co",
        "hank@yahoo.com",
        "iris@acme.com",
        "jack@outlook.com",
        "karen@bigcorp.io",
        "leo@startup.dev",
    ],
    "name": [
        "Alice Johnson",
        "Bob Smith",
        "Carol Williams",
        "Dave Brown",
        "Eve Davis",
        "Frank Miller",
        "Grace Wilson",
        "Hank Taylor",
        "Iris Anderson",
        "Jack Thomas",
        "Karen Martinez",
        "Leo Garcia",
    ],
    "created_at": pd.to_datetime(
        [
            "2024-01-15T10:30:00",
            "2024-02-20T14:15:00",
            "2024-03-10T09:00:00",
            "2024-04-05T16:45:00",
            "2024-05-12T11:20:00",
            "2024-06-01T08:00:00",
            "2024-07-20T13:30:00",
            "2024-08-15T15:00:00",
            "2024-09-03T10:00:00",
            "2024-10-18T12:00:00",
            "2024-11-25T09:30:00",
            "2024-12-30T17:00:00",
        ]
    ),
    "plan": [
        "pro",
        "starter",
        "enterprise",
        "pro",
        "starter",
        "free",
        "enterprise",
        "free",
        "pro",
        "starter",
        "enterprise",
        "pro",
    ],
    "amount_cents": [4900, 1900, 19900, 4900, 1900, 0, 19900, 0, 4900, 1900, 19900, 4900],
}

df = pd.DataFrame(data)
output_path = Path(__file__).parent / "customers.parquet"
df.to_parquet(output_path, index=False)
print(f"Written {len(df)} rows to {output_path}")
