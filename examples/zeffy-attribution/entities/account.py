"""Account entity: identity graph merging organizations and amplitude merge_ids.

Uses DerivedSource with IdentityGraphConfig to match on organization_id,
combining organization metadata with amplitude identity merge data.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.core.source import (
    DerivedSource,
    IdentityGraphConfig,
    IdentityGraphSource,
)

entity = Entity(
    name="account",
    description="Zeffy account identity graph from organizations and amplitude merge IDs",
    source=DerivedSource(
        identity_graph="zeffy_account_graph",
        identity_graph_config=IdentityGraphConfig(
            match_key="organization_id",
            sources=[
                IdentityGraphSource(
                    name="organizations",
                    source=TableSource(
                        project="zeffy",
                        dataset="core",
                        table="organizations",
                        duckdb_path="examples/zeffy-attribution/data/organizations/*.parquet",
                    ),
                    fields={
                        "org_name": "name",
                        "org_country": "country",
                        "org_region": "region",
                        "org_category": "category",
                        "org_cause": "cause",
                        "org_type": "type",
                        "org_website": "website",
                        "how_heard": "how_did_you_hear_about_simplyk_question",
                    },
                    match_key_field="id",
                    id_field="id",
                    date_field="created_at_utc",
                ),
                IdentityGraphSource(
                    name="amplitude",
                    source=TableSource(
                        project="zeffy",
                        dataset="amplitude",
                        table="merge_ids",
                        duckdb_path="examples/zeffy-attribution/data/amplitude/merge_ids/*.parquet",
                    ),
                    fields={
                        "primary_amplitude_id": "amplitude_id",
                        "merged_amplitude_id": "merged_amplitude_id",
                    },
                    match_key_field="amplitude_id",
                    id_field="amplitude_id",
                    date_field="merge_event_time",
                ),
            ],
            priority=["organizations", "amplitude"],
        ),
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_account"),
        dimension=DimensionLayer(
            model_name="dim_account",
            computed_columns=[
                ComputedColumn(
                    name="account_id",
                    expression="t.organization_id",
                    description="Primary account key (alias for organization_id)",
                ),
            ],
        ),
    ),
)
