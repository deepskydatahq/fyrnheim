"""Account attributed entity: wide table joining account with attribution models.

Uses DerivedSource to combine account (identity graph) with both attribution
models into a single denormalized table for analysis.
"""

from fyrnheim import (
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
)
from fyrnheim.core.source import DerivedSource

entity = Entity(
    name="account_attributed",
    description="Wide account table with first-touch and paid-priority attribution",
    source=DerivedSource(
        identity_graph="account_attributed_graph",
        depends_on=["account", "attribution_first_touch", "attribution_paid_priority"],
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_account_attributed"),
        dimension=DimensionLayer(model_name="dim_account_attributed"),
    ),
)
