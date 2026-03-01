"""Person entity: identity graph merging 4 source entities.

Uses DerivedSource with IdentityGraphConfig to merge ghost_person,
mailerlite_person, transactions, and subscriptions via cascading FULL OUTER JOIN
on email_hash. PriorityCoalesce resolves shared fields (email, name).
Auto-generated: is_{source} flags, {source}_id, first_seen_{source} dates.
"""

from fyrnheim import (
    ComputedColumn,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
)
from fyrnheim.core.source import (
    DerivedSource,
    IdentityGraphConfig,
    IdentityGraphSource,
)

# Personal email domains for account_id logic
_PERSONAL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
    "icloud.com", "aol.com", "protonmail.com", "mail.com",
]
_personal_list = repr(_PERSONAL_DOMAINS)

entity = Entity(
    name="person",
    description="Unified person identity graph from 4 source entities",
    source=DerivedSource(
        identity_graph="person_graph",
        identity_graph_config=IdentityGraphConfig(
            match_key="email_hash",
            sources=[
                IdentityGraphSource(
                    name="ghost_person",
                    entity="ghost_person",
                    match_key_field="email_hash",
                    fields={"email": "email", "name": "name"},
                    id_field="id",
                    date_field="created_at",
                ),
                IdentityGraphSource(
                    name="mailerlite_person",
                    entity="mailerlite_person",
                    match_key_field="email_hash",
                    fields={"email": "email"},
                    id_field="id",
                    date_field="created_at",
                ),
                IdentityGraphSource(
                    name="transactions",
                    entity="transactions",
                    match_key_field="customer_email_hash",
                    fields={"email": "customer_email", "name": "customer_name"},
                    id_field="transaction_id",
                    date_field="created_at",
                ),
                IdentityGraphSource(
                    name="subscriptions",
                    entity="subscriptions",
                    match_key_field="customer_email_hash",
                    fields={"email": "user_email", "name": "user_name"},
                    id_field="subscription_id",
                    date_field="created_at",
                ),
            ],
            priority=[
                "transactions",
                "subscriptions",
                "ghost_person",
                "mailerlite_person",
            ],
        ),
    ),
    layers=LayersConfig(
        prep=PrepLayer(model_name="prep_person"),
        dimension=DimensionLayer(
            model_name="dim_person",
            computed_columns=[
                ComputedColumn(
                    name="person_id",
                    expression="t.email_hash",
                    description="Person primary key (alias for email_hash)",
                ),
                ComputedColumn(
                    name="email_domain",
                    expression='t.email.split("@")[1]',
                    description="Email domain extracted from email address",
                ),
                ComputedColumn(
                    name="is_personal_email",
                    expression=f't.email.split("@")[1].isin({_personal_list})',
                    description="True if email is from a personal email provider",
                ),
                ComputedColumn(
                    name="account_id",
                    expression=(
                        f'ibis.ifelse(t.email.split("@")[1].isin({_personal_list}), '
                        f'ibis.literal(None).cast("string"), '
                        f't.email.split("@")[1].hash().cast("string"))'
                    ),
                    description="Business account ID (NULL for personal emails)",
                ),
                ComputedColumn(
                    name="created_at",
                    expression=(
                        "ibis.coalesce("
                        "t.first_seen_transactions, "
                        "t.first_seen_subscriptions, "
                        "t.first_seen_ghost_person, "
                        "t.first_seen_mailerlite_person"
                        ")"
                    ),
                    description="Earliest first-seen date across all sources",
                ),
            ],
        ),
    ),
)
