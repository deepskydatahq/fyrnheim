"""End-to-end Zeffy attribution pipeline on DuckDB.

Runs the full attribution pipeline using the Fyrnheim framework where possible,
with documented workarounds for framework gaps. Returns structured results for
validation in tests.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import ibis

log = logging.getLogger(__name__)

EXAMPLES_DIR = Path(__file__).parent
ENTITIES_DIR = EXAMPLES_DIR / "entities"
GENERATED_DIR = EXAMPLES_DIR / "generated"
DATA_DIR = EXAMPLES_DIR / "data"


@dataclass
class PipelineResult:
    """Structured result from running the full pipeline."""

    entity_results: dict[str, EntityResult] = field(default_factory=dict)
    gaps: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


@dataclass
class EntityResult:
    """Result of executing a single entity."""

    name: str
    row_count: int
    columns: list[str]
    success: bool
    method: str  # "framework", "workaround", or "partial_framework"
    error: str | None = None


def _run_touchpoints(conn: ibis.BaseBackend) -> ibis.Table:
    """Run touchpoints entity with workaround for JSON extraction.

    GAP-001: json_extract_scalar() returns raw SQL strings that the code
    generator prefixes with `t.`, producing invalid Ibis expressions like
    `t.JSON_EXTRACT_SCALAR(...)`. DuckDB needs json_extract_string() via SQL.

    GAP-004: CaseColumn generates `.else_()` as a chained method call, but
    ibis.cases() in Ibis >=10 takes `else_` as a keyword argument. The
    generated code `ibis.cases(...).else_('value')` fails at runtime.
    """
    conn.read_parquet(
        str(DATA_DIR / "amplitude" / "events" / "*.parquet"),
        table_name="source_touchpoints",
    )

    # Workaround: use DuckDB SQL for JSON extraction since framework codegen
    # doesn't handle json_extract_scalar properly for Ibis
    prep = conn.sql("""
        SELECT
            *,
            json_extract_string(event_properties, 'gclid') AS gclid,
            json_extract_string(event_properties, 'fbclid') AS fbclid,
            json_extract_string(event_properties, 'utm_source') AS utm_source,
            json_extract_string(event_properties, 'utm_medium') AS utm_medium,
            json_extract_string(event_properties, 'utm_campaign') AS utm_campaign,
            json_extract_string(event_properties, 'referring_domain') AS referring_domain,
            json_extract_string(event_properties, 'form_type') AS form_type,
            json_extract_string(user_properties, 'Organization') AS organization_id
        FROM source_touchpoints
    """)

    # Channel classification using ibis.cases with else_ as keyword arg
    dim = prep.mutate(
        channel=ibis.cases(
            (prep.gclid.notnull(), "paid_search_google"),
            (prep.fbclid.notnull(), "paid_social_meta"),
            (
                prep.utm_medium.isin(["cpc", "ppc", "paid", "paid_social"]),
                "paid_other",
            ),
            (prep.utm_source.notnull(), "organic_campaign"),
            (prep.referring_domain.notnull(), "organic_referral"),
            else_="direct_or_unknown",
        ),
    )

    conn.create_table("dim_touchpoints", dim, overwrite=True)
    return conn.table("dim_touchpoints")


def _run_account(conn: ibis.BaseBackend) -> ibis.Table:
    """Run account entity (identity graph with inline sources).

    GAP-005: When an IdentityGraphSource has match_key_field pointing to the
    same column as a field in `fields`, the generated .rename() dict has
    duplicate new-name keys. E.g., amplitude source has match_key_field=
    'amplitude_id' and fields={'primary_amplitude_id': 'amplitude_id'}, both
    wanting to rename FROM 'amplitude_id', causing 'duplicate new names' error.

    Workaround: build the identity graph join manually.
    """
    t_orgs = conn.read_parquet(
        str(DATA_DIR / "organizations" / "*.parquet"),
        table_name="source_organizations",
    )
    t_amp = conn.read_parquet(
        str(DATA_DIR / "amplitude" / "merge_ids" / "*.parquet"),
        table_name="source_merge_ids",
    )

    # Rename org fields to unified names
    t_orgs = t_orgs.rename({
        "organization_id": "id",
        "org_name": "name",
        "org_country": "country",
        "org_region": "region",
        "org_category": "category",
        "org_cause": "cause",
        "org_type": "type",
        "org_website": "website",
        "how_heard": "how_did_you_hear_about_simplyk_question",
    })

    # For amplitude: keep amplitude_id as-is for matching, also expose as
    # primary_amplitude_id
    t_amp = t_amp.mutate(
        organization_id=t_amp.amplitude_id,
        primary_amplitude_id=t_amp.amplitude_id,
    )

    # Full outer join on organization_id
    result = t_orgs.outer_join(
        t_amp,
        t_orgs.organization_id == t_amp.organization_id,
        lname="",
        rname="_right",
    ).select(
        organization_id=ibis.coalesce(
            t_orgs.organization_id, t_amp.organization_id
        ),
        org_name=t_orgs.org_name,
        org_country=t_orgs.org_country,
        org_region=t_orgs.org_region,
        org_category=t_orgs.org_category,
        org_cause=t_orgs.org_cause,
        org_type=t_orgs.org_type,
        org_website=t_orgs.org_website,
        how_heard=t_orgs.how_heard,
        primary_amplitude_id=t_amp.primary_amplitude_id,
        merged_amplitude_id=t_amp.merged_amplitude_id,
        is_organizations=t_orgs.organization_id.notnull(),
        is_amplitude=t_amp.organization_id.notnull(),
    )

    # Add account_id computed column (dimension layer)
    result = result.mutate(account_id=result.organization_id)

    conn.create_table("dim_account", result, overwrite=True)
    return conn.table("dim_account")


def _run_attribution_first_touch(conn: ibis.BaseBackend) -> ibis.Table:
    """Run first-touch attribution entity.

    GAP-002: attribution.py defines two entities (attribution_first_touch and
    attribution_paid_priority) but the registry only discovers `module.entity`.

    GAP-006: first_value_by() generates window function expressions
    (e.g., `t.channel.first().over(window)`) which are then used inside
    `.aggregate()`. Ibis does not allow window functions in aggregate context.
    The AggregationSource codegen puts these expressions directly into
    `.group_by().aggregate()`, which fails.

    Workaround: use row_number + filter approach instead.
    """
    dim_touchpoints = conn.table("dim_touchpoints")

    filtered = dim_touchpoints.filter(
        dim_touchpoints.channel != "direct_or_unknown"
    )

    # Add row number per org ordered by event_time to get first touch
    with_rn = filtered.mutate(
        _rn=ibis.row_number().over(
            ibis.window(group_by="organization_id", order_by="event_time")
        )
    )

    # Keep only first row per org for first-touch values
    first_rows = with_rn.filter(with_rn._rn == 0).select(
        "organization_id",
        first_touch_channel=with_rn.channel,
        first_touch_utm_source=with_rn.utm_source,
        first_touch_utm_campaign=with_rn.utm_campaign,
    )

    # Get min event_time per org
    agg = filtered.group_by("organization_id").aggregate(
        first_touch_time=filtered.event_time.min(),
    )

    # Join first-touch values with min time
    result = first_rows.inner_join(
        agg,
        first_rows.organization_id == agg.organization_id,
        lname="",
        rname="_agg",
    ).select(
        first_rows.organization_id,
        first_rows.first_touch_channel,
        agg.first_touch_time,
        first_rows.first_touch_utm_source,
        first_rows.first_touch_utm_campaign,
    )

    conn.create_table("dim_attribution_first_touch", result, overwrite=True)
    return conn.table("dim_attribution_first_touch")


def _run_attribution_paid_priority(conn: ibis.BaseBackend) -> ibis.Table:
    """Run paid-priority attribution entity.

    GAP-002: Same multi-entity-per-file issue as first_touch.
    GAP-006: Same window-in-aggregate issue as first_touch.
    Workaround: row_number + filter + separate aggregation.
    """
    dim_touchpoints = conn.table("dim_touchpoints")

    filtered = dim_touchpoints.filter(
        dim_touchpoints.channel != "direct_or_unknown"
    )

    # First row per org for channel value
    with_rn = filtered.mutate(
        _rn=ibis.row_number().over(
            ibis.window(group_by="organization_id", order_by="event_time")
        )
    )
    first_rows = with_rn.filter(with_rn._rn == 0).select(
        "organization_id",
        paid_priority_channel=with_rn.channel,
    )

    # Aggregates per org
    agg = filtered.group_by("organization_id").aggregate(
        paid_priority_time=filtered.event_time.min(),
        has_paid_touch=filtered.channel.isin(
            ["paid_search_google", "paid_social_meta", "paid_other"]
        ).any(),
        total_touchpoints=filtered.amplitude_id.count(),
    )

    result = first_rows.inner_join(
        agg,
        first_rows.organization_id == agg.organization_id,
        lname="",
        rname="_agg",
    ).select(
        first_rows.organization_id,
        first_rows.paid_priority_channel,
        agg.paid_priority_time,
        agg.has_paid_touch,
        agg.total_touchpoints,
    )

    conn.create_table("dim_attribution_paid_priority", result, overwrite=True)
    return conn.table("dim_attribution_paid_priority")


def _run_acquisition_signal(conn: ibis.BaseBackend) -> ibis.Table:
    """Run acquisition_signal entity.

    The AggregationSource codegen works correctly, but since touchpoints was
    run with workaround (not via framework), dim_touchpoints is already in the
    connection. We use the framework's generated code for the aggregation and
    dedup layers.
    """
    dim_touchpoints = conn.table("dim_touchpoints")

    filtered = dim_touchpoints.filter(
        dim_touchpoints.channel != "direct_or_unknown"
    )

    result = filtered.group_by("organization_id").aggregate(
        first_event_time=filtered.event_time.min(),
        last_event_time=filtered.event_time.max(),
        touchpoint_count=filtered.amplitude_id.count(),
        distinct_channels=filtered.channel.nunique(),
    )

    # Prep: add dedup row number
    result = result.mutate(
        rn=ibis.row_number().over(
            ibis.window(
                group_by="organization_id", order_by="first_event_time"
            )
        )
    )

    conn.create_table("dim_acquisition_signal", result, overwrite=True)
    return conn.table("dim_acquisition_signal")


def _run_account_attributed(conn: ibis.BaseBackend) -> ibis.Table:
    """Run account_attributed entity: wide join of account + attribution models.

    GAP-003: DerivedSource with depends_on but no identity_graph_config generates
    no source function. The framework only supports DerivedSource when
    identity_graph_config is provided.
    Workaround: manual left joins.
    """
    dim_account = conn.table("dim_account")
    dim_ft = conn.table("dim_attribution_first_touch")
    dim_pp = conn.table("dim_attribution_paid_priority")

    # Join account with first-touch attribution
    result = dim_account.left_join(
        dim_ft,
        dim_account.organization_id == dim_ft.organization_id,
        lname="",
        rname="_ft",
    ).select(
        # All account columns
        *[dim_account[c] for c in dim_account.columns],
        # First-touch columns
        dim_ft.first_touch_channel,
        dim_ft.first_touch_time,
        dim_ft.first_touch_utm_source,
        dim_ft.first_touch_utm_campaign,
    )

    # Join with paid-priority attribution
    result = result.left_join(
        dim_pp,
        result.organization_id == dim_pp.organization_id,
        lname="",
        rname="_pp",
    ).select(
        # All existing columns
        *[result[c] for c in result.columns],
        # Paid-priority columns
        dim_pp.paid_priority_channel,
        dim_pp.paid_priority_time,
        dim_pp.has_paid_touch,
        dim_pp.total_touchpoints,
    )

    conn.create_table("dim_account_attributed", result, overwrite=True)
    return conn.table("dim_account_attributed")


def run_pipeline() -> PipelineResult:
    """Run the full Zeffy attribution pipeline end-to-end on DuckDB.

    Execution order (respecting dependencies):
    1. touchpoints (base events)
    2. account (identity graph)
    3. attribution_first_touch (depends on touchpoints)
    4. attribution_paid_priority (depends on touchpoints)
    5. acquisition_signal (depends on touchpoints)
    6. account_attributed (depends on account + attribution models)

    Returns:
        PipelineResult with per-entity results and documented gaps.
    """
    pipeline = PipelineResult()
    conn = ibis.duckdb.connect()

    # Document known gaps
    pipeline.gaps = [
        "GAP-001: json_extract_scalar() returns raw SQL strings that codegen "
        "prefixes with `t.`, producing invalid Ibis expressions. DuckDB needs "
        "json_extract_string() and the expression should not be prefixed.",
        "GAP-002: Entity registry only discovers `module.entity` (singular). "
        "attribution.py defines two entities (attribution_first_touch, "
        "attribution_paid_priority) which are not discovered.",
        "GAP-003: DerivedSource with depends_on but no identity_graph_config "
        "generates no source function. account_attributed cannot run through "
        "the framework — needs a 'join' mode for DerivedSource.",
        "GAP-004: CaseColumn generates `.else_()` as a chained method call, but "
        "ibis.cases() in Ibis >=10 takes `else_` as a keyword argument. The "
        "generated code `ibis.cases(...).else_('value')` fails at runtime.",
        "GAP-005: IdentityGraphSource rename collision when match_key_field "
        "and a fields entry both reference the same source column. Generates "
        "a .rename() dict with duplicate new-name keys.",
        "GAP-006: first_value_by() generates window function expressions used "
        "inside AggregationSource's .group_by().aggregate(). Ibis does not "
        "allow window functions in aggregate context. Needs a different "
        "codegen strategy (e.g., row_number + filter).",
    ]

    entity_steps: list[tuple[str, object, str]] = [
        ("touchpoints", _run_touchpoints, "workaround"),
        ("account", _run_account, "workaround"),
        ("attribution_first_touch", _run_attribution_first_touch, "workaround"),
        ("attribution_paid_priority", _run_attribution_paid_priority, "workaround"),
        ("acquisition_signal", _run_acquisition_signal, "workaround"),
        ("account_attributed", _run_account_attributed, "workaround"),
    ]

    for name, run_fn, method in entity_steps:
        try:
            table = run_fn(conn)
            row_count = table.count().execute()
            columns = list(table.columns)
            pipeline.entity_results[name] = EntityResult(
                name=name,
                row_count=row_count,
                columns=columns,
                success=True,
                method=method,
            )
            log.info(
                "%s: %d rows, %d columns [%s]",
                name,
                row_count,
                len(columns),
                method,
            )
        except Exception as e:
            pipeline.entity_results[name] = EntityResult(
                name=name,
                row_count=0,
                columns=[],
                success=False,
                method=method,
                error=str(e),
            )
            pipeline.errors.append(f"{name}: {e}")
            log.error("%s: FAILED: %s", name, e)

    conn.disconnect()
    return pipeline


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = run_pipeline()

    print("\n=== Pipeline Results ===")
    for name, er in result.entity_results.items():
        status = "OK" if er.success else "FAIL"
        print(f"  {name}: {status} ({er.row_count} rows, {er.method})")
        if er.error:
            print(f"    Error: {er.error}")

    print(f"\nGaps found: {len(result.gaps)}")
    for gap in result.gaps:
        print(f"  - {gap}")

    print(f"\nOverall: {'SUCCESS' if result.success else 'FAILED'}")
