"""Materialization policy for warehouse-backed pipeline execution.

Fyrnheim supports local transformation compute on DuckDB/parquet fixtures.
Warehouse backends are different: source and intermediate transformations must
remain backend-executable Ibis expressions until an explicit final output
boundary. This module centralizes the guardrails that prevent accidental
warehouse-data downloads into pandas transformation paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fyrnheim.core.analytics_entity import AnalyticsEntity
from fyrnheim.core.source import StateSource

LOCAL_COMPUTE_BACKENDS = frozenset({"duckdb"})


class UnsupportedWarehouseComputeError(RuntimeError):
    """Raised when a warehouse run would require local pandas compute."""


@dataclass(frozen=True)
class WarehouseComputeFinding:
    """A warehouse-local-compute path found in a pipeline asset graph."""

    feature: str
    asset_name: str
    reason: str
    suggestion: str

    def render(self) -> str:
        return (
            f"{self.feature} {self.asset_name!r} is not supported on warehouse "
            f"backends: {self.reason}. {self.suggestion}"
        )


def allows_local_transform_compute(backend: str) -> bool:
    """Return True when intermediate pandas transformation compute is allowed."""
    return backend.lower() in LOCAL_COMPUTE_BACKENDS


def is_warehouse_backend(backend: str) -> bool:
    """Return True when a backend must honor the warehouse compute contract."""
    return not allows_local_transform_compute(backend)


def find_warehouse_compute_findings(
    *, backend: str, assets: dict[str, list[Any]]
) -> list[WarehouseComputeFinding]:
    """Return asset-level warehouse compute-contract violations.

    The first M091 guard is intentionally conservative and blocks known engine
    paths that still construct transformation results through pandas:

    * StateSource diff/full_refresh builds row events from pandas DataFrames.
    * AnalyticsEntity projection executes the aggregate result locally for JSON
      post-processing/computed fields and wraps it as an ``ibis.memtable``.

    EventSource, activities, identity, metrics, and staging views remain allowed
    by this policy because their v0.14 paths compose Ibis expressions and only
    materialize at final output/write boundaries.
    """
    if not is_warehouse_backend(backend):
        return []

    findings: list[WarehouseComputeFinding] = []
    for source in assets.get("sources", []):
        if isinstance(source, StateSource):
            findings.append(
                WarehouseComputeFinding(
                    feature="StateSource snapshot/full_refresh",
                    asset_name=source.name,
                    reason=(
                        "snapshot diff and full_refresh still materialize rows "
                        "into pandas to build row_appeared/field_changed events"
                    ),
                    suggestion=(
                        "use an EventSource or a StagingView-backed event stream "
                        "for warehouse runs until StateSource diff is rewritten "
                        "as backend-native Ibis/SQL"
                    ),
                )
            )

    for entity in assets.get("analytics_entities", []):
        if isinstance(entity, AnalyticsEntity):
            findings.append(
                WarehouseComputeFinding(
                    feature="AnalyticsEntity projection",
                    asset_name=entity.name,
                    reason=(
                        "projection currently executes the post-aggregation "
                        "result in Python for JSON value parsing and computed "
                        "field evaluation, then re-registers it as a memtable"
                    ),
                    suggestion=(
                        "use MetricsModel outputs or wait for a fully Ibis-native "
                        "AnalyticsEntity projection before running this asset on "
                        "a warehouse backend"
                    ),
                )
            )

    return findings


def assert_warehouse_compute_supported(
    *, backend: str, assets: dict[str, list[Any]]
) -> None:
    """Raise if a warehouse run would require local transformation compute."""
    findings = find_warehouse_compute_findings(backend=backend, assets=assets)
    if not findings:
        return

    rendered = "\n".join(f"- {finding.render()}" for finding in findings)
    raise UnsupportedWarehouseComputeError(
        "Warehouse-backed Fyrnheim pipelines cannot download source or "
        "intermediate data for pandas transformation compute. The following "
        f"asset(s) would violate that contract on backend {backend!r}:\n"
        f"{rendered}"
    )
