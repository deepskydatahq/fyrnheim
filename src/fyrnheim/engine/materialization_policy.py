"""Materialization and capability policy for warehouse execution.

Fyrnheim owns the semantics of each transformation phase. Libraries such as
Ibis, SQLGlot, and backend clients are implementation tools behind those
semantics. This module records which phases are warehouse-native, which
materialization boundaries they allow, and why unsupported phases fail fast.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from fyrnheim.core.analytics_entity import AnalyticsEntity
from fyrnheim.core.source import EventSource, StateSource

LOCAL_COMPUTE_BACKENDS = frozenset({"duckdb"})

PhaseName = Literal[
    "source_stage",
    "event_source",
    "state_source_diff",
    "activity",
    "identity",
    "metrics",
    "analytics_entity",
    "staging",
]
MaterializationPolicy = Literal[
    "expression_only",
    "final_output_only",
    "metadata_only",
    "backend_ddl",
    "local_compute_allowed",
    "unsupported",
]
CompilerTool = Literal["ibis", "sqlglot", "raw_sql", "backend_client", "pandas"]

CANONICAL_EVENT_SCHEMA = (
    "source:string",
    "entity_id:string",
    "ts:string",
    "event_type:string",
    "payload:string",
)


class UnsupportedWarehouseComputeError(RuntimeError):
    """Raised when a warehouse run would require local pandas compute."""


@dataclass(frozen=True)
class PhaseCapability:
    """Internal capability contract for one transformation phase.

    Attributes:
        phase: Stable internal phase name.
        backend: Backend this capability was evaluated for.
        input_schema: Human-readable expected input shape.
        output_schema: Human-readable produced output shape.
        materialization_policy: The materialization boundary allowed by this
            phase. Warehouse-native transformation phases should normally be
            ``expression_only`` until a caller reaches a final output boundary.
        compiler_tools: Implementation tools used behind Fyrnheim semantics.
        warehouse_native: Whether the phase can run on warehouse backends
            without local pandas transformation compute.
        reason: Short explanation for the support decision.
        suggestion: User-facing next step when unsupported.
    """

    phase: PhaseName
    backend: str
    input_schema: tuple[str, ...]
    output_schema: tuple[str, ...]
    materialization_policy: MaterializationPolicy
    compiler_tools: tuple[CompilerTool, ...]
    warehouse_native: bool
    reason: str
    suggestion: str = ""


@dataclass(frozen=True)
class WarehouseComputeFinding:
    """A warehouse-local-compute path found in a pipeline asset graph."""

    feature: str
    asset_name: str
    reason: str
    suggestion: str
    capability: PhaseCapability | None = None

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


def phase_capability(phase: PhaseName, *, backend: str) -> PhaseCapability:
    """Return the internal capability contract for ``phase`` on ``backend``."""
    normalized_backend = backend.lower()
    if allows_local_transform_compute(normalized_backend):
        return _local_phase_capability(phase, backend=normalized_backend)
    return _warehouse_phase_capability(phase, backend=normalized_backend)


def _local_phase_capability(phase: PhaseName, *, backend: str) -> PhaseCapability:
    base = _phase_shape(phase)
    return PhaseCapability(
        phase=phase,
        backend=backend,
        input_schema=base[0],
        output_schema=base[1],
        materialization_policy="local_compute_allowed",
        compiler_tools=base[2],
        warehouse_native=True,
        reason="DuckDB/parquet local development may use local compatibility paths",
    )


def _warehouse_phase_capability(phase: PhaseName, *, backend: str) -> PhaseCapability:
    input_schema, output_schema, compiler_tools = _phase_shape(phase)
    if phase == "analytics_entity":
        return PhaseCapability(
            phase=phase,
            backend=backend,
            input_schema=input_schema,
            output_schema=output_schema,
            materialization_policy="expression_only",
            compiler_tools=("ibis",),
            warehouse_native=True,
            reason=(
                "state fields and measures compose as backend-executable Ibis "
                "expressions when the entity does not declare Python-only "
                "computed_fields"
            ),
        )

    policy: MaterializationPolicy = "expression_only"
    if phase == "staging":
        policy = "backend_ddl"
    return PhaseCapability(
        phase=phase,
        backend=backend,
        input_schema=input_schema,
        output_schema=output_schema,
        materialization_policy=policy,
        compiler_tools=compiler_tools,
        warehouse_native=True,
        reason="phase composes backend-executable expressions or backend DDL",
    )


def _phase_shape(
    phase: PhaseName,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[CompilerTool, ...]]:
    """Return input schema, output schema, and implementation tools."""
    if phase == "source_stage":
        return (
            ("raw source table",),
            ("post-stage source table",),
            ("ibis",),
        )
    if phase == "event_source":
        return (("post-stage EventSource table",), CANONICAL_EVENT_SCHEMA, ("ibis",))
    if phase == "state_source_diff":
        return (
            ("current StateSource table", "previous snapshot table | None"),
            CANONICAL_EVENT_SCHEMA,
            ("ibis",),
        )
    if phase == "activity":
        return (CANONICAL_EVENT_SCHEMA, CANONICAL_EVENT_SCHEMA, ("ibis",))
    if phase == "identity":
        return (
            CANONICAL_EVENT_SCHEMA,
            (*CANONICAL_EVENT_SCHEMA, "canonical_id:string"),
            ("ibis",),
        )
    if phase == "metrics":
        return (
            (*CANONICAL_EVENT_SCHEMA, "canonical_id:string?"),
            ("metrics model output table",),
            ("ibis",),
        )
    if phase == "analytics_entity":
        return (
            (*CANONICAL_EVENT_SCHEMA, "canonical_id:string?"),
            ("analytics entity output table",),
            ("ibis",),
        )
    if phase == "staging":
        return (("staging SQL",), ("warehouse view/table",), ("raw_sql", "backend_client"))
    raise AssertionError(f"Unhandled phase: {phase}")


def capabilities_for_assets(
    *, backend: str, assets: dict[str, list[Any]]
) -> list[PhaseCapability]:
    """Return phase capabilities implied by an asset graph.

    This is intentionally phase-oriented rather than public-API-oriented:
    asset types imply one or more transformation phases, and the policy layer
    explains support at that phase boundary.
    """
    phases: list[PhaseName] = []
    sources = assets.get("sources", [])
    if sources:
        phases.append("source_stage")
    if any(isinstance(source, EventSource) for source in sources):
        phases.append("event_source")
    if any(isinstance(source, StateSource) for source in sources):
        phases.append("state_source_diff")
    if assets.get("activities"):
        phases.append("activity")
    if assets.get("identity_graphs"):
        phases.append("identity")
    if assets.get("metrics_models"):
        phases.append("metrics")
    if assets.get("analytics_entities"):
        phases.append("analytics_entity")
    if assets.get("staging_views"):
        phases.append("staging")

    # Preserve first occurrence order while deduplicating.
    unique_phases = list(dict.fromkeys(phases))
    return [phase_capability(phase, backend=backend) for phase in unique_phases]


def find_warehouse_compute_findings(
    *, backend: str, assets: dict[str, list[Any]]
) -> list[WarehouseComputeFinding]:
    """Return asset-level warehouse compute-contract violations."""
    if not is_warehouse_backend(backend):
        return []

    capabilities = capabilities_for_assets(backend=backend, assets=assets)
    by_phase = {capability.phase: capability for capability in capabilities}
    findings: list[WarehouseComputeFinding] = []

    analytics_capability = by_phase.get("analytics_entity")
    if analytics_capability is not None:
        for entity in assets.get("analytics_entities", []):
            if isinstance(entity, AnalyticsEntity) and entity.computed_fields:
                findings.append(
                    WarehouseComputeFinding(
                        feature="AnalyticsEntity computed_fields",
                        asset_name=entity.name,
                        reason=(
                            "computed_fields currently require Python row "
                            "evaluation after projection"
                        ),
                        suggestion=(
                            "remove computed_fields or wait for a supported "
                            "Ibis expression subset before running this entity "
                            "on a warehouse backend"
                        ),
                        capability=analytics_capability,
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
