from pathlib import Path

import pytest

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.config import ResolvedConfig
from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.engine.materialization_policy import (
    UnsupportedWarehouseComputeError,
    assert_warehouse_compute_supported,
    capabilities_for_assets,
    find_warehouse_compute_findings,
    is_warehouse_backend,
    phase_capability,
)
from fyrnheim.engine.pipeline import run_pipeline


class _NoSourceLoadExecutor:
    @property
    def connection(self):
        class _Conn:
            pass

        return _Conn()


def _config(backend: str) -> ResolvedConfig:
    return ResolvedConfig(
        entities_dir=Path("entities"),
        data_dir=Path("data"),
        output_dir=Path("generated"),
        backend=backend,
        project_root=Path("."),
    )


def test_phase_capability_records_contract_fields() -> None:
    capability = phase_capability("metrics", backend="bigquery")

    assert capability.phase == "metrics"
    assert capability.backend == "bigquery"
    assert "source:string" in capability.input_schema
    assert capability.output_schema == ("metrics model output table",)
    assert capability.materialization_policy == "expression_only"
    assert capability.compiler_tools == ("ibis",)
    assert capability.warehouse_native is True


def test_bigquery_analytics_entity_computed_fields_rejected_before_source_loading() -> None:
    entity = AnalyticsEntity(
        name="customers",
        measures=[Measure(name="opens", activity="opened", aggregation="count")],
        computed_fields=[ComputedColumn(name="double_opens", expression="opens * 2")],
    )
    source = EventSource(
        name="events",
        project="p",
        dataset="d",
        table="events",
        entity_id_field="customer_id",
        timestamp_field="ts",
        event_type="opened",
    )

    with pytest.raises(UnsupportedWarehouseComputeError) as exc_info:
        run_pipeline(
            {"sources": [source], "analytics_entities": [entity]},
            _config("bigquery"),
            _NoSourceLoadExecutor(),  # type: ignore[arg-type]
        )

    message = str(exc_info.value)
    assert "AnalyticsEntity computed_fields" in message
    assert "customers" in message
    assert "computed_fields" in message


def test_bigquery_state_source_is_policy_allowed_after_native_diff() -> None:
    source = StateSource(
        name="accounts",
        project="p",
        dataset="d",
        table="accounts",
        id_field="id",
    )

    findings = find_warehouse_compute_findings(
        backend="bigquery", assets={"sources": [source]}
    )

    assert findings == []
    assert_warehouse_compute_supported(backend="bigquery", assets={"sources": [source]})


def test_bigquery_asset_capabilities_cover_core_phases() -> None:
    event_source = EventSource(
        name="events",
        project="p",
        dataset="d",
        table="events",
        entity_id_field="customer_id",
        timestamp_field="ts",
        event_type="opened",
    )
    state_source = StateSource(
        name="accounts",
        project="p",
        dataset="d",
        table="accounts",
        id_field="id",
    )
    entity = AnalyticsEntity(
        name="accounts_entity",
        measures=[Measure(name="opens", activity="opened", aggregation="count")],
    )
    metrics = MetricsModel(
        name="event_counts",
        sources=["events"],
        grain="daily",
        metric_fields=[MetricField(field_name="opened", aggregation="count")],
    )

    capabilities = capabilities_for_assets(
        backend="bigquery",
        assets={
            "sources": [event_source, state_source],
            "activities": [object()],
            "identity_graphs": [object()],
            "metrics_models": [metrics],
            "analytics_entities": [entity],
            "staging_views": [object()],
        },
    )
    by_phase = {capability.phase: capability for capability in capabilities}

    assert by_phase["source_stage"].warehouse_native is True
    assert by_phase["event_source"].warehouse_native is True
    assert by_phase["state_source_diff"].warehouse_native is True
    assert by_phase["activity"].warehouse_native is True
    assert by_phase["identity"].warehouse_native is True
    assert by_phase["metrics"].warehouse_native is True
    assert by_phase["staging"].materialization_policy == "backend_ddl"
    assert by_phase["analytics_entity"].warehouse_native is True
    assert by_phase["analytics_entity"].materialization_policy == "expression_only"
    assert by_phase["analytics_entity"].compiler_tools == ("ibis",)


def test_bigquery_analytics_entity_without_computed_fields_is_policy_allowed() -> None:
    entity = AnalyticsEntity(
        name="customers",
        measures=[Measure(name="opens", activity="opened", aggregation="count")],
    )

    findings = find_warehouse_compute_findings(
        backend="bigquery", assets={"analytics_entities": [entity]}
    )

    assert findings == []
    assert_warehouse_compute_supported(
        backend="bigquery", assets={"analytics_entities": [entity]}
    )


def test_bigquery_event_source_metrics_only_is_policy_allowed() -> None:
    source = EventSource(
        name="events",
        project="p",
        dataset="d",
        table="events",
        entity_id_field="customer_id",
        timestamp_field="ts",
        event_type="opened",
    )
    metrics = MetricsModel(
        name="event_counts",
        sources=["events"],
        grain="daily",
        metric_fields=[MetricField(field_name="opened", aggregation="count")],
    )

    findings = find_warehouse_compute_findings(
        backend="bigquery", assets={"sources": [source], "metrics_models": [metrics]}
    )

    assert findings == []
    assert_warehouse_compute_supported(
        backend="bigquery", assets={"sources": [source], "metrics_models": [metrics]}
    )


def test_duckdb_state_source_and_analytics_entity_are_local_compute_allowed() -> None:
    source = StateSource(
        name="accounts",
        project="p",
        dataset="d",
        table="accounts",
        duckdb_path="accounts.parquet",
        id_field="id",
    )
    entity = AnalyticsEntity(
        name="accounts_entity",
        measures=[Measure(name="opens", activity="opened", aggregation="count")],
    )

    assert not is_warehouse_backend("duckdb")
    assert find_warehouse_compute_findings(
        backend="duckdb", assets={"sources": [source], "analytics_entities": [entity]}
    ) == []
    assert_warehouse_compute_supported(
        backend="duckdb", assets={"sources": [source], "analytics_entities": [entity]}
    )
