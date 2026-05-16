from pathlib import Path

import pytest

from fyrnheim.config import ResolvedConfig
from fyrnheim.core.analytics_entity import AnalyticsEntity, Measure
from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.core.source import EventSource, StateSource
from fyrnheim.engine.materialization_policy import (
    UnsupportedWarehouseComputeError,
    assert_warehouse_compute_supported,
    find_warehouse_compute_findings,
    is_warehouse_backend,
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


def test_bigquery_analytics_entity_rejected_before_source_loading() -> None:
    entity = AnalyticsEntity(
        name="customers",
        measures=[Measure(name="opens", activity="opened", aggregation="count")],
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
    assert "AnalyticsEntity projection" in message
    assert "customers" in message
    assert "memtable" in message


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
