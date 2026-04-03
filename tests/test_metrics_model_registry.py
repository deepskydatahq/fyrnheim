"""Tests for MetricsModelRegistry and e2e metrics aggregation."""

import json
import textwrap

import ibis
import pandas as pd
import pytest

from fyrnheim.core.metrics_model import MetricField, MetricsModel
from fyrnheim.engine.metrics_engine import aggregate_metrics
from fyrnheim.engine.metrics_model_registry import MetricsModelRegistry


class TestMetricsModelRegistry:
    def test_discover_single_model(self, tmp_path):
        models_dir = tmp_path / "metrics"
        models_dir.mkdir()
        (models_dir / "yt.py").write_text(
            textwrap.dedent("""\
                from fyrnheim.core.metrics_model import MetricField, MetricsModel

                metrics_model = MetricsModel(
                    name="yt_daily",
                    source="youtube",
                    grain="daily",
                    metric_fields=[
                        MetricField(field_name="view_count", aggregation="sum_delta"),
                    ],
                )
            """)
        )
        registry = MetricsModelRegistry()
        registry.discover(models_dir)
        assert len(registry) == 1
        assert "yt_daily" in registry
        model = registry.get("yt_daily")
        assert model.source == "youtube"

    def test_discover_list_of_models(self, tmp_path):
        models_dir = tmp_path / "metrics"
        models_dir.mkdir()
        (models_dir / "multi.py").write_text(
            textwrap.dedent("""\
                from fyrnheim.core.metrics_model import MetricField, MetricsModel

                metrics_models = [
                    MetricsModel(
                        name="model_a",
                        source="src_a",
                        grain="daily",
                        metric_fields=[
                            MetricField(field_name="x", aggregation="sum_delta"),
                        ],
                    ),
                    MetricsModel(
                        name="model_b",
                        source="src_b",
                        grain="weekly",
                        metric_fields=[
                            MetricField(field_name="y", aggregation="max_value"),
                        ],
                    ),
                ]
            """)
        )
        registry = MetricsModelRegistry()
        registry.discover(models_dir)
        assert len(registry) == 2
        assert "model_a" in registry
        assert "model_b" in registry

    def test_get_raises_on_missing(self):
        registry = MetricsModelRegistry()
        with pytest.raises(KeyError, match="not found"):
            registry.get("nonexistent")

    def test_duplicate_raises(self, tmp_path):
        models_dir = tmp_path / "metrics"
        models_dir.mkdir()
        for fname in ("a.py", "b.py"):
            (models_dir / fname).write_text(
                textwrap.dedent("""\
                    from fyrnheim.core.metrics_model import MetricField, MetricsModel

                    metrics_model = MetricsModel(
                        name="duplicate",
                        source="src",
                        grain="daily",
                        metric_fields=[
                            MetricField(field_name="x", aggregation="sum_delta"),
                        ],
                    )
                """)
            )
        registry = MetricsModelRegistry()
        with pytest.raises(ValueError, match="Duplicate"):
            registry.discover(models_dir)

    def test_discover_missing_dir(self):
        registry = MetricsModelRegistry()
        with pytest.raises(FileNotFoundError):
            registry.discover("/nonexistent/path")

    def test_all_returns_list(self, tmp_path):
        models_dir = tmp_path / "metrics"
        models_dir.mkdir()
        (models_dir / "m.py").write_text(
            textwrap.dedent("""\
                from fyrnheim.core.metrics_model import MetricField, MetricsModel

                metrics_model = MetricsModel(
                    name="test",
                    source="src",
                    grain="daily",
                    metric_fields=[
                        MetricField(field_name="x", aggregation="sum_delta"),
                    ],
                )
            """)
        )
        registry = MetricsModelRegistry()
        registry.discover(models_dir)
        models = registry.all()
        assert len(models) == 1
        assert models[0].name == "test"

    def test_items(self, tmp_path):
        models_dir = tmp_path / "metrics"
        models_dir.mkdir()
        (models_dir / "m.py").write_text(
            textwrap.dedent("""\
                from fyrnheim.core.metrics_model import MetricField, MetricsModel

                metrics_model = MetricsModel(
                    name="test",
                    source="src",
                    grain="daily",
                    metric_fields=[
                        MetricField(field_name="x", aggregation="sum_delta"),
                    ],
                )
            """)
        )
        registry = MetricsModelRegistry()
        registry.discover(models_dir)
        items = list(registry.items())
        assert len(items) == 1
        assert items[0][0] == "test"


def _payload(field_name: str, old_value: str, new_value: str) -> str:
    return json.dumps(
        {"field_name": field_name, "old_value": old_value, "new_value": new_value}
    )


class TestMetricsE2E:
    """End-to-end test: field_changed events -> sum_delta per day."""

    def test_e2e_sum_delta_multiple_entities(self):
        """
        Day 1: Video A view_count 100->150 (delta=50), Video B view_count 50->80 (delta=30)
        Day 2: Video A view_count 150->200 (delta=50), Video C view_count 200->250 (delta=50)
        Expected: day 1 sum_delta=80, day 2 sum_delta=100
        """
        events = ibis.memtable(
            pd.DataFrame(
                [
                    {
                        "source": "youtube",
                        "entity_id": "video_a",
                        "ts": "2024-01-01T10:00:00",
                        "event_type": "field_changed",
                        "payload": _payload("view_count", "100", "150"),
                    },
                    {
                        "source": "youtube",
                        "entity_id": "video_b",
                        "ts": "2024-01-01T14:00:00",
                        "event_type": "field_changed",
                        "payload": _payload("view_count", "50", "80"),
                    },
                    {
                        "source": "youtube",
                        "entity_id": "video_a",
                        "ts": "2024-01-02T10:00:00",
                        "event_type": "field_changed",
                        "payload": _payload("view_count", "150", "200"),
                    },
                    {
                        "source": "youtube",
                        "entity_id": "video_c",
                        "ts": "2024-01-02T12:00:00",
                        "event_type": "field_changed",
                        "payload": _payload("view_count", "200", "250"),
                    },
                ]
            )
        )

        model = MetricsModel(
            name="yt_daily_views",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
            ],
        )

        result = aggregate_metrics(events, model).execute()

        assert len(result) == 2
        day1 = result[result["_date"] == "2024-01-01"]
        day2 = result[result["_date"] == "2024-01-02"]
        assert day1["view_count_sum_delta"].iloc[0] == 80.0
        assert day2["view_count_sum_delta"].iloc[0] == 100.0

    def test_e2e_multiple_metrics_with_dimensions(self):
        """E2e with multiple aggregation types and entity_id dimension."""
        events = ibis.memtable(
            pd.DataFrame(
                [
                    {
                        "source": "youtube",
                        "entity_id": "video_a",
                        "ts": "2024-01-01T08:00:00",
                        "event_type": "field_changed",
                        "payload": _payload("view_count", "100", "200"),
                    },
                    {
                        "source": "youtube",
                        "entity_id": "video_a",
                        "ts": "2024-01-01T20:00:00",
                        "event_type": "field_changed",
                        "payload": _payload("view_count", "200", "350"),
                    },
                    {
                        "source": "youtube",
                        "entity_id": "video_b",
                        "ts": "2024-01-01T10:00:00",
                        "event_type": "field_changed",
                        "payload": _payload("view_count", "50", "80"),
                    },
                ]
            )
        )

        model = MetricsModel(
            name="yt_entity_daily",
            source="youtube",
            grain="daily",
            metric_fields=[
                MetricField(field_name="view_count", aggregation="sum_delta"),
                MetricField(field_name="view_count", aggregation="last_value"),
                MetricField(field_name="view_count", aggregation="max_value"),
            ],
            dimensions=["entity_id"],
        )

        result = aggregate_metrics(events, model).execute()

        va = result[result["entity_id"] == "video_a"]
        vb = result[result["entity_id"] == "video_b"]

        # video_a: sum_delta = (200-100) + (350-200) = 250
        assert va["view_count_sum_delta"].iloc[0] == 250.0
        # video_a: last_value = 350 (from the 20:00 event)
        assert va["view_count_last_value"].iloc[0] == 350.0
        # video_a: max_value = 350
        assert va["view_count_max_value"].iloc[0] == 350.0

        # video_b: sum_delta = 30
        assert vb["view_count_sum_delta"].iloc[0] == 30.0
        # video_b: last_value = 80
        assert vb["view_count_last_value"].iloc[0] == 80.0
        # video_b: max_value = 80
        assert vb["view_count_max_value"].iloc[0] == 80.0
