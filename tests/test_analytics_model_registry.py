"""Tests for AnalyticsModelRegistry."""

from __future__ import annotations

from pathlib import Path

import pytest

from fyrnheim.engine.analytics_model_registry import AnalyticsModelRegistry


@pytest.fixture
def tmp_models_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for analytics model files."""
    models_dir = tmp_path / "analytics_models"
    models_dir.mkdir()
    return models_dir


def _write_model_file(models_dir: Path, filename: str, content: str) -> Path:
    """Write a Python file to the models directory."""
    filepath = models_dir / filename
    filepath.write_text(content)
    return filepath


class TestDiscover:
    """Test AnalyticsModelRegistry.discover()."""

    def test_discover_single_analytics_model(self, tmp_models_dir: Path):
        _write_model_file(
            tmp_models_dir,
            "daily_metrics.py",
            """\
from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric

analytics_model = StreamAnalyticsModel(
    name="daily_metrics",
    identity_graph="test_graph",
    date_grain="daily",
    metrics=[
        StreamMetric(name="signups", expression="*", metric_type="count", event_filter="signup"),
    ],
)
""",
        )

        registry = AnalyticsModelRegistry()
        registry.discover(tmp_models_dir)

        assert len(registry) == 1
        assert "daily_metrics" in registry

    def test_discover_analytics_models_list(self, tmp_models_dir: Path):
        _write_model_file(
            tmp_models_dir,
            "metrics.py",
            """\
from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric

analytics_models = [
    StreamAnalyticsModel(
        name="daily_signups",
        identity_graph="test_graph",
        date_grain="daily",
        metrics=[StreamMetric(name="signups", expression="*", metric_type="count")],
    ),
    StreamAnalyticsModel(
        name="weekly_revenue",
        identity_graph="test_graph",
        date_grain="weekly",
        metrics=[StreamMetric(name="revenue", expression="amount", metric_type="sum")],
    ),
]
""",
        )

        registry = AnalyticsModelRegistry()
        registry.discover(tmp_models_dir)

        assert len(registry) == 2
        assert "daily_signups" in registry
        assert "weekly_revenue" in registry


class TestGet:
    """Test AnalyticsModelRegistry.get()."""

    def test_get_returns_model_by_name(self, tmp_models_dir: Path):
        _write_model_file(
            tmp_models_dir,
            "daily_metrics.py",
            """\
from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric

analytics_model = StreamAnalyticsModel(
    name="daily_metrics",
    identity_graph="test_graph",
    date_grain="daily",
    metrics=[StreamMetric(name="signups", expression="*", metric_type="count")],
)
""",
        )

        registry = AnalyticsModelRegistry()
        registry.discover(tmp_models_dir)

        model = registry.get("daily_metrics")
        assert model.name == "daily_metrics"
        assert model.date_grain == "daily"

    def test_get_raises_key_error_for_unknown_name(self):
        registry = AnalyticsModelRegistry()
        with pytest.raises(KeyError, match="not found"):
            registry.get("nonexistent")


class TestDuplicateNames:
    """Test duplicate name detection."""

    def test_raises_value_error_on_duplicate_names(self, tmp_models_dir: Path):
        _write_model_file(
            tmp_models_dir,
            "metrics_a.py",
            """\
from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric

analytics_model = StreamAnalyticsModel(
    name="daily_metrics",
    identity_graph="test_graph",
    date_grain="daily",
    metrics=[StreamMetric(name="signups", expression="*", metric_type="count")],
)
""",
        )
        _write_model_file(
            tmp_models_dir,
            "metrics_b.py",
            """\
from fyrnheim.core.analytics_model import StreamAnalyticsModel, StreamMetric

analytics_model = StreamAnalyticsModel(
    name="daily_metrics",
    identity_graph="test_graph",
    date_grain="daily",
    metrics=[StreamMetric(name="signups", expression="*", metric_type="count")],
)
""",
        )

        registry = AnalyticsModelRegistry()
        with pytest.raises(ValueError, match="Duplicate analytics model name"):
            registry.discover(tmp_models_dir)
