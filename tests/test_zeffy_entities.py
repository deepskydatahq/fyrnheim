"""Tests for Zeffy attribution PoC entity definitions.

Validates that all entities load correctly, have expected source types,
and key configuration details match expectations.
"""

import importlib.util
from pathlib import Path

from fyrnheim import TableSource
from fyrnheim.components.expressions import CaseColumn
from fyrnheim.core.source import AggregationSource, DerivedSource

_ENTITIES_DIR = Path(__file__).parent.parent / "examples" / "zeffy-attribution" / "entities"


def _load_entity_module(name: str):
    """Load an entity module by name using importlib to avoid sys.path conflicts."""
    path = _ENTITIES_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"zeffy_entities.{name}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestEntityImports:
    """All entities import and validate without errors."""

    def test_touchpoints_loads(self) -> None:
        mod = _load_entity_module("touchpoints")
        assert mod.entity.name == "touchpoints"

    def test_account_loads(self) -> None:
        mod = _load_entity_module("account")
        assert mod.entity.name == "account"

    def test_acquisition_signal_loads(self) -> None:
        mod = _load_entity_module("acquisition_signal")
        assert mod.entity.name == "acquisition_signal"

    def test_attribution_first_touch_loads(self) -> None:
        mod = _load_entity_module("attribution")
        assert mod.attribution_first_touch.name == "attribution_first_touch"

    def test_attribution_paid_priority_loads(self) -> None:
        mod = _load_entity_module("attribution")
        assert mod.attribution_paid_priority.name == "attribution_paid_priority"

    def test_account_attributed_loads(self) -> None:
        mod = _load_entity_module("account_attributed")
        assert mod.entity.name == "account_attributed"


class TestSourceTypes:
    """Each entity has the expected source type."""

    def test_touchpoints_is_table_source(self) -> None:
        mod = _load_entity_module("touchpoints")
        assert isinstance(mod.entity.source, TableSource)

    def test_account_is_derived_source(self) -> None:
        mod = _load_entity_module("account")
        assert isinstance(mod.entity.source, DerivedSource)

    def test_acquisition_signal_is_aggregation_source(self) -> None:
        mod = _load_entity_module("acquisition_signal")
        assert isinstance(mod.entity.source, AggregationSource)

    def test_attribution_first_touch_is_aggregation_source(self) -> None:
        mod = _load_entity_module("attribution")
        assert isinstance(mod.attribution_first_touch.source, AggregationSource)

    def test_attribution_paid_priority_is_aggregation_source(self) -> None:
        mod = _load_entity_module("attribution")
        assert isinstance(mod.attribution_paid_priority.source, AggregationSource)

    def test_account_attributed_is_derived_source(self) -> None:
        mod = _load_entity_module("account_attributed")
        assert isinstance(mod.entity.source, DerivedSource)


class TestTouchpointsChannel:
    """Touchpoints channel CaseColumn generates expected expression."""

    def _get_channel_col(self):
        mod = _load_entity_module("touchpoints")
        dim_columns = mod.entity.layers.dimension.computed_columns
        return next(c for c in dim_columns if c.name == "channel")

    def test_channel_case_column_exists(self) -> None:
        mod = _load_entity_module("touchpoints")
        dim_columns = mod.entity.layers.dimension.computed_columns
        channel_cols = [c for c in dim_columns if c.name == "channel"]
        assert len(channel_cols) == 1
        assert isinstance(channel_cols[0], CaseColumn)

    def test_channel_expression_contains_ibis_cases(self) -> None:
        col = self._get_channel_col()
        assert "ibis.cases(" in col.expression

    def test_channel_expression_has_paid_search_google(self) -> None:
        col = self._get_channel_col()
        assert "paid_search_google" in col.expression

    def test_channel_expression_has_default(self) -> None:
        col = self._get_channel_col()
        assert "direct_or_unknown" in col.expression

    def test_channel_has_five_cases(self) -> None:
        col = self._get_channel_col()
        assert isinstance(col, CaseColumn)
        assert len(col.cases) == 5


class TestAccountIdentityGraph:
    """Account IdentityGraphConfig has correct match_key and source names."""

    def _get_config(self):
        mod = _load_entity_module("account")
        return mod.entity.source.identity_graph_config

    def test_match_key_is_organization_id(self) -> None:
        config = self._get_config()
        assert config.match_key == "organization_id"

    def test_has_two_sources(self) -> None:
        config = self._get_config()
        assert len(config.sources) == 2

    def test_source_names(self) -> None:
        config = self._get_config()
        names = [s.name for s in config.sources]
        assert "organizations" in names
        assert "amplitude" in names

    def test_priority_order(self) -> None:
        config = self._get_config()
        assert config.priority == ["organizations", "amplitude"]

    def test_organizations_source_has_inline_table_source(self) -> None:
        config = self._get_config()
        org_source = next(s for s in config.sources if s.name == "organizations")
        assert org_source.source is not None
        assert isinstance(org_source.source, TableSource)
        assert org_source.entity is None

    def test_organizations_match_key_field(self) -> None:
        config = self._get_config()
        org_source = next(s for s in config.sources if s.name == "organizations")
        assert org_source.match_key_field == "id"
