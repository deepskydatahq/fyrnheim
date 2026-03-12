"""Tests for HelperEntity validation in the runner."""

import pytest

from fyrnheim import (
    AggregationSource,
    DerivedSource,
    DimensionLayer,
    Entity,
    LayersConfig,
    PrepLayer,
    TableSource,
)
from fyrnheim.core.entity import HelperEntity
from fyrnheim.engine.runner import validate_helper_entities

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_helper(name: str) -> HelperEntity:
    """Create a minimal HelperEntity with a TableSource."""
    return HelperEntity(
        name=name,
        description=f"Helper entity {name}",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=TableSource(project="p", dataset="d", table=name),
    )


def _make_entity(name: str, *, depends_on: list[str] | None = None) -> Entity:
    """Create a regular Entity with optional DerivedSource depends_on."""
    if depends_on:
        source = DerivedSource(identity_graph="default", depends_on=depends_on)
    else:
        source = TableSource(project="p", dataset="d", table=name)
    return Entity(
        name=name,
        description=f"Entity {name}",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=source,
    )


def _make_entity_with_aggregation(name: str, source_entity: str) -> Entity:
    """Create an Entity with AggregationSource referencing another entity."""
    return Entity(
        name=name,
        description=f"Entity {name}",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=AggregationSource(
            source_entity=source_entity,
            group_by_column="id",
        ),
    )


# ---------------------------------------------------------------------------
# Tests: HelperEntity class
# ---------------------------------------------------------------------------


class TestHelperEntityClass:
    def test_helper_entity_is_entity_subclass(self):
        helper = _make_helper("mapping")
        assert isinstance(helper, Entity)
        assert isinstance(helper, HelperEntity)

    def test_helper_entity_rejects_non_prep_layers(self):
        from fyrnheim import DimensionLayer

        with pytest.raises(ValueError, match="only supports the prep layer"):
            HelperEntity(
                name="bad_helper",
                description="Bad helper",
                layers=LayersConfig(
                    prep=PrepLayer(model_name="prep_bad"),
                    dimension=DimensionLayer(model_name="dim_bad"),
                ),
                source=TableSource(project="p", dataset="d", table="bad"),
            )

    def test_helper_entity_requires_prep(self):
        """HelperEntity must have prep layer configured."""
        # Entity base class requires at least one layer, and HelperEntity
        # further requires it to be prep specifically. But since LayersConfig
        # requires at least one layer, we can't easily create one without prep
        # and without any other layer. This test verifies the validator message.
        # Actually, we can test this indirectly: if someone manages to pass
        # only a non-prep layer, both validators fire.
        with pytest.raises(ValueError):
            HelperEntity(
                name="no_prep",
                description="No prep",
                layers=LayersConfig(
                    dimension=DimensionLayer(model_name="dim_x"),
                ),
                source=TableSource(project="p", dataset="d", table="x"),
            )


# ---------------------------------------------------------------------------
# Tests: validate_helper_entities
# ---------------------------------------------------------------------------


class TestValidateHelperEntities:
    def test_orphaned_helper_raises(self):
        """Orphaned HelperEntity (not in any depends_on) raises ValueError."""
        helper = _make_helper("orphan_helper")
        regular = _make_entity("regular")

        with pytest.raises(ValueError, match="not referenced"):
            validate_helper_entities([helper, regular])

    def test_helper_referenced_via_derived_source_passes(self):
        """HelperEntity referenced via DerivedSource.depends_on passes validation."""
        helper = _make_helper("mapping")
        consumer = _make_entity("consumer", depends_on=["mapping"])

        # Should not raise
        validate_helper_entities([helper, consumer])

    def test_helper_referenced_via_aggregation_source_passes(self):
        """HelperEntity referenced via AggregationSource.source_entity passes validation."""
        helper = _make_helper("base_table")
        consumer = _make_entity_with_aggregation("agg_entity", source_entity="base_table")

        # Should not raise
        validate_helper_entities([helper, consumer])

    def test_no_helper_entities_passes(self):
        """Pipeline with no HelperEntities passes validation."""
        regular_a = _make_entity("alpha")
        regular_b = _make_entity("beta")

        # Should not raise
        validate_helper_entities([regular_a, regular_b])

    def test_empty_entity_list_passes(self):
        """Empty list passes validation."""
        validate_helper_entities([])

    def test_multiple_orphaned_helpers_all_listed(self):
        """Multiple orphaned helpers are all listed in the error message."""
        helper_a = _make_helper("helper_a")
        helper_b = _make_helper("helper_b")
        regular = _make_entity("regular")

        with pytest.raises(ValueError, match="not referenced") as exc_info:
            validate_helper_entities([helper_a, helper_b, regular])

        error_msg = str(exc_info.value)
        assert "helper_a" in error_msg
        assert "helper_b" in error_msg
