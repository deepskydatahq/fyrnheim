"""Tests for HelperEntity."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fyrnheim import (
    AggregationSource,
    DerivedSource,
    DimensionLayer,
    Entity,
    Field,
    HelperEntity,
    LayersConfig,
    PrepLayer,
    SnapshotLayer,
    TableSource,
)
from fyrnheim.engine.runner import validate_helper_entities


def _make_prep_layers() -> LayersConfig:
    return LayersConfig(prep=PrepLayer(model_name="prep_helper"))


def _make_source() -> TableSource:
    return TableSource(
        project="test",
        dataset="raw",
        table="test_table",
        fields=[Field(name="id", type="int64")],
    )


def _make_helper(name: str = "identity_map") -> HelperEntity:
    return HelperEntity(
        name=name,
        description=f"Helper entity {name}",
        layers=LayersConfig(prep=PrepLayer(model_name=f"prep_{name}")),
        source=TableSource(project="p", dataset="d", table=name),
    )


def _make_entity(name: str, *, depends_on: list[str] | None = None) -> Entity:
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


# ---------------------------------------------------------------------------
# Layer restriction tests
# ---------------------------------------------------------------------------


class TestHelperEntityLayerRestriction:
    def test_prep_only_validates(self) -> None:
        h = HelperEntity(
            name="helper",
            description="test helper",
            layers=_make_prep_layers(),
            source=_make_source(),
        )
        assert h.layers.prep is not None

    def test_dimension_layer_raises(self) -> None:
        with pytest.raises(ValidationError, match="does not support dimension layer"):
            HelperEntity(
                name="helper",
                description="test helper",
                layers=LayersConfig(
                    prep=PrepLayer(model_name="prep_helper"),
                    dimension=DimensionLayer(model_name="dim_helper"),
                ),
                source=_make_source(),
            )

    def test_snapshot_layer_raises(self) -> None:
        with pytest.raises(ValidationError, match="does not support snapshot layer"):
            HelperEntity(
                name="helper",
                description="test helper",
                layers=LayersConfig(
                    prep=PrepLayer(model_name="prep_helper"),
                    snapshot=SnapshotLayer(tracked_fields=["id"]),
                ),
                source=_make_source(),
            )

    def test_prep_plus_dimension_raises(self) -> None:
        with pytest.raises(ValidationError, match="does not support dimension layer"):
            HelperEntity(
                name="helper",
                description="test helper",
                layers=LayersConfig(
                    prep=PrepLayer(model_name="prep_helper"),
                    dimension=DimensionLayer(model_name="dim_helper"),
                ),
                source=_make_source(),
            )


# ---------------------------------------------------------------------------
# is_internal tests
# ---------------------------------------------------------------------------


class TestHelperEntityIsInternal:
    def test_is_internal_default_true(self) -> None:
        h = HelperEntity(
            name="helper",
            description="test helper",
            layers=_make_prep_layers(),
            source=_make_source(),
        )
        assert h.is_internal is True

    def test_is_internal_forced_true(self) -> None:
        h = HelperEntity(
            name="helper",
            description="test helper",
            layers=_make_prep_layers(),
            source=_make_source(),
            is_internal=False,
        )
        assert h.is_internal is True


# ---------------------------------------------------------------------------
# Export and subclass tests
# ---------------------------------------------------------------------------


class TestHelperEntityExport:
    def test_importable_from_fyrnheim(self) -> None:
        from fyrnheim import HelperEntity as TopLevel
        from fyrnheim.core.entity import HelperEntity as Core

        assert TopLevel is Core


class TestHelperEntityIsEntity:
    def test_isinstance_entity(self) -> None:
        h = HelperEntity(
            name="helper",
            description="test helper",
            layers=_make_prep_layers(),
            source=_make_source(),
        )
        assert isinstance(h, Entity)


# ---------------------------------------------------------------------------
# Runner orphan validation tests
# ---------------------------------------------------------------------------


class TestValidateHelperEntities:
    def test_orphaned_helper_raises(self):
        helper = _make_helper("orphan_helper")
        regular = _make_entity("regular")

        with pytest.raises(ValueError, match="not referenced"):
            validate_helper_entities([helper, regular])

    def test_helper_referenced_via_derived_source_passes(self):
        helper = _make_helper("mapping")
        consumer = _make_entity("consumer", depends_on=["mapping"])

        validate_helper_entities([helper, consumer])

    def test_helper_referenced_via_aggregation_source_passes(self):
        helper = _make_helper("base_table")
        consumer = Entity(
            name="agg_entity",
            description="Aggregation entity",
            layers=LayersConfig(prep=PrepLayer(model_name="prep_agg")),
            source=AggregationSource(
                source_entity="base_table",
                group_by_column="id",
            ),
        )

        validate_helper_entities([helper, consumer])

    def test_no_helper_entities_passes(self):
        regular_a = _make_entity("alpha")
        regular_b = _make_entity("beta")

        validate_helper_entities([regular_a, regular_b])

    def test_empty_entity_list_passes(self):
        validate_helper_entities([])

    def test_multiple_orphaned_helpers_all_listed(self):
        helper_a = _make_helper("helper_a")
        helper_b = _make_helper("helper_b")
        regular = _make_entity("regular")

        with pytest.raises(ValueError, match="not referenced") as exc_info:
            validate_helper_entities([helper_a, helper_b, regular])

        error_msg = str(exc_info.value)
        assert "helper_a" in error_msg
        assert "helper_b" in error_msg
