"""Tests for HelperEntity."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from fyrnheim import (
    DimensionLayer,
    Entity,
    Field,
    HelperEntity,
    LayersConfig,
    PrepLayer,
    SnapshotLayer,
    TableSource,
)


def _make_prep_layers() -> LayersConfig:
    return LayersConfig(prep=PrepLayer(model_name="prep_helper"))


def _make_source() -> TableSource:
    return TableSource(
        project="test",
        dataset="raw",
        table="test_table",
        fields=[Field(name="id", type="int64")],
    )


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
                layers=LayersConfig(prep=PrepLayer(model_name="prep_helper"), dimension=DimensionLayer(model_name="dim_helper")),
                source=_make_source(),
            )

    def test_snapshot_layer_raises(self) -> None:
        with pytest.raises(ValidationError, match="does not support snapshot layer"):
            HelperEntity(
                name="helper",
                description="test helper",
                layers=LayersConfig(prep=PrepLayer(model_name="prep_helper"), snapshot=SnapshotLayer(tracked_fields=["id"])),
                source=_make_source(),
            )

    def test_prep_plus_dimension_raises(self) -> None:
        with pytest.raises(ValidationError, match="does not support dimension layer"):
            HelperEntity(
                name="helper",
                description="test helper",
                layers=LayersConfig(prep=PrepLayer(model_name="prep_helper"), dimension=DimensionLayer(model_name="dim_helper")),
                source=_make_source(),
            )


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
