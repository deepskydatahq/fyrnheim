"""Tests for the docs catalog builder."""

import json
from pathlib import Path

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.components.measure import Measure
from fyrnheim.core.entity import Entity, LayersConfig
from fyrnheim.core.layer import DimensionLayer, PrepLayer, SnapshotLayer
from fyrnheim.core.source import (
    AggregationSource,
    DerivedSource,
    EventAggregationSource,
    Field,
    TableSource,
    UnionSource,
)
from fyrnheim.docs import build_catalog
from fyrnheim.engine.registry import EntityInfo, EntityRegistry
from fyrnheim.quality import NotNull, QualityConfig, Unique

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_layers(**kwargs):
    """Create a LayersConfig with prep by default."""
    if not kwargs:
        kwargs = {"prep": PrepLayer(model_name="prep_test")}
    return LayersConfig(**kwargs)


def _make_registry(*entity_infos: EntityInfo) -> EntityRegistry:
    """Build an EntityRegistry from EntityInfo objects."""
    reg = EntityRegistry()
    for info in entity_infos:
        reg._entities[info.name] = info
    return reg


def _make_entity_info(
    entity: Entity,
    layers: list[str] | None = None,
    path: str = "test.py",
) -> EntityInfo:
    """Wrap an Entity in EntityInfo."""
    if layers is None:
        layer_names = ["prep", "dimension", "snapshot", "activity", "analytics"]
        layers = [ln for ln in layer_names if entity.has_layer(ln)]
    return EntityInfo(
        name=entity.name,
        entity=entity,
        path=Path(path),
        layers=layers,
    )


# ---------------------------------------------------------------------------
# Tests: empty registry
# ---------------------------------------------------------------------------


class TestCatalogEmpty:
    """Test catalog with empty registry."""

    def test_empty_registry_returns_empty_entities(self):
        reg = EntityRegistry()
        result = build_catalog(reg)
        assert result["entities"] == []

    def test_empty_registry_has_metadata(self):
        reg = EntityRegistry()
        result = build_catalog(reg)
        assert "metadata" in result
        assert result["metadata"]["entity_count"] == 0
        assert result["metadata"]["generator"] == "fyrnheim"
        assert "generated_at" in result["metadata"]

    def test_empty_registry_is_json_serializable(self):
        reg = EntityRegistry()
        result = build_catalog(reg)
        serialized = json.dumps(result)
        assert isinstance(serialized, str)


# ---------------------------------------------------------------------------
# Tests: single entity with fields, computed columns, measures
# ---------------------------------------------------------------------------


class TestCatalogSingleEntity:
    """Test catalog with a single entity containing all metadata."""

    def _build_entity(self) -> Entity:
        return Entity(
            name="customer",
            description="Customer entity",
            layers=_make_layers(
                prep=PrepLayer(model_name="prep_customer"),
                dimension=DimensionLayer(
                    model_name="dim_customer",
                    computed_columns=[
                        ComputedColumn(
                            name="full_name",
                            expression="first_name || ' ' || last_name",
                            description="Full name",
                        ),
                    ],
                ),
            ),
            source=TableSource(
                project="proj",
                dataset="raw",
                table="customers",
                fields=[
                    Field(name="id", type="STRING", description="Customer ID"),
                    Field(name="email", type="STRING", nullable=True),
                    Field(name="amount", type="FLOAT64"),
                ],
            ),
            core_computed=[
                ComputedColumn(
                    name="email_domain",
                    expression="SPLIT(email, '@')[1]",
                    description="Email domain",
                ),
            ],
            core_measures=[
                Measure(
                    name="total_revenue",
                    expression="SUM(amount)",
                    description="Total revenue",
                ),
                Measure(name="customer_count", expression="COUNT(*)"),
            ],
            quality=QualityConfig(
                checks=[NotNull("id", "email"), Unique("id")],
                primary_key="id",
            ),
        )

    def test_entity_name_and_description(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        e = result["entities"][0]
        assert e["name"] == "customer"
        assert e["description"] == "Customer entity"

    def test_source_type_detected(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        assert result["entities"][0]["source_type"] == "table"

    def test_fields_extracted(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        fields = result["entities"][0]["fields"]
        assert len(fields) == 3
        assert fields[0]["name"] == "id"
        assert fields[0]["type"] == "STRING"
        assert fields[0]["description"] == "Customer ID"

    def test_computed_columns_extracted(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        cc = result["entities"][0]["computed_columns"]
        # core_computed (1) + dimension computed (1)
        assert len(cc) == 2
        names = {c["name"] for c in cc}
        assert "email_domain" in names
        assert "full_name" in names

    def test_measures_extracted(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        measures = result["entities"][0]["measures"]
        assert len(measures) == 2
        assert measures[0]["name"] == "total_revenue"
        assert measures[0]["expression"] == "SUM(amount)"

    def test_quality_checks_extracted(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        quality = result["entities"][0]["quality"]
        assert quality["primary_key"] == "id"
        assert len(quality["checks"]) == 2
        types = {c["type"] for c in quality["checks"]}
        assert "NotNull" in types
        assert "Unique" in types

    def test_layers_extracted(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        layers = result["entities"][0]["layers"]
        assert "prep" in layers["active"]
        assert "dimension" in layers["active"]
        assert layers["prep"]["model_name"] == "prep_customer"
        assert layers["dimension"]["model_name"] == "dim_customer"

    def test_no_dependencies_for_standalone_entity(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        assert result["entities"][0]["dependencies"] == []
        assert result["entities"][0]["dependents"] == []

    def test_full_catalog_is_json_serializable(self):
        entity = self._build_entity()
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        serialized = json.dumps(result)
        assert isinstance(serialized, str)
        parsed = json.loads(serialized)
        assert parsed["metadata"]["entity_count"] == 1


# ---------------------------------------------------------------------------
# Tests: multiple entities with dependencies
# ---------------------------------------------------------------------------


class TestCatalogDependencies:
    """Test catalog with entities that have dependency relationships."""

    def _build_registry(self) -> EntityRegistry:
        person = Entity(
            name="person",
            description="Person entity",
            layers=_make_layers(),
            source=TableSource(project="p", dataset="d", table="persons"),
        )
        account = Entity(
            name="account",
            description="Account aggregated from person",
            layers=_make_layers(),
            source=AggregationSource(
                source_entity="person",
                group_by_column="account_id",
            ),
        )
        derived = Entity(
            name="unified_person",
            description="Derived person via identity graph",
            layers=_make_layers(),
            source=DerivedSource(
                identity_graph="person_graph",
                depends_on=["person", "account"],
            ),
        )
        return _make_registry(
            _make_entity_info(person),
            _make_entity_info(account),
            _make_entity_info(derived),
        )

    def test_dependencies_computed(self):
        reg = self._build_registry()
        result = build_catalog(reg)
        by_name = {e["name"]: e for e in result["entities"]}

        assert by_name["person"]["dependencies"] == []
        assert "person" in by_name["account"]["dependencies"]
        assert "person" in by_name["unified_person"]["dependencies"]
        assert "account" in by_name["unified_person"]["dependencies"]

    def test_dependents_computed(self):
        reg = self._build_registry()
        result = build_catalog(reg)
        by_name = {e["name"]: e for e in result["entities"]}

        # person is depended on by both account and unified_person
        assert "account" in by_name["person"]["dependents"]
        assert "unified_person" in by_name["person"]["dependents"]

        # account is depended on by unified_person
        assert "unified_person" in by_name["account"]["dependents"]

        # unified_person has no dependents
        assert by_name["unified_person"]["dependents"] == []

    def test_entity_count_in_metadata(self):
        reg = self._build_registry()
        result = build_catalog(reg)
        assert result["metadata"]["entity_count"] == 3

    def test_multiple_entities_json_serializable(self):
        reg = self._build_registry()
        result = build_catalog(reg)
        serialized = json.dumps(result)
        assert isinstance(serialized, str)


# ---------------------------------------------------------------------------
# Tests: source type detection
# ---------------------------------------------------------------------------


class TestSourceTypeDetection:
    """Test that all source types are correctly identified."""

    def _catalog_source_type(self, source) -> str:
        entity = Entity(
            name="test_entity",
            description="test",
            layers=_make_layers(),
            source=source,
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        return result["entities"][0]["source_type"]

    def test_table_source(self):
        source = TableSource(project="p", dataset="d", table="t")
        assert self._catalog_source_type(source) == "table"

    def test_derived_source(self):
        source = DerivedSource(
            identity_graph="graph",
            depends_on=["other"],
        )
        assert self._catalog_source_type(source) == "derived"

    def test_aggregation_source(self):
        source = AggregationSource(
            source_entity="person",
            group_by_column="account_id",
        )
        assert self._catalog_source_type(source) == "aggregation"

    def test_event_aggregation_source(self):
        source = EventAggregationSource(
            project="p",
            dataset="d",
            table="events",
            group_by_column="user_id",
        )
        assert self._catalog_source_type(source) == "event_aggregation"

    def test_union_source(self):
        source = UnionSource(
            sources=[
                TableSource(project="p", dataset="d", table="t1"),
                TableSource(project="p", dataset="d", table="t2"),
            ]
        )
        assert self._catalog_source_type(source) == "union"

    def test_no_source(self):
        entity = Entity(
            name="test_entity",
            description="test",
            layers=_make_layers(),
            required_fields=[Field(name="id", type="STRING")],
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        assert result["entities"][0]["source_type"] == "none"


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------


class TestCatalogEdgeCases:
    """Test edge cases for catalog builder."""

    def test_entity_with_no_quality(self):
        entity = Entity(
            name="simple",
            description="No quality checks",
            layers=_make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        quality = result["entities"][0]["quality"]
        assert quality["checks"] == []
        assert quality["primary_key"] is None

    def test_entity_with_no_computed_columns(self):
        entity = Entity(
            name="simple",
            description="No computed",
            layers=_make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        assert result["entities"][0]["computed_columns"] == []

    def test_entity_with_no_measures(self):
        entity = Entity(
            name="simple",
            description="No measures",
            layers=_make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        assert result["entities"][0]["measures"] == []

    def test_entity_with_snapshot_layer(self):
        entity = Entity(
            name="versioned",
            description="With snapshot",
            layers=_make_layers(
                prep=PrepLayer(model_name="prep_v"),
                snapshot=SnapshotLayer(),
            ),
            source=TableSource(project="p", dataset="d", table="t"),
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        layers = result["entities"][0]["layers"]
        assert "snapshot" in layers["active"]
        assert "prep" in layers["active"]

    def test_is_internal_included(self):
        entity = Entity(
            name="internal_entity",
            description="Internal",
            layers=_make_layers(),
            source=TableSource(project="p", dataset="d", table="t"),
            is_internal=True,
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        assert result["entities"][0]["is_internal"] is True

    def test_entity_with_required_fields_no_source(self):
        """Entity using contract pattern with required_fields instead of source."""
        entity = Entity(
            name="contract_entity",
            description="Uses contract pattern",
            layers=_make_layers(),
            required_fields=[
                Field(name="id", type="STRING"),
                Field(name="name", type="STRING", description="Entity name"),
            ],
        )
        reg = _make_registry(_make_entity_info(entity))
        result = build_catalog(reg)
        fields = result["entities"][0]["fields"]
        assert len(fields) == 2
        assert fields[0]["name"] == "id"
