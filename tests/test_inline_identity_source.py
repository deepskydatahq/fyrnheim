"""Tests for inline identity graph source model changes (M017-E001)."""

import pytest
from pydantic import ValidationError

from fyrnheim.components.computed_column import ComputedColumn
from fyrnheim.core.source import (
    DerivedSource,
    IdentityGraphConfig,
    IdentityGraphSource,
    TableSource,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _table_source(**overrides):
    """Build a minimal TableSource with sensible defaults."""
    defaults = {"project": "proj", "dataset": "ds", "table": "tbl"}
    defaults.update(overrides)
    return TableSource(**defaults)


def _entity_source(name="src_a", entity="entity_a", match_key_field="email"):
    """Build an IdentityGraphSource backed by a named entity."""
    return IdentityGraphSource(name=name, entity=entity, match_key_field=match_key_field)


def _inline_source(name="src_b", match_key_field="email", **kwargs):
    """Build an IdentityGraphSource backed by an inline TableSource."""
    return IdentityGraphSource(
        name=name,
        source=_table_source(**kwargs),
        match_key_field=match_key_field,
    )


# ---------------------------------------------------------------------------
# IdentityGraphSource – entity-only (existing behaviour)
# ---------------------------------------------------------------------------

class TestEntityReferenceStillWorks:
    """Verify existing entity-reference behaviour is preserved."""

    def test_basic_entity_reference(self):
        """Entity reference sets entity field and leaves source as None."""
        src = _entity_source()
        assert src.entity == "entity_a"
        assert src.source is None
        assert src.prep_columns == []

    def test_entity_reference_with_fields(self):
        """Entity reference accepts field mappings."""
        src = IdentityGraphSource(
            name="s", entity="e", match_key_field="k",
            fields={"col1": "alias1"},
        )
        assert src.fields == {"col1": "alias1"}


# ---------------------------------------------------------------------------
# IdentityGraphSource – inline source
# ---------------------------------------------------------------------------

class TestInlineSource:
    """Verify inline TableSource can replace entity references."""

    def test_inline_source_validates(self):
        """Inline source sets source field and leaves entity as None."""
        src = _inline_source()
        assert src.entity is None
        assert src.source is not None
        assert src.source.project == "proj"

    def test_inline_source_with_duckdb_path(self):
        """Inline source propagates duckdb_path to the inner TableSource."""
        src = _inline_source(duckdb_path="/data/events.parquet")
        assert src.source.duckdb_path == "/data/events.parquet"

    def test_inline_source_with_fields(self):
        """Inline source accepts field mappings."""
        src = IdentityGraphSource(
            name="s",
            source=_table_source(),
            match_key_field="email",
            fields={"raw_col": "alias"},
        )
        assert src.fields == {"raw_col": "alias"}

    def test_inline_source_with_id_and_date_fields(self):
        """Inline source accepts optional id_field and date_field."""
        src = IdentityGraphSource(
            name="s",
            source=_table_source(),
            match_key_field="email",
            id_field="user_id",
            date_field="created_at",
        )
        assert src.id_field == "user_id"
        assert src.date_field == "created_at"


# ---------------------------------------------------------------------------
# IdentityGraphSource – prep_columns
# ---------------------------------------------------------------------------

class TestPrepColumns:
    """Verify prep_columns field on IdentityGraphSource."""

    def test_inline_source_with_prep_columns(self):
        """Inline source stores prep_columns as ComputedColumn list."""
        cols = [
            ComputedColumn(name="domain", expression="split_part(email, '@', 2)"),
        ]
        src = IdentityGraphSource(
            name="s",
            source=_table_source(),
            match_key_field="email",
            prep_columns=cols,
        )
        assert len(src.prep_columns) == 1
        assert src.prep_columns[0].name == "domain"

    def test_entity_source_with_prep_columns(self):
        """prep_columns is allowed on entity sources too (no restriction)."""
        cols = [ComputedColumn(name="lower_email", expression="lower(email)")]
        src = IdentityGraphSource(
            name="s", entity="e", match_key_field="email", prep_columns=cols,
        )
        assert len(src.prep_columns) == 1

    def test_default_prep_columns_is_empty(self):
        """Default prep_columns is an empty list."""
        src = _entity_source()
        assert src.prep_columns == []


# ---------------------------------------------------------------------------
# IdentityGraphSource – validation errors
# ---------------------------------------------------------------------------

class TestValidationErrors:
    """Verify mutual exclusivity and required-field validation."""

    def test_both_entity_and_source_raises(self):
        """Providing both entity and source raises ValidationError."""
        with pytest.raises(ValidationError, match="not both"):
            IdentityGraphSource(
                name="s",
                entity="e",
                source=_table_source(),
                match_key_field="email",
            )

    def test_neither_entity_nor_source_raises(self):
        """Omitting both entity and source raises ValidationError."""
        with pytest.raises(ValidationError, match="requires either"):
            IdentityGraphSource(name="s", match_key_field="email")

    def test_empty_entity_string_raises(self):
        """Empty entity string is rejected by min_length constraint."""
        with pytest.raises(ValidationError):
            IdentityGraphSource(name="s", entity="", match_key_field="email")

    def test_empty_name_raises(self):
        """Empty name string is rejected by min_length constraint."""
        with pytest.raises(ValidationError):
            IdentityGraphSource(name="", entity="e", match_key_field="email")


# ---------------------------------------------------------------------------
# IdentityGraphConfig with mixed sources
# ---------------------------------------------------------------------------

class TestIdentityGraphConfigMixed:
    """Verify IdentityGraphConfig accepts mixed source types."""

    def test_config_with_entity_and_inline_sources(self):
        """Config with one entity-ref and one inline source validates."""
        s1 = _entity_source(name="a", entity="entity_a")
        s2 = _inline_source(name="b")
        config = IdentityGraphConfig(
            match_key="email",
            sources=[s1, s2],
            priority=["a", "b"],
        )
        assert len(config.sources) == 2

    def test_config_with_all_inline_sources(self):
        """Config with all inline sources validates."""
        s1 = _inline_source(name="x")
        s2 = _inline_source(name="y")
        config = IdentityGraphConfig(
            match_key="email",
            sources=[s1, s2],
            priority=["x", "y"],
        )
        assert len(config.sources) == 2


# ---------------------------------------------------------------------------
# DerivedSource – depends_on with inline sources
# ---------------------------------------------------------------------------

class TestDerivedSourceDependsOn:
    """Verify DerivedSource.depends_on correctly filters inline sources."""
    def test_depends_on_skips_inline_sources(self):
        """Inline sources (entity=None) must NOT appear in depends_on."""
        s1 = _entity_source(name="a", entity="entity_a")
        s2 = _inline_source(name="b")
        config = IdentityGraphConfig(
            match_key="email",
            sources=[s1, s2],
            priority=["a", "b"],
        )
        ds = DerivedSource(identity_graph="id_graph", identity_graph_config=config)
        assert "entity_a" in ds.depends_on
        # inline source should not contribute to depends_on
        assert None not in ds.depends_on
        assert len(ds.depends_on) == 1

    def test_depends_on_with_all_entity_sources(self):
        """All entity-ref sources appear in depends_on."""
        s1 = _entity_source(name="a", entity="entity_a")
        s2 = _entity_source(name="b", entity="entity_b")
        config = IdentityGraphConfig(
            match_key="email",
            sources=[s1, s2],
            priority=["a", "b"],
        )
        ds = DerivedSource(identity_graph="id_graph", identity_graph_config=config)
        assert set(ds.depends_on) == {"entity_a", "entity_b"}

    def test_depends_on_with_all_inline_sources(self):
        """All-inline sources produce empty depends_on list."""
        s1 = _inline_source(name="a")
        s2 = _inline_source(name="b")
        config = IdentityGraphConfig(
            match_key="email",
            sources=[s1, s2],
            priority=["a", "b"],
        )
        ds = DerivedSource(identity_graph="id_graph", identity_graph_config=config)
        assert ds.depends_on == []

    def test_depends_on_preserves_explicit_deps(self):
        """Explicit depends_on entries are kept alongside config-derived ones."""
        s1 = _entity_source(name="a", entity="entity_a")
        s2 = _inline_source(name="b")
        config = IdentityGraphConfig(
            match_key="email",
            sources=[s1, s2],
            priority=["a", "b"],
        )
        ds = DerivedSource(
            identity_graph="id_graph",
            depends_on=["extra_dep"],
            identity_graph_config=config,
        )
        assert "extra_dep" in ds.depends_on
        assert "entity_a" in ds.depends_on
        assert len(ds.depends_on) == 2

    def test_depends_on_without_config(self):
        """DerivedSource without config has empty depends_on."""
        ds = DerivedSource(identity_graph="id_graph")
        assert ds.depends_on == []
