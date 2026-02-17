"""Tests for core type definitions and enums."""


from fyrnheim.core.types import IncrementalStrategy, MaterializationType, SourcePriority


class TestMaterializationType:
    """Tests for MaterializationType enum."""

    def test_is_str_enum(self):
        """MaterializationType members are strings."""
        assert isinstance(MaterializationType.TABLE, str)

    def test_table_value(self):
        assert MaterializationType.TABLE == "table"
        assert MaterializationType.TABLE.value == "table"

    def test_view_value(self):
        assert MaterializationType.VIEW == "view"
        assert MaterializationType.VIEW.value == "view"

    def test_incremental_value(self):
        assert MaterializationType.INCREMENTAL == "incremental"
        assert MaterializationType.INCREMENTAL.value == "incremental"

    def test_ephemeral_value(self):
        assert MaterializationType.EPHEMERAL == "ephemeral"
        assert MaterializationType.EPHEMERAL.value == "ephemeral"

    def test_member_count(self):
        assert len(MaterializationType) == 4


class TestIncrementalStrategy:
    """Tests for IncrementalStrategy enum."""

    def test_is_str_enum(self):
        """IncrementalStrategy members are strings."""
        assert isinstance(IncrementalStrategy.MERGE, str)

    def test_merge_value(self):
        assert IncrementalStrategy.MERGE == "merge"
        assert IncrementalStrategy.MERGE.value == "merge"

    def test_append_value(self):
        assert IncrementalStrategy.APPEND == "append"
        assert IncrementalStrategy.APPEND.value == "append"

    def test_delete_insert_value(self):
        assert IncrementalStrategy.DELETE_INSERT == "delete+insert"
        assert IncrementalStrategy.DELETE_INSERT.value == "delete+insert"

    def test_member_count(self):
        assert len(IncrementalStrategy) == 3


class TestSourcePriority:
    """Tests for SourcePriority enum."""

    def test_is_int_enum(self):
        """SourcePriority members are integers."""
        assert isinstance(SourcePriority.PRIMARY, int)

    def test_primary_value(self):
        assert SourcePriority.PRIMARY == 1
        assert SourcePriority.PRIMARY.value == 1

    def test_secondary_value(self):
        assert SourcePriority.SECONDARY == 2
        assert SourcePriority.SECONDARY.value == 2

    def test_tertiary_value(self):
        assert SourcePriority.TERTIARY == 3
        assert SourcePriority.TERTIARY.value == 3

    def test_quaternary_value(self):
        assert SourcePriority.QUATERNARY == 4
        assert SourcePriority.QUATERNARY.value == 4

    def test_member_count(self):
        assert len(SourcePriority) == 4

    def test_ordering(self):
        """Priority values are ordered: PRIMARY < SECONDARY < TERTIARY < QUATERNARY."""
        assert SourcePriority.PRIMARY < SourcePriority.SECONDARY
        assert SourcePriority.SECONDARY < SourcePriority.TERTIARY
        assert SourcePriority.TERTIARY < SourcePriority.QUATERNARY


class TestReExports:
    """Verify re-exports from fyrnheim.core work."""

    def test_materialization_type_from_core(self):
        from fyrnheim.core import MaterializationType as MT
        assert MT.TABLE == "table"

    def test_incremental_strategy_from_core(self):
        from fyrnheim.core import IncrementalStrategy as IS
        assert IS.MERGE == "merge"

    def test_source_priority_from_core(self):
        from fyrnheim.core import SourcePriority as SP
        assert SP.PRIMARY == 1
