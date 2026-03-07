"""fyrnheim.testing -- Entity-level unit testing framework.

Provides EntityTest for defining sample input data and asserting
output after executing an entity on an ephemeral DuckDB backend.

Usage::

    from fyrnheim.testing import EntityTest

    class TestMyEntity(EntityTest):
        entity = my_entity

        def test_row_count(self):
            result = (
                self.given({"source_my_entity": [{"id": 1, "name": "Alice"}]})
                .run()
            )
            assert result.row_count == 1
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import ibis
import pandas as pd

from fyrnheim._generate import generate
from fyrnheim.core.entity import Entity
from fyrnheim.engine.executor import IbisExecutor


@dataclass
class TestResult:
    """Wraps the output of an entity execution for easy assertion.

    Provides convenience accessors over the underlying pandas DataFrame.
    """

    _df: pd.DataFrame

    @property
    def row_count(self) -> int:
        """Number of output rows."""
        return len(self._df)

    @property
    def columns(self) -> list[str]:
        """List of column names in the output."""
        return list(self._df.columns)

    def column(self, name: str) -> list[Any]:
        """Return all values for a given column as a list.

        Args:
            name: Column name.

        Raises:
            KeyError: If column does not exist.
        """
        if name not in self._df.columns:
            raise KeyError(f"Column '{name}' not found. Available: {self.columns}")
        return list(self._df[name])

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return all rows as a list of dicts for easy assertion."""
        return self._df.to_dict(orient="records")


class EntityTest:
    """Base class for entity-level unit tests.

    Subclass this and set ``entity`` to an Entity instance.
    Use ``given()`` to provide fixture data, then ``run()`` to execute
    the entity on an ephemeral DuckDB and get a ``TestResult``.

    Example::

        class TestCustomers(EntityTest):
            entity = customers_entity

            def test_basics(self):
                result = (
                    self.given({
                        "source_customers": [
                            {"id": 1, "name": "Alice", "plan": "pro"},
                        ]
                    })
                    .run()
                )
                assert result.row_count == 1

    The keys in the ``given()`` dict should match the table names
    that the generated source function expects (typically ``source_{entity_name}``).
    """

    entity: Entity

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

    def given(self, tables: dict[str, list[dict[str, Any]]]) -> _EntityTestRunner:
        """Provide fixture data as a dict of table_name -> list of row dicts.

        Args:
            tables: Mapping of table names to lists of row dictionaries.
                Keys should match the source table names expected by the
                generated transform (e.g. ``source_customers``).

        Returns:
            An _EntityTestRunner that can be ``run()`` to execute the entity.
        """
        if not hasattr(self, "entity") or self.entity is None:
            raise ValueError(
                "EntityTest subclass must set 'entity' to an Entity instance"
            )
        return _EntityTestRunner(entity=self.entity, tables=tables)


@dataclass
class _EntityTestRunner:
    """Internal runner that holds fixture data and executes the entity."""

    entity: Entity
    tables: dict[str, list[dict[str, Any]]]

    def run(self) -> TestResult:
        """Execute the entity on ephemeral DuckDB with the given fixture data.

        1. Generates transform code to a temp directory.
        2. Creates an in-memory DuckDB connection.
        3. Registers fixture data as in-memory tables.
        4. Executes the entity pipeline.
        5. Returns the output wrapped in a TestResult.

        Returns:
            TestResult wrapping the output DataFrame.

        Raises:
            ValueError: If no fixture data was provided.
        """
        if not self.tables:
            raise ValueError(
                "No fixture data provided. Call given() with table data first."
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            gen_dir = Path(tmp_dir) / "generated"

            # Generate transform code
            generate(self.entity, output_dir=gen_dir)

            # Create ephemeral DuckDB
            conn = ibis.duckdb.connect()
            executor = IbisExecutor(
                conn=conn, backend="duckdb", generated_dir=gen_dir
            )

            try:
                # Register fixture data as in-memory tables
                for table_name, rows in self.tables.items():
                    df = pd.DataFrame(rows)
                    conn.create_table(table_name, df, overwrite=True)

                # Execute the entity
                exec_result = executor.execute(
                    self.entity.name,
                    generated_dir=gen_dir,
                    entity=self.entity,
                )

                # Read the output table as a DataFrame
                output_table = conn.table(exec_result.target_name)
                output_df = output_table.execute()

                return TestResult(_df=output_df)
            finally:
                executor.close()


__all__ = ["EntityTest", "TestResult"]
