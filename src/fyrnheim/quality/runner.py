"""Quality check runner for database backends via ibis."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .checks import (
    ForeignKey,
    MatchesPattern,
    MaxAge,
    QualityCheck,
    QualityConfig,
    Unique,
)
from .results import CheckResult, EntityResult

if TYPE_CHECKING:
    import ibis


class QualityRunner:
    """Runs quality checks against a database backend via ibis."""

    def __init__(self, connection: ibis.BaseBackend, dataset: str | None = None):
        self.connection = connection
        self.dataset = dataset

    def _table_ref(self, table: str) -> str:
        """Return a qualified table reference."""
        if self.dataset:
            return f"{self.dataset}.{table}"
        return table

    def _execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a raw SQL query and return results as list of dicts."""
        cursor = self.connection.raw_sql(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]

    def build_failure_query(
        self,
        table: str,
        check: QualityCheck,
        select_columns: list[str],
        limit: int = 10,
    ) -> str:
        """Build SQL query to find failing rows."""
        cols = ", ".join(select_columns)
        where = check.get_where_clause()
        ref = self._table_ref(table)
        return f"SELECT {cols}\nFROM {ref}\nWHERE {where}\nLIMIT {limit}"

    def build_unique_check_query(
        self,
        table: str,
        check: Unique,
        select_columns: list[str],
        limit: int = 10,
    ) -> str:
        """Build SQL query to find duplicate rows."""
        cols = ", ".join(select_columns)
        group_cols = ", ".join(check.columns)
        ref = self._table_ref(table)
        return f"""SELECT {cols}, COUNT(*) as _duplicate_count
FROM {ref}
GROUP BY {group_cols}
HAVING COUNT(*) > 1
LIMIT {limit}"""

    def build_max_age_check_query(
        self,
        table: str,
        check: MaxAge,
    ) -> str:
        """Build SQL query to check data freshness."""
        ref = self._table_ref(table)
        return f"""SELECT
    MAX({check.column}) as max_value,
    DATE_DIFF(CURRENT_DATE(), DATE(MAX({check.column})), DAY) as days_old,
    {check.days} as max_allowed_days
FROM {ref}
HAVING DATE_DIFF(CURRENT_DATE(), DATE(MAX({check.column})), DAY) > {check.days}"""

    def build_foreign_key_query(
        self,
        table: str,
        check: ForeignKey,
        select_columns: list[str],
        limit: int = 10,
    ) -> str:
        """Build SQL query to find orphaned foreign keys."""
        cols = ", ".join(f"t.{c}" for c in select_columns)
        ref_column = check.references.split(".")[1]
        fk_ref_table = check._resolved_ref_table

        ref = self._table_ref(table)
        fk_ref = self._table_ref(fk_ref_table)

        return f"""SELECT {cols}
FROM {ref} t
LEFT JOIN {fk_ref} r ON t.{check.column} = r.{ref_column}
WHERE r.{ref_column} IS NULL AND t.{check.column} IS NOT NULL
LIMIT {limit}"""

    def run_check(
        self,
        table: str,
        check: QualityCheck,
        primary_key: str,
        limit: int = 10,
    ) -> CheckResult:
        """Run a single quality check and return result."""
        try:
            # Determine columns to show
            select_columns = [primary_key] + [c for c in check.columns_to_show if c != primary_key]

            # Build appropriate query based on check type
            if isinstance(check, MatchesPattern):
                return self._run_matches_pattern_check(table, check, primary_key, limit)
            elif isinstance(check, Unique):
                query = self.build_unique_check_query(table, check, select_columns, limit)
            elif isinstance(check, MaxAge):
                query = self.build_max_age_check_query(table, check)
            elif isinstance(check, ForeignKey):
                query = self.build_foreign_key_query(table, check, select_columns, limit)
            else:
                query = self.build_failure_query(table, check, select_columns, limit)

            # Execute query
            rows = self._execute_query(query)

            # For MaxAge, check if we got any result (meaning it failed)
            if isinstance(check, MaxAge):
                if rows:
                    return CheckResult(
                        check_name=check.display_name,
                        passed=False,
                        failure_count=1,
                        sample_failures=rows,
                    )
                else:
                    return CheckResult(
                        check_name=check.display_name,
                        passed=True,
                        failure_count=0,
                        sample_failures=[],
                    )

            # Get total count if there are failures
            failure_count = len(rows)
            if failure_count == limit:
                # There might be more, get actual count
                count_query = self._build_count_query(table, check)
                count_rows = self._execute_query(count_query)
                failure_count = list(count_rows[0].values())[0]

            return CheckResult(
                check_name=check.display_name,
                passed=(failure_count == 0),
                failure_count=failure_count,
                sample_failures=rows,
            )

        except Exception as e:
            return CheckResult(
                check_name=check.display_name,
                passed=False,
                failure_count=0,
                sample_failures=[],
                error=str(e),
            )

    def _run_matches_pattern_check(
        self,
        table: str,
        check: MatchesPattern,
        primary_key: str,
        limit: int = 10,
    ) -> CheckResult:
        """Run MatchesPattern check using portable Ibis re_search()."""
        try:
            t = self.connection.table(table)
            col = t[check.column]
            # Rows that do NOT match the pattern are failures
            failures = t.filter(~col.re_search(check.pattern))
            failure_count = failures.count().execute()

            sample_failures: list[dict[str, Any]] = []
            if failure_count > 0:
                select_cols = [primary_key] + [c for c in [check.column] if c != primary_key]
                sample = failures.select(*select_cols).limit(limit).to_pandas()
                sample_failures = sample.to_dict("records")

            return CheckResult(
                check_name=check.display_name,
                passed=(failure_count == 0),
                failure_count=failure_count,
                sample_failures=sample_failures,
            )
        except Exception as e:
            return CheckResult(
                check_name=check.display_name,
                passed=False,
                failure_count=0,
                sample_failures=[],
                error=str(e),
            )

    def _build_count_query(self, table: str, check: QualityCheck) -> str:
        """Build count query for total failures."""
        ref = self._table_ref(table)
        if isinstance(check, Unique):
            group_cols = ", ".join(check.columns)
            return f"""SELECT COUNT(*) FROM (
    SELECT {group_cols}
    FROM {ref}
    GROUP BY {group_cols}
    HAVING COUNT(*) > 1
)"""
        elif isinstance(check, ForeignKey):
            ref_column = check.references.split(".")[1]
            fk_ref_table = check._resolved_ref_table
            fk_ref = self._table_ref(fk_ref_table)
            return f"""SELECT COUNT(*)
FROM {ref} t
LEFT JOIN {fk_ref} r ON t.{check.column} = r.{ref_column}
WHERE r.{ref_column} IS NULL AND t.{check.column} IS NOT NULL"""
        else:
            where = check.get_where_clause()
            return f"SELECT COUNT(*) FROM {ref} WHERE {where}"

    def run_entity_checks(
        self,
        entity_name: str,
        quality_config: QualityConfig,
        primary_key: str,
        table_name: str | None = None,
        limit: int = 10,
    ) -> EntityResult:
        """Run all quality checks for an entity."""
        table = table_name or entity_name
        results = []

        for check in quality_config.checks:
            result = self.run_check(table, check, primary_key, limit)
            results.append(result)

        return EntityResult(
            entity_name=entity_name,
            table_name=table,
            results=results,
        )
