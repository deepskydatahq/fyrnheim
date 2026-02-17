"""Quality check result models."""

from typing import Any

from pydantic import BaseModel


class CheckResult(BaseModel):
    """Result of running a single quality check."""

    check_name: str
    passed: bool
    failure_count: int
    sample_failures: list[dict[str, Any]]
    error: str | None = None


class EntityResult(BaseModel):
    """Result of running all checks for an entity."""

    entity_name: str
    table_name: str
    results: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.passed)
