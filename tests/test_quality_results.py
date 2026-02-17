"""Unit tests for quality check result models."""

from typedata.quality import CheckResult, EntityResult


class TestCheckResult:
    def test_creation_passed(self) -> None:
        result = CheckResult(
            check_name="NotNull: email",
            passed=True,
            failure_count=0,
            sample_failures=[],
        )
        assert result.check_name == "NotNull: email"
        assert result.passed is True
        assert result.failure_count == 0
        assert result.sample_failures == []
        assert result.error is None

    def test_creation_failed(self) -> None:
        result = CheckResult(
            check_name="NotNull: email",
            passed=False,
            failure_count=3,
            sample_failures=[{"id": 1, "email": None}, {"id": 2, "email": None}],
        )
        assert result.passed is False
        assert result.failure_count == 3
        assert len(result.sample_failures) == 2

    def test_creation_with_error(self) -> None:
        result = CheckResult(
            check_name="NotNull: email",
            passed=False,
            failure_count=0,
            sample_failures=[],
            error="Table not found",
        )
        assert result.error == "Table not found"


class TestEntityResult:
    def test_all_passed(self) -> None:
        results = [
            CheckResult(check_name="check1", passed=True, failure_count=0, sample_failures=[]),
            CheckResult(check_name="check2", passed=True, failure_count=0, sample_failures=[]),
        ]
        entity = EntityResult(entity_name="user", table_name="user", results=results)
        assert entity.passed is True
        assert entity.passed_count == 2
        assert entity.failed_count == 0

    def test_some_failed(self) -> None:
        results = [
            CheckResult(check_name="check1", passed=True, failure_count=0, sample_failures=[]),
            CheckResult(check_name="check2", passed=False, failure_count=5, sample_failures=[]),
            CheckResult(check_name="check3", passed=False, failure_count=2, sample_failures=[]),
        ]
        entity = EntityResult(entity_name="user", table_name="user", results=results)
        assert entity.passed is False
        assert entity.passed_count == 1
        assert entity.failed_count == 2

    def test_empty_results(self) -> None:
        entity = EntityResult(entity_name="user", table_name="user", results=[])
        assert entity.passed is True
        assert entity.passed_count == 0
        assert entity.failed_count == 0
