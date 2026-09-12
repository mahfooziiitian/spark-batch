from __future__ import annotations

import pytest

from spark_sql.runner import (
    SqlResult,
    ValidationError,
    analyze_plan,
    expect,
    expect_plan,
)

pytestmark = [pytest.mark.unit]


def _result(**overrides: object) -> SqlResult:
    base: dict[str, object] = {
        "statement": "SELECT id, name FROM t",
        "columns": ["id", "name"],
        "rows": [(1, "a"), (2, "b")],
        "backend": "local-spark",
    }
    base.update(overrides)
    return SqlResult(**base)  # type: ignore[arg-type]


_BROADCAST_PLAN = analyze_plan("BroadcastHashJoin [id], [id], Inner\nScan a\nScan b\n")
_SHUFFLE_PLAN = analyze_plan("SortMergeJoin [id], [id]\nExchange hashpartitioning\nExchange hashpartitioning\n")


class TestResultExpectations:
    def test_non_empty_and_row_count(self: TestResultExpectations) -> None:
        expect(_result()).non_empty().row_count(2).min_rows(1)

    def test_empty_pass(self: TestResultExpectations) -> None:
        expect(_result(rows=[])).empty()

    def test_columns_exact_and_subset(self: TestResultExpectations) -> None:
        expect(_result()).columns(["id", "name"]).columns(["name"], exact=False)

    def test_scalar(self: TestResultExpectations) -> None:
        expect(_result(columns=["v"], rows=[(9,)])).scalar(9)

    def test_rows_order_insensitive(self: TestResultExpectations) -> None:
        expect(_result()).rows([(2, "b"), (1, "a")])

    def test_contains_row(self: TestResultExpectations) -> None:
        expect(_result()).contains_row((1, "a"))

    def test_non_empty_failure(self: TestResultExpectations) -> None:
        with pytest.raises(ValidationError):
            expect(_result(rows=[])).non_empty()

    def test_row_count_failure(self: TestResultExpectations) -> None:
        with pytest.raises(ValidationError):
            expect(_result()).row_count(5)

    def test_columns_failure(self: TestResultExpectations) -> None:
        with pytest.raises(ValidationError):
            expect(_result()).columns(["wrong"])

    def test_rows_order_sensitive_failure(self: TestResultExpectations) -> None:
        with pytest.raises(ValidationError):
            expect(_result()).rows([(2, "b"), (1, "a")], ignore_order=False)

    def test_validation_error_is_assertion_error(self: TestResultExpectations) -> None:
        with pytest.raises(AssertionError):
            expect(_result()).row_count(99)


class TestPlanExpectations:
    def test_broadcast_no_shuffle(self: TestPlanExpectations) -> None:
        expect_plan(_BROADCAST_PLAN).no_shuffle().broadcast_join().no_cartesian().contains("BroadcastHashJoin")

    def test_shuffle_count(self: TestPlanExpectations) -> None:
        expect_plan(_SHUFFLE_PLAN).shuffle_count(2)

    def test_no_shuffle_failure(self: TestPlanExpectations) -> None:
        with pytest.raises(ValidationError):
            expect_plan(_SHUFFLE_PLAN).no_shuffle()

    def test_broadcast_failure(self: TestPlanExpectations) -> None:
        with pytest.raises(ValidationError):
            expect_plan(_SHUFFLE_PLAN).broadcast_join()

    def test_cartesian_failure(self: TestPlanExpectations) -> None:
        cartesian = analyze_plan("CartesianProduct\nScan a\nScan b\n")
        with pytest.raises(ValidationError):
            expect_plan(cartesian).no_cartesian()

    def test_contains_failure(self: TestPlanExpectations) -> None:
        with pytest.raises(ValidationError):
            expect_plan(_BROADCAST_PLAN).contains("SortMergeJoin")
