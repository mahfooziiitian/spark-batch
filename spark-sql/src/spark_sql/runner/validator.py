"""Validate SQL results and physical plans.

Provides two styles over the same checks:

* **Functional** assertions (``expect_row_count``, ``expect_broadcast_join``, …)
  that raise :class:`~spark_sql.runner.exceptions.ValidationError` on failure.
* A **fluent** :func:`expect` / :func:`expect_plan` builder for chaining checks.

Because :class:`ValidationError` subclasses :class:`AssertionError`, these read
naturally in pytest while remaining catchable as a runner error in application code.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from spark_sql.runner.exceptions import ValidationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from spark_sql.runner.examiner import PlanSummary
    from spark_sql.runner.result import SqlResult


def _fail(message: str) -> None:
    raise ValidationError(message)


# --------------------------------------------------------------------------- #
# Result assertions
# --------------------------------------------------------------------------- #
def expect_non_empty(result: SqlResult) -> None:
    """Assert the result has at least one row."""
    if result.is_empty:
        _fail(f"expected a non-empty result, but got 0 rows from {result.backend}")


def expect_empty(result: SqlResult) -> None:
    """Assert the result has no rows."""
    if not result.is_empty:
        _fail(f"expected an empty result, but got {result.row_count} row(s) from {result.backend}")


def expect_row_count(result: SqlResult, expected: int) -> None:
    """Assert the result has exactly *expected* rows."""
    if result.row_count != expected:
        _fail(f"expected {expected} row(s), got {result.row_count} from {result.backend}")


def expect_min_row_count(result: SqlResult, minimum: int) -> None:
    """Assert the result has at least *minimum* rows."""
    if result.row_count < minimum:
        _fail(f"expected at least {minimum} row(s), got {result.row_count} from {result.backend}")


def expect_columns(result: SqlResult, columns: Sequence[str], *, exact: bool = True) -> None:
    """Assert the result's columns match *columns*.

    Args:
        result: The result to check.
        columns: Expected column names.
        exact: When ``True``, order and membership must match exactly; when
            ``False``, *columns* need only be a subset of the result's columns.
    """
    expected = list(columns)
    if exact:
        if result.columns != expected:
            _fail(f"expected columns {expected}, got {result.columns}")
    else:
        missing = [column for column in expected if column not in result.columns]
        if missing:
            _fail(f"missing expected column(s) {missing}; result has {result.columns}")


def expect_scalar(result: SqlResult, value: Any) -> None:
    """Assert the result is a single cell equal to *value*."""
    actual = result.scalar()
    if actual != value:
        _fail(f"expected scalar {value!r}, got {actual!r} from {result.backend}")


def expect_rows(result: SqlResult, rows: Sequence[Sequence[Any]], *, ignore_order: bool = True) -> None:
    """Assert the result's rows equal *rows*.

    Args:
        result: The result to check.
        rows: Expected rows as sequences of values.
        ignore_order: When ``True`` (default), compare as multisets so row order
            does not matter; when ``False``, order must match.
    """
    actual = [tuple(row) for row in result.rows]
    expected = [tuple(row) for row in rows]
    if ignore_order:
        if sorted(actual, key=repr) != sorted(expected, key=repr):
            _fail(f"row multiset mismatch: expected {expected}, got {actual}")
    elif actual != expected:
        _fail(f"row sequence mismatch: expected {expected}, got {actual}")


def expect_contains_row(result: SqlResult, row: Sequence[Any]) -> None:
    """Assert the result contains *row* at least once."""
    target = tuple(row)
    if target not in [tuple(existing) for existing in result.rows]:
        _fail(f"expected result to contain row {target!r}, but it did not (backend={result.backend})")


# --------------------------------------------------------------------------- #
# Plan assertions
# --------------------------------------------------------------------------- #
def expect_no_shuffle(summary: PlanSummary) -> None:
    """Assert the plan performs no shuffle (no ``Exchange``)."""
    if summary.has_shuffle:
        _fail(f"expected no shuffle, but plan has {summary.exchanges} Exchange operator(s)")


def expect_shuffle_count(summary: PlanSummary, expected: int) -> None:
    """Assert the plan has exactly *expected* shuffle ``Exchange`` operators."""
    if summary.exchanges != expected:
        _fail(f"expected {expected} Exchange operator(s), got {summary.exchanges}")


def expect_broadcast_join(summary: PlanSummary) -> None:
    """Assert the plan uses at least one ``BroadcastHashJoin``."""
    if not summary.uses_broadcast_join():
        _fail(f"expected a BroadcastHashJoin, but join strategies were {summary.join_strategies}")


def expect_no_cartesian(summary: PlanSummary) -> None:
    """Assert the plan contains no Cartesian product."""
    if "cartesian" in summary.join_strategies:
        _fail("expected no Cartesian product, but plan contains CartesianProduct")


def expect_plan_contains(summary: PlanSummary, token: str) -> None:
    """Assert *token* appears in the plan text."""
    if not summary.contains(token):
        _fail(f"expected plan to contain {token!r}, but it did not")


# --------------------------------------------------------------------------- #
# Fluent API
# --------------------------------------------------------------------------- #
class ResultExpectation:
    """Chainable assertions over a :class:`SqlResult` (see :func:`expect`)."""

    def __init__(self, result: SqlResult) -> None:
        self._result = result

    def non_empty(self) -> ResultExpectation:
        """Assert the result has at least one row."""
        expect_non_empty(self._result)
        return self

    def empty(self) -> ResultExpectation:
        """Assert the result has no rows."""
        expect_empty(self._result)
        return self

    def row_count(self, expected: int) -> ResultExpectation:
        """Assert the result has exactly *expected* rows."""
        expect_row_count(self._result, expected)
        return self

    def min_rows(self, minimum: int) -> ResultExpectation:
        """Assert the result has at least *minimum* rows."""
        expect_min_row_count(self._result, minimum)
        return self

    def columns(self, columns: Sequence[str], *, exact: bool = True) -> ResultExpectation:
        """Assert the result's columns match *columns* (see :func:`expect_columns`)."""
        expect_columns(self._result, columns, exact=exact)
        return self

    def scalar(self, value: Any) -> ResultExpectation:
        """Assert the result is a single cell equal to *value*."""
        expect_scalar(self._result, value)
        return self

    def rows(self, rows: Sequence[Sequence[Any]], *, ignore_order: bool = True) -> ResultExpectation:
        """Assert the result's rows equal *rows* (see :func:`expect_rows`)."""
        expect_rows(self._result, rows, ignore_order=ignore_order)
        return self

    def contains_row(self, row: Sequence[Any]) -> ResultExpectation:
        """Assert the result contains *row* at least once."""
        expect_contains_row(self._result, row)
        return self


class PlanExpectation:
    """Chainable assertions over a :class:`PlanSummary` (see :func:`expect_plan`)."""

    def __init__(self, summary: PlanSummary) -> None:
        self._summary = summary

    def no_shuffle(self) -> PlanExpectation:
        """Assert the plan performs no shuffle (no ``Exchange``)."""
        expect_no_shuffle(self._summary)
        return self

    def shuffle_count(self, expected: int) -> PlanExpectation:
        """Assert the plan has exactly *expected* shuffle ``Exchange`` operators."""
        expect_shuffle_count(self._summary, expected)
        return self

    def broadcast_join(self) -> PlanExpectation:
        """Assert the plan uses at least one ``BroadcastHashJoin``."""
        expect_broadcast_join(self._summary)
        return self

    def no_cartesian(self) -> PlanExpectation:
        """Assert the plan contains no Cartesian product."""
        expect_no_cartesian(self._summary)
        return self

    def contains(self, token: str) -> PlanExpectation:
        """Assert *token* appears in the plan text."""
        expect_plan_contains(self._summary, token)
        return self


def expect(result: SqlResult) -> ResultExpectation:
    """Start a fluent assertion chain over a :class:`SqlResult`."""
    return ResultExpectation(result)


def expect_plan(summary: PlanSummary) -> PlanExpectation:
    """Start a fluent assertion chain over a :class:`PlanSummary`."""
    return PlanExpectation(summary)
