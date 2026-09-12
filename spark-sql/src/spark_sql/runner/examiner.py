"""Examine SQL results and physical plans.

The examiner turns a raw result or ``EXPLAIN`` plan into structured, assertable
facts: row/column shape for a :class:`~spark_sql.runner.result.SqlResult`, and a
:class:`PlanSummary` that counts the execution operators (shuffles, join
strategies, scans) which the ``docs/`` pages reason about. This is the bridge
between *running* a statement and *validating* how it executes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from spark_sql.runner.result import SqlResult
    from spark_sql.runner.runner import SqlRunner

_JOIN_TOKENS = {
    "BroadcastHashJoin": "broadcast",
    "SortMergeJoin": "sort-merge",
    "ShuffledHashJoin": "shuffled-hash",
    "BroadcastNestedLoopJoin": "broadcast-nested-loop",
    "CartesianProduct": "cartesian",
}


@dataclass(frozen=True)
class PlanSummary:
    """Structured view of a physical plan captured via ``EXPLAIN``.

    Attributes:
        raw: The full plan text, unmodified.
        exchanges: Number of ``Exchange`` (shuffle) operators.
        join_strategies: Human-readable join strategies present, in plan order
            (e.g. ``["broadcast", "sort-merge"]``); duplicates preserved.
        scans: Number of scan/leaf-read operators.
    """

    raw: str
    exchanges: int
    join_strategies: list[str]
    scans: int

    @property
    def has_shuffle(self) -> bool:
        """``True`` when the plan contains at least one shuffle ``Exchange``."""
        return self.exchanges > 0

    @property
    def join_count(self) -> int:
        """Total number of join operators of any strategy."""
        return len(self.join_strategies)

    def contains(self, token: str) -> bool:
        """``True`` when *token* appears anywhere in the raw plan (case-sensitive)."""
        return token in self.raw

    def uses_broadcast_join(self) -> bool:
        """``True`` when any join is a ``BroadcastHashJoin``."""
        return "broadcast" in self.join_strategies

    def uses_sort_merge_join(self) -> bool:
        """``True`` when any join is a ``SortMergeJoin``."""
        return "sort-merge" in self.join_strategies


def analyze_plan(plan: str) -> PlanSummary:
    """Parse an ``EXPLAIN`` plan string into a :class:`PlanSummary`.

    Operator detection is textual (Spark's own plan node names), which keeps the
    examiner backend-neutral: a plan captured from local Spark and one captured
    from a Databricks warehouse are analyzed the same way.

    Args:
        plan: The plan text (typically from ``EXPLAIN FORMATTED``).

    Returns:
        A :class:`PlanSummary` with shuffle, join, and scan counts.
    """
    exchanges = plan.count("Exchange")
    scans = plan.count("Scan ")

    strategies: list[str] = []
    for line in plan.splitlines():
        for token, label in _JOIN_TOKENS.items():
            strategies.extend([label] * line.count(token))

    return PlanSummary(raw=plan, exchanges=exchanges, join_strategies=strategies, scans=scans)


def summarize(result: SqlResult) -> dict[str, Any]:
    """Return a compact, JSON-friendly summary of a result's shape.

    Args:
        result: The result to describe.

    Returns:
        A dictionary with backend name, row/column counts, column names, and
        whether a plan was captured — safe to log or serialize.
    """
    return {
        "backend": result.backend,
        "row_count": result.row_count,
        "column_count": result.column_count,
        "columns": list(result.columns),
        "is_empty": result.is_empty,
        "has_plan": result.plan is not None,
    }


class SqlExaminer:
    """Convenience wrapper that runs ``EXPLAIN`` and analyzes the plan in one step."""

    def __init__(self, runner: SqlRunner) -> None:
        self._runner = runner

    def plan_text(self, statement: str, mode: str = "FORMATTED") -> str:
        """Return the raw ``EXPLAIN`` plan text for *statement*."""
        return self._runner.explain(statement, mode=mode)

    def plan_summary(self, statement: str) -> PlanSummary:
        """Return a :class:`PlanSummary` for *statement* (via ``EXPLAIN FORMATTED``)."""
        return analyze_plan(self._runner.explain(statement, mode="FORMATTED"))
