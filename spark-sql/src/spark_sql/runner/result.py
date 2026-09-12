"""Backend-agnostic result of running a single SQL statement.

A :class:`SqlResult` is a small, immutable snapshot of one statement's output:
the column names, the materialized rows (as plain Python tuples), which backend
produced it, and — optionally — the physical plan captured via ``EXPLAIN``. It is
deliberately backend-neutral so that a result from local Spark and a result from
Databricks can be examined and validated with the same code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator


@dataclass(frozen=True)
class SqlResult:
    """Immutable snapshot of a single statement's output.

    Attributes:
        statement: The SQL text that produced this result.
        columns: Ordered output column names.
        rows: Materialized rows as tuples, in the order returned by the backend.
        backend: Name of the backend that produced the result (e.g. ``"local-spark"``).
        plan: Physical plan text captured via ``EXPLAIN``, if requested; else ``None``.
    """

    statement: str
    columns: list[str]
    rows: list[tuple[Any, ...]]
    backend: str
    plan: str | None = field(default=None)

    @property
    def row_count(self) -> int:
        """Number of rows returned."""
        return len(self.rows)

    @property
    def column_count(self) -> int:
        """Number of output columns."""
        return len(self.columns)

    @property
    def is_empty(self) -> bool:
        """``True`` when the result has no rows."""
        return not self.rows

    def __len__(self) -> int:
        return len(self.rows)

    def __bool__(self) -> bool:
        return bool(self.rows)

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self.rows)

    def first(self) -> tuple[Any, ...] | None:
        """Return the first row, or ``None`` when the result is empty."""
        return self.rows[0] if self.rows else None

    def scalar(self) -> Any:
        """Return the single cell of a one-row, one-column result.

        Raises:
            ValueError: If the result is not exactly one row and one column.
        """
        if len(self.rows) != 1 or len(self.columns) != 1:
            raise ValueError(
                f"scalar() requires a 1x1 result, got {self.row_count}x{self.column_count}",
            )
        return self.rows[0][0]

    def column(self, name: str) -> list[Any]:
        """Return every value in one column, top to bottom.

        Raises:
            KeyError: If ``name`` is not an output column.
        """
        try:
            index = self.columns.index(name)
        except ValueError as exc:
            raise KeyError(f"No such column {name!r} in {self.columns}") from exc
        return [row[index] for row in self.rows]

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return the rows as a list of ``{column: value}`` dictionaries."""
        return [dict(zip(self.columns, row, strict=False)) for row in self.rows]
