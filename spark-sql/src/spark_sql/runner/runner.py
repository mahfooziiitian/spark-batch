"""High-level orchestrator that runs statements and ``.sql`` files on any backend.

:class:`SqlRunner` is the main entry point of the library. It wraps a
:class:`~spark_sql.runner.backends.SqlBackend` and adds:

* single-statement execution (delegating to the backend),
* multi-statement ``.sql`` file execution, reusing the repository's own
  comment-aware splitter in :mod:`spark_sql._helpers`, and
* convenience constructors for the local Spark and Databricks backends.

The same runner API therefore drives the local Spark validation path and the
Databricks warehouse path with no call-site changes.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from spark_sql._helpers import read_sql_statements
from spark_sql.runner.backends import DatabricksBackend, LocalSparkBackend

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import SparkSession

    from spark_sql.runner.backends import SqlBackend
    from spark_sql.runner.result import SqlResult

logger = logging.getLogger(__name__)

_QUERY_PREFIXES = ("SELECT", "WITH", "TABLE", "VALUES", "FROM", "DESC", "DESCRIBE", "SHOW", "EXPLAIN")


class SqlRunner:
    """Run SQL statements and files against a pluggable backend."""

    def __init__(self, backend: SqlBackend) -> None:
        self._backend = backend

    @property
    def backend(self) -> SqlBackend:
        """The backend statements are executed against."""
        return self._backend

    @classmethod
    def local(cls, spark: SparkSession, *, backend_name: str = "local-spark") -> SqlRunner:
        """Build a runner backed by a local (or any) :class:`SparkSession`."""
        return cls(LocalSparkBackend(spark, backend_name=backend_name))

    @classmethod
    def databricks(
        cls,
        *,
        warehouse_id: str | None = None,
        warehouse_name: str | None = None,
        profile: str | None = None,
        host: str | None = None,
        token: str | None = None,
    ) -> SqlRunner:
        """Build a runner backed by a Databricks SQL warehouse (auth via env/profile)."""
        backend = DatabricksBackend.from_env(
            warehouse_id=warehouse_id,
            warehouse_name=warehouse_name,
            profile=profile,
            host=host,
            token=token,
        )
        return cls(backend)

    def run(self, statement: str, *, explain: bool = False) -> SqlResult:
        """Execute a single statement and return its :class:`SqlResult`."""
        return self._backend.run(statement, explain=explain)

    def explain(self, statement: str, mode: str = "FORMATTED") -> str:
        """Return the ``EXPLAIN`` plan text for *statement*."""
        return self._backend.explain(statement, mode=mode)

    def run_file(
        self,
        sql_path: str,
        *,
        skip_predicate: Callable[[str], bool] | None = None,
        transform: Callable[[str], str | None] | None = None,
        queries_only: bool = False,
        explain: bool = False,
    ) -> list[SqlResult]:
        """Execute every statement in a ``.sql`` file under the repo ``sql/`` tree.

        Statements are split and comment-stripped by
        :func:`spark_sql._helpers.read_sql_statements`, so the file is executed
        exactly the way the pytest suite reads it.

        Args:
            sql_path: Repo-relative path to the ``.sql`` file (e.g. ``"sql/scd/type2/expire.sql"``).
            skip_predicate: Optional filter; statements for which it returns ``True`` are skipped.
            transform: Optional rewrite applied to each statement before execution; returning
                ``None``/empty skips the statement (e.g. to inject a catalog prefix).
            queries_only: When ``True``, only results of query statements (``SELECT``/``WITH``/…)
                are returned; DDL/DML still executes but its (empty) result is omitted.
            explain: When ``True``, capture each returned result's physical plan.

        Returns:
            One :class:`SqlResult` per executed (and, if *queries_only*, query) statement,
            in file order.
        """
        results: list[SqlResult] = []
        for statement in read_sql_statements(sql_path):
            if skip_predicate and skip_predicate(statement):
                continue
            executable = transform(statement) if transform else statement
            if not executable:
                continue
            is_query = executable.lstrip().upper().startswith(_QUERY_PREFIXES)
            result = self._backend.run(executable, explain=explain and is_query)
            if queries_only and not is_query:
                continue
            results.append(result)
        logger.info("Ran %s -> %d result(s) on %s", sql_path, len(results), self._backend.name)
        return results
