"""Pluggable execution backends for the SQL runner.

A backend knows how to execute one SQL statement and return a backend-neutral
:class:`~spark_sql.runner.result.SqlResult`. Two backends ship with the library:

* :class:`LocalSparkBackend` — runs against a local (or any) :class:`SparkSession`,
  used to validate the ``sql/`` examples exactly as the pytest suite does.
* :class:`DatabricksBackend` — runs against a Databricks SQL warehouse through the
  Databricks SDK's Statement Execution API (the same path the ``dbx_mcp`` server
  uses), so the identical runner/examiner/validator code works on Databricks.

Both share the ABC below, which centralizes ``SqlResult`` construction and the
``EXPLAIN`` plan-capture logic so each concrete backend only implements a single
low-level ``_execute`` method.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from spark_sql.runner.exceptions import BackendError
from spark_sql.runner.result import SqlResult

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_EXPLAIN_MODES = frozenset({"", "FORMATTED", "EXTENDED", "CODEGEN", "COST"})


class SqlBackend(ABC):
    """Abstract execution target for a single SQL statement.

    Subclasses implement :meth:`_execute` (the raw ``(columns, rows)`` fetch) and
    :attr:`name`; :meth:`run` and :meth:`explain` are provided here so result
    construction and plan capture stay identical across backends.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short, stable identifier for this backend (e.g. ``"local-spark"``)."""

    @abstractmethod
    def _execute(self, statement: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        """Execute *statement* and return ``(column_names, rows)``.

        Raises:
            BackendError: If the underlying engine fails to execute the statement.
        """

    def run(self, statement: str, *, explain: bool = False) -> SqlResult:
        """Execute *statement* and return a :class:`SqlResult`.

        Args:
            statement: A single SQL statement (no trailing semicolon required).
            explain: When ``True``, also capture the physical plan via ``EXPLAIN
                FORMATTED`` and attach it to :attr:`SqlResult.plan`.

        Returns:
            The materialized result, optionally carrying the physical plan.

        Raises:
            BackendError: If execution (or the optional explain) fails.
        """
        columns, rows = self._execute(statement)
        plan = self.explain(statement) if explain else None
        logger.debug("[%s] ran statement -> %d row(s), %d column(s)", self.name, len(rows), len(columns))
        return SqlResult(statement=statement, columns=columns, rows=rows, backend=self.name, plan=plan)

    def explain(self, statement: str, mode: str = "FORMATTED") -> str:
        """Return the plan for *statement* via ``EXPLAIN [mode]``.

        Args:
            statement: The statement to explain.
            mode: One of ``""``, ``"FORMATTED"``, ``"EXTENDED"``, ``"CODEGEN"``,
                ``"COST"`` (case-insensitive).

        Returns:
            The plan text (all plan rows joined by newlines).

        Raises:
            ValueError: If *mode* is not a recognized ``EXPLAIN`` mode.
            BackendError: If the backend fails to produce a plan.
        """
        normalized = mode.strip().upper()
        if normalized not in _EXPLAIN_MODES:
            raise ValueError(f"Unsupported EXPLAIN mode {mode!r}; expected one of {sorted(_EXPLAIN_MODES)}")
        prefix = f"EXPLAIN {normalized}".strip()
        _columns, rows = self._execute(f"{prefix} {statement}")
        return "\n".join(str(row[0]) for row in rows if row)


class LocalSparkBackend(SqlBackend):
    """Execute statements against a provided :class:`SparkSession`.

    The session is injected (never created here, per project convention), so the
    caller controls its configuration and lifecycle.
    """

    def __init__(self, spark: SparkSession, *, backend_name: str = "local-spark") -> None:
        self._spark = spark
        self._name = backend_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def spark(self) -> SparkSession:
        """The wrapped :class:`SparkSession`."""
        return self._spark

    def _execute(self, statement: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        try:
            dataframe = self._spark.sql(statement)
            columns = list(dataframe.columns)
            rows = [tuple(row) for row in dataframe.collect()]
        except Exception as exc:
            raise BackendError(f"local-spark failed to execute statement: {exc}") from exc
        return columns, rows


class DatabricksBackend(SqlBackend):
    """Execute statements on a Databricks SQL warehouse via the SDK.

    Uses ``WorkspaceClient.statement_execution.execute_statement`` — the same
    Statement Execution API the ``dbx_mcp`` MCP server uses — so a query can be
    run, examined, and validated identically to the local Spark path.
    """

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        *,
        backend_name: str = "databricks",
    ) -> None:
        self._client = workspace_client
        self._warehouse_id = warehouse_id
        self._name = backend_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def warehouse_id(self) -> str:
        """The SQL warehouse ID statements are executed against."""
        return self._warehouse_id

    @classmethod
    def from_env(
        cls,
        *,
        warehouse_id: str | None = None,
        warehouse_name: str | None = None,
        profile: str | None = None,
        host: str | None = None,
        token: str | None = None,
        backend_name: str = "databricks",
    ) -> DatabricksBackend:
        """Build a backend from explicit values or the standard Databricks env vars.

        Authentication follows :func:`spark_sql.util.cli_env.get_workspace_client`
        (profile → host/token → SDK default resolution). The warehouse is resolved
        from *warehouse_id*, then *warehouse_name*, then ``DATABRICKS_WAREHOUSE_ID`` /
        ``DATABRICKS_WAREHOUSE_NAME``.

        Raises:
            BackendError: If no warehouse can be resolved.
        """
        import os

        from spark_sql.util.cli_env import get_workspace_client

        client = get_workspace_client(profile=profile, host=host, token=token)

        resolved_id = (warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")).strip()
        if not resolved_id:
            name = (warehouse_name or os.environ.get("DATABRICKS_WAREHOUSE_NAME", "")).strip()
            if not name:
                raise BackendError(
                    "No warehouse configured: pass warehouse_id/warehouse_name or set "
                    "DATABRICKS_WAREHOUSE_ID / DATABRICKS_WAREHOUSE_NAME.",
                )
            resolved_id = cls._resolve_warehouse_name(client, name)

        return cls(client, resolved_id, backend_name=backend_name)

    @staticmethod
    def _resolve_warehouse_name(client: WorkspaceClient, warehouse_name: str) -> str:
        for warehouse in client.warehouses.list():
            if warehouse.name == warehouse_name and warehouse.id:
                return warehouse.id
        raise BackendError(f"No SQL warehouse named {warehouse_name!r} found in the workspace.")

    def _execute(self, statement: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        try:
            response = self._client.statement_execution.execute_statement(
                warehouse_id=self._warehouse_id,
                statement=statement,
            )
        except Exception as exc:
            raise BackendError(f"databricks failed to execute statement: {exc}") from exc

        manifest = response.manifest
        schema = manifest.schema if manifest else None
        columns = [column.name or "" for column in schema.columns] if schema and schema.columns else []

        result = response.result
        data_array = result.data_array if result and result.data_array else []
        rows = [tuple(row) for row in data_array]
        return columns, rows
