"""Read-only system table / information_schema querying for the ``dbx_mcp`` server."""

import logging
import re

from spark_sql.dbx_mcp.clients.databricks_client import (
    get_workspace_client,
)
from spark_sql.dbx_mcp.config.settings import settings
from spark_sql.dbx_mcp.tools.warehouses import get_warehouse_id

logger = logging.getLogger(__name__)

# Observability tools are read-only: mutations must go through the grants
# tools instead, which use typed SDK calls rather than free-form SQL.
_READONLY_PREFIX = re.compile(r"^\s*(SELECT|SHOW|DESCRIBE|WITH|EXPLAIN)\b", re.IGNORECASE)

_DEFAULT_ROW_LIMIT = 500


def _resolve_warehouse_id() -> str:
    """Resolve the SQL warehouse ID to use for system table queries.

    Prefers an explicit ``DATABRICKS_WAREHOUSE_ID``. Falls back to resolving
    ``DATABRICKS_WAREHOUSE_NAME`` to its ID via the SQL Warehouses API, so
    either env var alone is sufficient to configure this tool.

    Returns:
        The resolved warehouse ID.

    Raises:
        ValueError: If neither a warehouse ID nor a resolvable warehouse name is configured.
    """
    if settings.databricks_warehouse_id:
        return settings.databricks_warehouse_id

    if settings.databricks_warehouse_name:
        warehouse_id = get_warehouse_id(settings.databricks_warehouse_name)
        if warehouse_id:
            logger.info(
                "Resolved databricks_warehouse_name=%s -> warehouse_id=%s",
                settings.databricks_warehouse_name,
                warehouse_id,
            )
            return warehouse_id
        raise ValueError(
            f"No warehouse named '{settings.databricks_warehouse_name}' (DATABRICKS_WAREHOUSE_NAME) was found in the workspace."
        )

    raise ValueError("Neither databricks_warehouse_id nor databricks_warehouse_name is configured.")


def query_system_table(
    sql: str,
    row_limit: int = _DEFAULT_ROW_LIMIT,
) -> list[dict]:
    """Run a read-only SQL query against Databricks system tables or information_schema.

    Covers tables such as ``system.access.audit``, ``system.billing.usage``,
    ``system.access.table_lineage``, and ``information_schema.volumes``.

    Only SELECT/SHOW/DESCRIBE/WITH/EXPLAIN statements are permitted.

    Args:
        sql: The read-only SQL statement to execute.
        row_limit: Maximum number of result rows to return.

    Returns:
        Result rows as a list of ``{column: value}`` dicts, truncated to *row_limit*.

    Raises:
        ValueError: If *sql* is not a read-only statement, or no warehouse is configured.
    """
    if not _READONLY_PREFIX.match(sql):
        logger.warning("Rejected non-read-only system table query")
        raise ValueError(
            "Only read-only statements (SELECT/SHOW/DESCRIBE/WITH/EXPLAIN) are allowed. "
            "Use the grants tools for catalog/schema privilege changes."
        )

    logger.debug("Executing system table query (row_limit=%d): %s", row_limit, sql)

    workspace_client = get_workspace_client()
    warehouse_id = _resolve_warehouse_id()

    response = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
    )

    result = response.result
    columns = (
        [c.name for c in response.manifest.schema.columns]
        if response.manifest and response.manifest.schema and response.manifest.schema.columns
        else []
    )
    data_array = result.data_array if result and result.data_array else []

    rows = [dict(zip(columns, row, strict=False)) for row in data_array[:row_limit]]

    logger.info("System table query returned %d row(s)", len(rows))
    return rows
