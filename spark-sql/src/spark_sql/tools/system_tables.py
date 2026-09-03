import re

from spark_sql.clients.databricks_client import get_workspace_client
from spark_sql.config.settings import settings

# Observability tools are read-only: mutations must go through the grants
# tools instead, which use typed SDK calls rather than free-form SQL.
_READONLY_PREFIX = re.compile(r"^\s*(SELECT|SHOW|DESCRIBE|WITH|EXPLAIN)\b", re.IGNORECASE)

_DEFAULT_ROW_LIMIT = 500


def query_system_table(
    sql: str,
    row_limit: int = _DEFAULT_ROW_LIMIT,
) -> list[dict]:
    """
    Run a read-only SQL query against Databricks system tables or
    information_schema (e.g. system.access.audit, system.billing.usage,
    system.access.table_lineage, information_schema.volumes).

    Only SELECT/SHOW/DESCRIBE/WITH/EXPLAIN statements are permitted.
    """

    if not _READONLY_PREFIX.match(sql):
        raise ValueError(
            "Only read-only statements (SELECT/SHOW/DESCRIBE/WITH/EXPLAIN) are allowed. "
            "Use the grants tools for catalog/schema privilege changes."
        )

    workspace_client = get_workspace_client()
    warehouse_id = settings.databricks_warehouse_id
    if not warehouse_id:
        raise ValueError("databricks_warehouse_id is not configured.")

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

    return rows
