import logging

from spark_sql.dbx_mcp.clients.databricks_client import (
    get_workspace_client,
)

logger = logging.getLogger(__name__)


def list_tables(
    catalog_name: str,
    schema_name: str,
) -> list[dict]:
    logger.info("Listing tables in catalog=%s schema=%s", catalog_name, schema_name)
    workspace_client = get_workspace_client()

    tables = [
        {
            "name": table.name,
            "full_name": table.full_name,
            "table_type": (table.table_type.value if table.table_type else None),
        }
        for table in workspace_client.tables.list(
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
    ]
    logger.info("Found %d table(s) in catalog=%s schema=%s", len(tables), catalog_name, schema_name)
    return tables
