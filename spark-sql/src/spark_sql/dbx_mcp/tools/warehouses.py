"""SQL warehouse listing and name resolution for the ``dbx_mcp`` server."""

import logging

from spark_sql.dbx_mcp.clients.databricks_client import (
    get_workspace_client,
)

logger = logging.getLogger(__name__)


def list_warehouses() -> list[dict]:
    """List SQL warehouses visible to the configured Databricks credentials.

    Returns:
        One dict per warehouse with ``id``, ``name``, and ``state`` keys.
    """
    logger.info("Listing SQL warehouses")
    workspace_client = get_workspace_client()

    warehouses = [
        {
            "id": warehouse.id,
            "name": warehouse.name,
            "state": warehouse.state.value if warehouse.state else None,
        }
        for warehouse in workspace_client.warehouses.list()
    ]
    logger.info("Found %d warehouse(s)", len(warehouses))
    return warehouses


def get_warehouse_id(warehouse_name: str) -> str | None:
    """Resolve a SQL warehouse display name to its ID.

    Args:
        warehouse_name: Display name of the warehouse to look up, exactly as
            shown in the Databricks SQL Warehouses UI (case-sensitive).

    Returns:
        The warehouse ID string, or None if no warehouse with that name exists.
    """
    logger.info("Resolving warehouse id for name=%s", warehouse_name)
    workspace_client = get_workspace_client()

    for warehouse in workspace_client.warehouses.list():
        if warehouse.name == warehouse_name:
            logger.info("Resolved warehouse '%s' -> %s", warehouse_name, warehouse.id)
            return warehouse.id

    logger.warning("No warehouse found with name=%s", warehouse_name)
    return None
