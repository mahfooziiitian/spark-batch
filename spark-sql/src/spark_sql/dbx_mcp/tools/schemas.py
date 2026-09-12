"""Unity Catalog schema listing for the ``dbx_mcp`` server."""

import logging

from spark_sql.dbx_mcp.clients.databricks_client import (
    get_workspace_client,
)

logger = logging.getLogger(__name__)


def list_schemas(
    catalog_name: str,
) -> list[dict]:
    """List schemas in a Unity Catalog catalog.

    Args:
        catalog_name: Name of the catalog to list schemas from.

    Returns:
        One dict per schema with ``name``, ``catalog_name``, and ``comment`` keys.
    """
    logger.info("Listing schemas in catalog=%s", catalog_name)
    workspace_client = get_workspace_client()

    schemas = [
        {
            "name": schema.name,
            "catalog_name": schema.catalog_name,
            "comment": schema.comment,
        }
        for schema in workspace_client.schemas.list(
            catalog_name=catalog_name,
        )
    ]
    logger.info("Found %d schema(s) in catalog=%s", len(schemas), catalog_name)
    return schemas
