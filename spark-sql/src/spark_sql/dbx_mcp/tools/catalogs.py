"""Unity Catalog catalog listing for the ``dbx_mcp`` server."""

import logging

from spark_sql.dbx_mcp.clients.databricks_client import (
    get_workspace_client,
)

logger = logging.getLogger(__name__)


def list_catalogs() -> list[dict]:
    """List Unity Catalog catalogs visible to the configured Databricks credentials.

    Returns:
        One dict per catalog with ``name`` and ``comment`` keys.
    """
    logger.info("Listing catalogs")
    workspace_client = get_workspace_client()

    catalogs = [
        {
            "name": catalog.name,
            "comment": catalog.comment,
        }
        for catalog in workspace_client.catalogs.list()
    ]
    logger.info("Found %d catalog(s)", len(catalogs))
    return catalogs
