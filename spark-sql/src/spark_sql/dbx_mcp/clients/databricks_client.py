"""Databricks SDK ``WorkspaceClient`` construction for ``dbx_mcp`` tools.

Centralizes auth resolution (config profile vs. explicit host/token, sourced
from :data:`spark_sql.dbx_mcp.config.settings.settings`) so every tool module
builds its client the same way.
"""

import logging

from databricks.sdk import WorkspaceClient

from spark_sql.dbx_mcp.config.settings import settings
from spark_sql.util.cli_env import get_workspace_client as _get_workspace_client

logger = logging.getLogger(__name__)


def get_workspace_client() -> WorkspaceClient:
    """Build a Databricks WorkspaceClient using either a config profile or a PAT.

    If ``databricks_config_profile`` is set (e.g. via ``DATABRICKS_CONFIG_PROFILE``),
    authentication is delegated to the named profile in ``~/.databrickscfg``. Otherwise,
    the client falls back to the explicit host/token pair from settings.

    Returns:
        A configured WorkspaceClient instance.
    """
    if settings.databricks_config_profile:
        logger.debug("Authenticating via DATABRICKS_CONFIG_PROFILE=%s", settings.databricks_config_profile)
    else:
        logger.debug("Authenticating via DATABRICKS_HOST/DATABRICKS_TOKEN (host=%s)", settings.databricks_host)

    return _get_workspace_client(
        profile=settings.databricks_config_profile,
        host=settings.databricks_host,
        token=settings.databricks_token,
    )
