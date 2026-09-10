"""Factory helpers for building common objects from environment variables.

Provides convenience functions used by CLI scripts and examples to construct
``EnvConfig`` and ``DashboardManager`` instances without repeating boilerplate
env-var reading and definition-file discovery logic. Also re-exports
:func:`apply_databricks_config_profile` (from :mod:`spark_sql.util.cli_env`) so
callers can support authenticating via a ``DATABRICKS_CONFIG_PROFILE`` in
addition to explicit host/token env vars.
"""

from __future__ import annotations

import os

from spark_sql.model.env import EnvConfig
from spark_sql.util.cli_env import apply_databricks_config_profile

__all__ = ["apply_databricks_config_profile", "env_config_from_env"]


def env_config_from_env() -> EnvConfig:
    """Build an ``EnvConfig`` from environment variables with sensible defaults.

    Reads the following environment variables:

    - ``DATABRICKS_WAREHOUSE_NAME`` (default: ``"Starter Warehouse"``)
    - ``ENV_SHORT`` (default: ``"dev"``)
    - ``ENV_LONG`` (default: ``"development"``)
    - ``AUDIT_SCHEMA`` (default: ``"audit"``)

    Returns:
        A populated ``EnvConfig`` instance.
    """
    return EnvConfig(
        DBX_WAREHOUSE=os.environ.get("DATABRICKS_WAREHOUSE_NAME", "Starter Warehouse"),
        ENV_SHORT=os.environ.get("ENV_SHORT", "dev"),
        ENV_LONG=os.environ.get("ENV_LONG", "development"),
        AUDIT_SCHEMA=os.environ.get("AUDIT_SCHEMA", "audit"),
    )
