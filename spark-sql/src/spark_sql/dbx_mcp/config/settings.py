"""Environment-variable-backed settings for the ``dbx_mcp`` server.

Loads the same ``.env`` + ``.env.<APP_ENV>`` layering used by the Makefile
(via :func:`spark_sql.util.cli_env.load_env_layers`) and drops MCP-host
passthrough env vars (see ``.mcp.json``) when they resolve to an empty string,
so an unset ``${env:VAR}`` substitution doesn't shadow a ``.env`` value.
"""

import os

from pydantic_settings import BaseSettings, SettingsConfigDict

from spark_sql.util.cli_env import load_env_layers

# Config keys that may be forwarded by the MCP host (see .mcp.json ``env`` block).
# The VS Code / Copilot MCP client substitutes an unset ``${env:VAR}`` with an empty
# string, which would otherwise shadow the value in ``.env``/``.env.$(APP_ENV)``. Drop
# empties so the passthrough stays optional and the dotenv layers remain the fallback.
_OPTIONAL_ENV_KEYS = (
    "APP_ENV",
    "DATABRICKS_CONFIG_PROFILE",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_WAREHOUSE_NAME",
    "DATABRICKS_WAREHOUSE_ID",
)
for _key in _OPTIONAL_ENV_KEYS:
    if os.environ.get(_key, None) == "":
        del os.environ[_key]

# Load the same ``.env`` + ``.env.$(APP_ENV)`` layering the Makefile and other CLI
# entry points use, so dbx_mcp resolves dev/staging/prod the same way `make dab-*` does.
# This populates os.environ directly, so ``Settings`` below reads everything from its
# default env-var source alone — no ``env_file`` needed (which only covers ``.env`` and
# can't express the per-target override layer anyway). Values already exported in the
# process env (e.g. by ``.mcp.json``) still win over ``.env``, but a ``.env.<target>``
# layer overrides ``.env`` intentionally.
_app_env = load_env_layers()


class Settings(BaseSettings):
    """Runtime configuration for the ``dbx_mcp`` server, resolved from the environment.

    Attributes:
        databricks_host: Databricks workspace URL (used when no config profile is set).
        databricks_token: Databricks personal access token.
        databricks_warehouse_id: Explicit SQL warehouse ID for system-table queries.
        databricks_warehouse_name: SQL warehouse display name, resolved to an ID if
            ``databricks_warehouse_id`` is unset.
        databricks_config_profile: Named profile in ``~/.databrickscfg`` to authenticate
            with, taking precedence over ``databricks_host``/``databricks_token``.
        template_key_values: JSON-encoded key/value overrides for dashboard templating.
        app_env: Resolved deployment target (``"dev"``/``"staging"``/``"prod"``, etc.).
    """

    databricks_host: str = ""
    databricks_token: str = ""
    databricks_warehouse_id: str | None = None
    databricks_warehouse_name: str | None = None
    databricks_config_profile: str | None = None
    template_key_values: str = "{}"
    app_env: str = _app_env
    model_config = SettingsConfigDict(extra="ignore")


settings = Settings()
