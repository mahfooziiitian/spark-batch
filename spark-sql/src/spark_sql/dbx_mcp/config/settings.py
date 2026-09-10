import os

from pydantic_settings import BaseSettings, SettingsConfigDict

from spark_sql.util.cli_env import load_env_layers

# Config keys that may be forwarded by the MCP host (see .mcp.json ``env`` block).
# The VS Code / Copilot MCP client substitutes an unset ``${env:VAR}`` with an empty
# string, which would otherwise shadow the value in ``.env``/``.env.$(DAB_TARGET)``. Drop
# empties so the passthrough stays optional and the dotenv layers remain the fallback.
_OPTIONAL_ENV_KEYS = (
    "DAB_TARGET",
    "DATABRICKS_CONFIG_PROFILE",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_WAREHOUSE_NAME",
    "DATABRICKS_WAREHOUSE_ID",
)
for _key in _OPTIONAL_ENV_KEYS:
    if os.environ.get(_key, None) == "":
        del os.environ[_key]

# Load the same ``.env`` + ``.env.$(DAB_TARGET)`` layering the Makefile and other CLI
# entry points use, so dbx_mcp resolves dev/staging/prod the same way `make dab-*` does.
# This populates os.environ directly, so ``Settings`` below reads everything from its
# default env-var source alone — no ``env_file`` needed (which only covers ``.env`` and
# can't express the per-target override layer anyway). Values already exported in the
# process env (e.g. by ``.mcp.json``) still win over ``.env``, but a ``.env.<target>``
# layer overrides ``.env`` intentionally.
_dab_target = load_env_layers()


class Settings(BaseSettings):
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_warehouse_id: str | None = None
    databricks_warehouse_name: str | None = None
    databricks_config_profile: str | None = None
    template_key_values: str = "{}"
    dab_target: str = _dab_target
    model_config = SettingsConfigDict(extra="ignore")


settings = Settings()
