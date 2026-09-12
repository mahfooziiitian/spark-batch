"""Environment configuration models for the ``dbx_mcp`` server and dashboard tooling.

Defines the required-environment-variable validation used by CLI entry points
(:func:`validate_env_vars`) and the :class:`EnvConfig` model consumed by
``DashboardManager``/``spark_sql.util.factory`` to parameterize per-environment
Databricks resources (warehouse, schema, catalog names).
"""

import os
from string import Template

from pydantic import BaseModel


class EnvironmentConfigError(Exception):
    """Raised when one or more required environment variables are missing or empty."""


class DashboardEnvTemplate(Template):
    """``string.Template`` variant using ``$$`` delimiters for dashboard JSON templating.

    Dashboard definition files already use single ``$`` for Lakeview's own
    parameter syntax, so this template class uses a doubled ``$$`` delimiter to
    avoid colliding with it when substituting environment-specific values.
    """

    delimiter = "$$"
    idpattern = r"[_a-z][_a-z0-9]*"


# Required environment variables. No fallbacks — set these directly.
REQUIRED_ENV_VARS: dict[str, str | None] = {
    "DATABRICKS_HOST": None,
    "DATABRICKS_TOKEN": None,  # nosec B105 - env var name placeholder, not a hardcoded secret
}


def validate_env_vars() -> None:
    """Validate that all required environment variables are set and non-empty.

    Each variable is checked against its primary key first, then its fallback
    (if one exists). If neither provides a non-empty value the variable is
    reported as missing.

    Raises:
        EnvironmentConfigError: When one or more required variables are missing,
            listing every missing variable in the error message.
    """
    missing: list[str] = []
    for var, fallback in REQUIRED_ENV_VARS.items():
        value = os.environ.get(var, "").strip()
        if not value and fallback:
            value = os.environ.get(fallback, "").strip()
        if not value:
            hint = f" (or {fallback})" if fallback else ""
            missing.append(f"{var}{hint}")

    if missing:
        formatted = ", ".join(missing)
        raise EnvironmentConfigError(
            f"Missing required environment variable(s): {formatted}. Set them before running or add them to a .env file."
        )


class EnvConfig(BaseModel):
    """Per-environment settings used to parameterize dashboard/warehouse deployments.

    Attributes:
        DBX_WAREHOUSE: Display name of the SQL warehouse to target.
        ENV_SHORT: Short environment code (e.g. ``"dev"``, ``"stg"``, ``"prod"``).
        ENV_LONG: Full environment name (e.g. ``"development"``, ``"production"``).
        AUDIT_SCHEMA: Schema name used for audit-related objects.
    """

    DBX_WAREHOUSE: str
    ENV_SHORT: str
    ENV_LONG: str
    AUDIT_SCHEMA: str
