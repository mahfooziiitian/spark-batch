import os
from string import Template

from pydantic import BaseModel


class EnvironmentConfigError(Exception):
    """Raised when one or more required environment variables are missing or empty."""


class DashboardEnvTemplate(Template):
    delimiter = "$$"
    idpattern = r"[_a-z][_a-z0-9]*"


# Required environment variables. No fallbacks — set these directly.
REQUIRED_ENV_VARS: dict[str, str | None] = {
    "DATABRICKS_HOST": None,
    "DATABRICKS_TOKEN": None,
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
    DBX_WAREHOUSE: str
    ENV_SHORT: str
    ENV_LONG: str
    AUDIT_SCHEMA: str
