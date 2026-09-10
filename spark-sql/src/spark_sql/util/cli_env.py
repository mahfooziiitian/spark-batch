"""Shared environment-resolution helpers for CLI entry points.

Both implementation paths (``spark_sql.rest.*`` and ``spark_sql.sdk.*`` entry-point scripts)
need to normalize/validate the same core Databricks auth environment variables before doing
any work. This module centralizes that logic so each entry point doesn't reimplement it.
"""

import logging
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from dotenv import load_dotenv

from spark_sql.model.env import EnvConfig

logger = logging.getLogger(__name__)


def load_env_layers(target_env_var: str = "DAB_TARGET", default_target: str = "dev") -> str:
    """Load ``.env`` in the same base + target-override layers the Makefile uses.

    Standard per-environment dotenv layering (the same pattern Node/Next.js/Rails use):
    a shared base file plus one override file per environment, selected by a single
    env-var name. Concretely:

        1. ``.env``                    — shared/non-target-specific values.
        2. ``.env.<target>``           — environment-specific overrides (``dev``/``staging``/
                                          ``prod``), where ``<target>`` is the value of
                                          *target_env_var* (``DAB_TARGET`` by default, matching
                                          the Makefile's own layering so `make dab-*`/`make deploy`
                                          and a directly-invoked entry point (e.g. running
                                          ``deploy-dashboard`` without ``make``) resolve the same
                                          values either way.

    Both files are optional; missing files are silently skipped. Values already present in
    the process environment (e.g. exported by ``make``, or set directly by CI) are never
    overridden by ``.env``, but *are* overridden by ``.env.<target>`` so a target file can
    always express an intentional per-environment override.

    Args:
        target_env_var: Name of the env var selecting which target's override file to load.
        default_target: Target to use when *target_env_var* is unset.

    Returns:
        The resolved target name (e.g. ``"dev"``), for callers that want to log/use it.
    """
    load_dotenv()  # .env - never overrides already-exported values

    target = os.environ.get(target_env_var, default_target).strip() or default_target
    target_file = f".env.{target}"
    if os.path.isfile(target_file):
        load_dotenv(target_file, override=True)

    return target


def get_workspace_client(
    *,
    profile: str | None = None,
    host: str | None = None,
    token: str | None = None,
) -> WorkspaceClient:
    """Build a ``WorkspaceClient`` supporting every Databricks auth method.

    Centralizes the ``WorkspaceClient()`` construction used across ``spark_sql.sdk``,
    ``spark_sql.dbx_mcp``, and any other caller, so auth precedence is defined once.

    Precedence:
        1. *profile* (falls back to ``DATABRICKS_CONFIG_PROFILE``) — delegates to the
           named profile in ``~/.databrickscfg``, supporting any ``auth_type`` it declares
           (PAT, OAuth user-to-machine, OAuth service principal, ``databricks-cli``, etc.).
        2. *host*/*token* (falls back to ``DATABRICKS_HOST``/``DATABRICKS_TOKEN``) — direct
           personal access token auth.
        3. Neither set — falls back to the SDK's own unified-auth default resolution (env
           vars, the ``DEFAULT`` profile in ``~/.databrickscfg``, OAuth service-principal env
           vars, Azure MSI, GCP credentials, the instance metadata service, etc.).

    Args:
        profile: Named profile from ``~/.databrickscfg``. Defaults to the
            ``DATABRICKS_CONFIG_PROFILE`` env var.
        host: Databricks workspace URL. Defaults to the ``DATABRICKS_HOST`` env var.
        token: Databricks personal access token. Defaults to the ``DATABRICKS_TOKEN`` env var.

    Returns:
        A configured ``WorkspaceClient`` instance.
    """
    resolved_profile: str = (profile or "") or os.environ.get("DATABRICKS_CONFIG_PROFILE", "")
    resolved_profile = resolved_profile.strip()
    if resolved_profile:
        return WorkspaceClient(profile=resolved_profile)

    resolved_host: str = ((host or "") or os.environ.get("DATABRICKS_HOST", "")).strip()
    resolved_token: str = ((token or "") or os.environ.get("DATABRICKS_TOKEN", "")).strip()
    if resolved_host or resolved_token:
        return WorkspaceClient(host=resolved_host or None, token=resolved_token or None)

    return WorkspaceClient()


def apply_databricks_config_profile() -> None:
    """Resolve ``DATABRICKS_HOST``/``DATABRICKS_TOKEN`` from a config profile.

    When ``DATABRICKS_CONFIG_PROFILE`` is set, looks up the named profile in
    ``~/.databrickscfg`` (via the Databricks SDK) and exports its resolved host and an
    access token as ``DATABRICKS_HOST``/``DATABRICKS_TOKEN``, overriding any values
    already present. This lets ``DashboardManager`` and other code that reads those two
    variables directly work unchanged, whether the profile uses a PAT, OAuth, or
    ``databricks-cli`` auth type. Call this once, before :func:`resolve_host`/
    :func:`resolve_token`, so a profile takes priority over explicit host/token env vars.

    A no-op when no profile is configured.
    """
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "").strip()
    if not profile:
        return

    config = Config(profile=profile)
    if config.host:
        os.environ["DATABRICKS_HOST"] = config.host

    auth_headers = config.authenticate()
    token = auth_headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if token:
        os.environ["DATABRICKS_TOKEN"] = token


def resolve_host(required: bool = True) -> str:
    """Return a normalized Databricks workspace URL from ``DATABRICKS_HOST``.

    Prefixes the host with ``https://`` when missing and re-exports the normalized value
    back to ``os.environ`` so downstream code reading the env var directly sees it too.

    Args:
        required: When ``True``, a missing/empty host logs an error and exits the process.
            When ``False``, an empty string is returned instead (for read-only CLIs like
            list/query scripts that shouldn't hard-fail on missing auth).

    Returns:
        The normalized host, or ``""`` when unset and *required* is ``False``.

    Raises:
        SystemExit: When *required* is ``True`` and no host can be resolved.
    """
    host = os.environ.get("DATABRICKS_HOST", "").strip()
    if host and not host.startswith("https://"):
        host = f"https://{host}"
    os.environ["DATABRICKS_HOST"] = host

    if not host and required:
        logger.error("DATABRICKS_HOST is not set. Export it before running or add it to a .env file.")
        sys.exit(1)

    return host


def resolve_token(required: bool = True) -> str:
    """Return the Databricks PAT from ``DATABRICKS_TOKEN``.

    Args:
        required: When ``True``, a missing/empty token logs an error and exits the process.
            When ``False``, an empty string is returned instead.

    Returns:
        The token string, or ``""`` when unset and *required* is ``False``.

    Raises:
        SystemExit: When *required* is ``True`` and no token can be resolved.
    """
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()

    if not token and required:
        logger.error("DATABRICKS_TOKEN is not set. Export it before running or add it to a .env file.")
        sys.exit(1)

    return token


def resolve_auth_env(*, required: bool = True) -> str:
    """Resolve auth boilerplate shared by the ``deploy``/``delete`` CLI entry points.

    Applies a config profile (if set), normalizes ``DATABRICKS_HOST``/``DATABRICKS_TOKEN``,
    defaults ``TEMPLATE_KEY_VALUES`` to ``"{}"`` when unset, and logs both resolved values.
    Shared by ``rest/deploy_spark_sql.py``, ``rest/delete_spark_sql.py``, and
    ``sdk/deploy_spark_sql.py``, which otherwise duplicated this exact sequence.

    Args:
        required: Passed through to :func:`resolve_host`/:func:`resolve_token` — when
            ``True`` (the default), a missing host/token exits the process.

    Returns:
        The normalized ``DATABRICKS_HOST`` value.

    Raises:
        SystemExit: When *required* is ``True`` and the host or token can't be resolved.
    """
    apply_databricks_config_profile()

    host = resolve_host(required=required)
    resolve_token(required=required)

    os.environ.setdefault("TEMPLATE_KEY_VALUES", "{}")

    logger.info("DATABRICKS_HOST     : %s", host)
    logger.info("TEMPLATE_KEY_VALUES : %s", os.environ["TEMPLATE_KEY_VALUES"])

    return host


def bootstrap_rest_list_env(
    *,
    warehouse_name: str = "Starter Endpoint",
    env_short: str = "dev",
    env_long: str = "development",
    audit_schema: str = "audit",
) -> EnvConfig:
    """Resolve auth/logging boilerplate shared by the ``rest`` list-* CLI scripts.

    Applies a config profile (if set), normalizes ``DATABRICKS_HOST``/``DATABRICKS_TOKEN``/
    ``TEMPLATE_KEY_VALUES``, logs their resolved values, and builds the ``EnvConfig`` used to
    construct a ``DashboardManager``. Shared by ``rest/list_spark_sql.py`` and
    ``rest/list_dashboard_schedules.py``, which are otherwise identical apart from what they
    do with each discovered spark_sql.

    Args:
        warehouse_name: Value for ``EnvConfig.DBX_WAREHOUSE``.
        env_short: Value for ``EnvConfig.ENV_SHORT``.
        env_long: Value for ``EnvConfig.ENV_LONG``.
        audit_schema: Value for ``EnvConfig.AUDIT_SCHEMA``.

    Returns:
        A populated ``EnvConfig`` instance.
    """
    apply_databricks_config_profile()
    resolve_host(required=False)
    os.environ["DATABRICKS_TOKEN"] = os.getenv("DATABRICKS_TOKEN", "")
    os.environ["TEMPLATE_KEY_VALUES"] = os.getenv("TEMPLATE_KEY_VALUES", "{}")
    logger.info("DATABRICKS_HOST: %s", os.environ["DATABRICKS_HOST"])
    logger.info("DATABRICKS_TOKEN: %s", "***" if os.environ.get("DATABRICKS_TOKEN") else "(not set)")
    logger.info("TEMPLATE_KEY_VALUES: %s", os.environ["TEMPLATE_KEY_VALUES"])

    return EnvConfig(
        DBX_WAREHOUSE=warehouse_name,
        ENV_SHORT=env_short,
        ENV_LONG=env_long,
        AUDIT_SCHEMA=audit_schema,
    )
