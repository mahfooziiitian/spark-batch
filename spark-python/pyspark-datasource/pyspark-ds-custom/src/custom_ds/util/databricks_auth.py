"""Databricks authentication utilities.

Resolves a temporary access token and workspace host from a Databricks CLI
profile (``~/.databrickscfg``) or environment variables using the
``databricks-sdk`` unified auth.

Usage::

    from custom_ds.util.databricks_auth import get_databricks_auth, create_arg_parser

    parser = create_arg_parser("List Databricks jobs")
    args = parser.parse_args()
    auth = get_databricks_auth(args.profile)

    print(auth.host)    # https://<workspace>.cloud.databricks.com
    print(auth.token)   # temporary or PAT token

CLI usage::

    python script.py --profile dev
    # or via env var:
    DATABRICKS_PROFILE=dev python script.py
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from custom_ds.util.log import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class DatabricksAuth:
    """Resolved Databricks host and token pair."""

    host: str
    token: str


def create_arg_parser(
    description: str = "Databricks REST API example",
) -> argparse.ArgumentParser:
    """Create an ArgumentParser pre-configured with ``--profile``.

    Subclasses / callers can add extra arguments before calling ``parse_args()``.

    Args:
        description: Script description shown in ``--help``.

    Returns:
        An :class:`argparse.ArgumentParser` with ``--profile`` already added.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--profile",
        default=os.environ.get("DATABRICKS_PROFILE"),
        help="Databricks CLI profile from ~/.databrickscfg (default: DATABRICKS_PROFILE env var)",
    )
    return parser


def get_databricks_auth(profile: str | None = None) -> DatabricksAuth:
    """Resolve a Databricks host and token.

    Resolution order:
        1. ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN`` environment variables
        2. ``databricks-sdk`` ``WorkspaceClient`` with the given *profile*
           (or the ``DATABRICKS_PROFILE`` env var, or the default profile)

    Args:
        profile: CLI profile name from ``~/.databrickscfg``.
            Falls back to ``DATABRICKS_PROFILE`` env var, then default profile.

    Returns:
        A :class:`DatabricksAuth` with ``host`` and ``token``.

    Raises:
        RuntimeError: When neither env vars nor SDK auth can produce a token.
    """
    # 1. Explicit environment variables
    env_host = os.environ.get("DATABRICKS_HOST", "")
    env_token = os.environ.get("DATABRICKS_TOKEN", "")
    if env_host and env_token:
        return DatabricksAuth(host=env_host.rstrip("/"), token=env_token)

    # 2. Databricks SDK unified auth
    profile = profile or os.environ.get("DATABRICKS_PROFILE")
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.config import Config
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sdk is not installed. Install with: uv sync --extra databricks"
        ) from exc

    cfg = Config(profile=profile) if profile else Config()
    client = WorkspaceClient(config=cfg)

    host = cfg.host
    if not host:
        raise RuntimeError(
            f"Could not resolve Databricks host from profile '{profile or 'default'}'. "
            "Set DATABRICKS_HOST or configure a profile in ~/.databrickscfg"
        )

    # Get a fresh token via the SDK's auth provider
    headers = cfg.authenticate()
    auth_header = headers.get("Authorization", "")

    if auth_header.startswith("Bearer "):
        token = auth_header[len("Bearer ") :]
    else:
        raise RuntimeError(
            f"Could not obtain token from profile '{profile or 'default'}'. "
            "Run: databricks auth login --profile <name>"
        )

    # Verify connectivity
    try:
        me = client.current_user.me()
        logger.info("Authenticated as %s on %s", me.user_name, host)
    except Exception as e:
        logger.warning("Connectivity check failed: %s", e)

    return DatabricksAuth(host=host.rstrip("/"), token=token)
