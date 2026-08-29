"""Databricks authentication utilities.

Resolves a temporary access token and workspace host from a Databricks CLI
profile (``~/.databrickscfg``) or environment variables using the
``databricks-sdk`` unified auth.

Usage::

    from custom_ds.util.databricks_auth import get_databricks_auth, parse_profile_arg

    profile = parse_profile_arg()           # reads --profile from sys.argv
    auth = get_databricks_auth(profile)     # resolves host + token

    print(auth.host)    # https://<workspace>.cloud.databricks.com
    print(auth.token)   # temporary or PAT token

CLI usage::

    python script.py --profile dev
    # or via env var:
    DATABRICKS_PROFILE=dev python script.py
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabricksAuth:
    """Resolved Databricks host and token pair."""

    host: str
    token: str


def parse_profile_arg() -> str | None:
    """Parse ``--profile <name>`` from command-line arguments.

    Falls back to the ``DATABRICKS_PROFILE`` environment variable.

    Returns:
        Profile name, or ``None`` if not specified.
    """
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--profile" and i + 1 < len(args):
            return args[i + 1]
        if arg.startswith("--profile="):
            return arg.split("=", 1)[1]
    return os.environ.get("DATABRICKS_PROFILE")


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
    headers: dict[str, str] = {}
    cfg.authenticate(headers)
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
        print(f"[auth] Authenticated as {me.user_name} on {host}")
    except Exception as e:
        print(f"[auth] Warning: connectivity check failed: {e}")

    return DatabricksAuth(host=host.rstrip("/"), token=token)
