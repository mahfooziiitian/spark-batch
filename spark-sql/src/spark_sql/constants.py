"""Repo-wide constants that never vary by environment or deployment target.

Unlike the environment variables layered from ``.env``/``.env.<target>`` (see
``dashboard.util.cli_env.load_env_layers``), values here are fixed for every
workspace and clone of this repository, so they're defined once as plain
constants instead of being read from the environment.
"""

# Local directory (relative to the repo root / current working directory) scanned for
# *.lvdash.json definition files by the REST/SDK deploy and list CLI scripts.
DASHBOARD_LOCATION = "./definitions"

# Fallback value for the DATABRICKS_DASHBOARD_PARENT_PATH env var (workspace destination
# folder) when it isn't set. Defined once here so every REST/SDK deploy/list CLI script
# falls back to the exact same default instead of each hardcoding its own copy.
DEFAULT_DASHBOARD_PARENT_PATH = "/Workspace/Shared/dashboards"
