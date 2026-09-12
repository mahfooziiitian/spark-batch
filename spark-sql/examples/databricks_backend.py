"""Run and validate a statement on a Databricks SQL warehouse.

Uses the same runner/examiner/validator API as the local examples — only the
backend changes. Configuration is resolved the same way the rest of the repo does:

1. ``load_env_layers()`` loads ``.env`` then ``.env.<APP_ENV>`` (default ``dev``),
   matching the Makefile's layering — so values in ``.env.dev`` are picked up.
2. Auth + warehouse are read from the standard variables:

   * ``DATABRICKS_CONFIG_PROFILE`` (named profile in ``~/.databrickscfg``), **or**
   * ``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN``
   * ``DATABRICKS_WAREHOUSE_ID`` **or** ``DATABRICKS_WAREHOUSE_NAME``

Authentication itself is performed by
``spark_sql.util.cli_env.get_workspace_client`` inside ``DatabricksBackend``.

    uv run python examples/databricks_backend.py
"""

from __future__ import annotations

import os

from spark_sql.runner import DatabricksBackend, RunnerError, SqlRunner, expect, summarize
from spark_sql.util.cli_env import load_env_layers

QUERY = "SELECT 1 AS n, current_catalog() AS catalog"


def _mask(secret: str) -> str:
    return f"{'*' * max(len(secret) - 4, 0)}{secret[-4:]}" if secret else "<unset>"


def main() -> None:
    # Load .env + .env.<APP_ENV> (default "dev"), same layering as `make`.
    target = load_env_layers()

    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "").strip()
    host = os.environ.get("DATABRICKS_HOST", "").strip()
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
    warehouse_name = os.environ.get("DATABRICKS_WAREHOUSE_NAME", "").strip()

    print(f"target:         {target}")
    print(f"profile:        {profile or '<unset>'}")
    print(f"host:           {host or '<unset>'}")
    print(f"token:          {_mask(token)}")
    print(f"warehouse_id:   {warehouse_id or '<unset>'}")
    print(f"warehouse_name: {warehouse_name or '<unset>'}")

    has_auth = bool(profile or host)
    has_warehouse = bool(warehouse_id or warehouse_name)
    if not (has_auth and has_warehouse):
        print(
            "\nDatabricks not configured. Set DATABRICKS_CONFIG_PROFILE (or DATABRICKS_HOST/"
            "DATABRICKS_TOKEN) and DATABRICKS_WAREHOUSE_NAME/DATABRICKS_WAREHOUSE_ID in your "
            "environment or .env.<target>, then re-run.",
        )
        return

    try:
        # Explicit construction so the resolved profile/host/token/warehouse are visible.
        backend = DatabricksBackend.from_env(
            profile=profile or None,
            host=host or None,
            token=token or None,
            warehouse_id=warehouse_id or None,
            warehouse_name=warehouse_name or None,
        )
        runner = SqlRunner(backend)
        print(f"\nresolved warehouse_id: {backend.warehouse_id}")

        result = runner.run(QUERY)
        print("columns:", result.columns)
        print("rows:   ", result.rows)
        print("summary:", summarize(result))
        expect(result).non_empty()
        print("validation: OK")
    except RunnerError as exc:
        print("Databricks run failed:", exc)


if __name__ == "__main__":
    main()
