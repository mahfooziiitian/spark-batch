"""SparkSession helpers — local (PySpark) and remote (Databricks Connect).

Provides two entry points:

* ``create_spark_session()``      — local ``SparkSession`` (``local[*]`` default)
* ``create_dbconnect_session()``  — remote via Databricks Connect with automatic
  wheel upload and cluster-name lookup; falls back to local when unconfigured.

Both return a ``SparkSession``-compatible object.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Local SparkSession
# ---------------------------------------------------------------------------


def create_spark_session(app_name: str = "custom-ds") -> SparkSession:
    """Create (or fetch) a local SparkSession for examples and tests.

    Uses the ``SPARK_MASTER`` env var with a ``local[*]`` fallback so every
    script runs locally without modification.

    When running under Databricks Connect (where ``pyspark.sql.SparkSession``
    is not available), falls back to ``DatabricksSession``.
    """
    try:
        from pyspark.sql import SparkSession as _SparkSession

        spark = (
            _SparkSession.builder.appName(app_name)
            .master(os.environ.get("SPARK_MASTER", "local[*]"))
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except ImportError:
        from databricks.connect import DatabricksSession  # type: ignore[import-untyped]

        return DatabricksSession.builder.getOrCreate()


# ---------------------------------------------------------------------------
# Databricks Connect helpers
# ---------------------------------------------------------------------------


def _resolve_cluster_id(
    cluster_name: str,
    host: str = "",
    token: str = "",
    profile: str = "",
) -> str | None:
    """Look up a cluster ID by name using the Databricks SDK."""
    try:
        from databricks.sdk import WorkspaceClient

        if profile:
            client = WorkspaceClient(profile=profile)
        elif host and token:
            client = WorkspaceClient(host=host, token=token)
        else:
            client = WorkspaceClient()

        for cluster in client.clusters.list():
            if cluster.cluster_name == cluster_name:
                print(f"[dbconnect] Resolved cluster '{cluster_name}' → {cluster.cluster_id}")
                return cluster.cluster_id

        print(f"[dbconnect] Cluster '{cluster_name}' not found")
    except ImportError:
        print("[dbconnect] databricks-sdk not installed, cannot resolve cluster name")
    except Exception as e:
        print(f"[dbconnect] Cluster lookup failed: {e}")
    return None


def _find_or_build_wheel() -> Path | None:
    """Find an existing wheel in dist/ or build one with uv."""
    dist_dir = _PROJECT_ROOT / "dist"
    wheels = sorted(dist_dir.glob("pyspark_ds_custom-*.whl")) if dist_dir.exists() else []
    if wheels:
        return wheels[-1]

    print("[dbconnect] No wheel found — building with 'uv build'...")
    try:
        subprocess.run(
            ["uv", "build", "--wheel", "--quiet"],
            cwd=_PROJECT_ROOT,
            check=True,
        )
        wheels = sorted(dist_dir.glob("pyspark_ds_custom-*.whl"))
        if wheels:
            return wheels[-1]
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        print(f"[dbconnect] Wheel build failed: {e}")
    return None


def _upload_wheel(spark: SparkSession) -> None:
    """Upload the custom_ds wheel to the remote cluster via addArtifact().

    ``addArtifact(pyfile=True)`` only accepts ``.py`` and ``.zip`` files.
    Since wheels are zip-compatible archives we copy to a ``.zip`` extension
    in a temporary directory before uploading.
    """
    import shutil
    import tempfile

    wheel_path = _find_or_build_wheel()
    if wheel_path is None:
        print(
            "[dbconnect] WARNING: Could not find/build wheel — "
            "custom_ds must already be installed on the cluster"
        )
        return

    print(f"[dbconnect] Uploading wheel: {wheel_path.name}")
    tmp_dir = Path(tempfile.mkdtemp(prefix="dbconnect_"))
    zip_path = tmp_dir / wheel_path.with_suffix(".zip").name
    try:
        shutil.copy2(wheel_path, zip_path)
        spark.addArtifact(str(zip_path), pyfile=True)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def create_dbconnect_session(
    app_name: str = "dbconnect-custom-ds",
    *,
    upload_wheel: bool = True,
) -> SparkSession:
    """Create a remote SparkSession via Databricks Connect.

    Configuration via environment variables:
        DATABRICKS_HOST:         Workspace URL (e.g. https://xxx.cloud.databricks.com)
        DATABRICKS_CLUSTER_ID:   Target cluster ID (takes priority)
        DATABRICKS_CLUSTER_NAME: Cluster name to look up (used if ID not set)
        DATABRICKS_TOKEN:        Personal access token (or use profile-based auth)
        DATABRICKS_PROFILE:      ~/.databrickscfg profile name (alternative to token)

    Args:
        app_name: Spark application name.
        upload_wheel: When True (default), automatically uploads the custom_ds
            wheel to the remote cluster so datasource classes are available.

    Raises:
        RuntimeError: When cluster ID/name is not configured, databricks-connect
            is not installed, or the connection fails.
    """
    try:
        from databricks.connect import DatabricksSession
    except ImportError as exc:
        raise RuntimeError(
            "databricks-connect is not installed. Install with: uv sync --extra databricks"
        ) from exc

    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    profile = os.environ.get("DATABRICKS_PROFILE", "")

    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")

    # Resolve cluster name → ID if no explicit ID
    if not cluster_id:
        cluster_name = os.environ.get("DATABRICKS_CLUSTER_NAME", "")
        if cluster_name:
            cluster_id = _resolve_cluster_id(cluster_name, host, token, profile)

    if not cluster_id:
        raise RuntimeError(
            "No Databricks cluster configured. Set DATABRICKS_CLUSTER_ID or "
            "DATABRICKS_CLUSTER_NAME environment variable."
        )

    builder = DatabricksSession.builder

    if profile:
        builder = builder.profile(profile)
    elif host and token:
        builder = builder.host(host).token(token)

    builder = builder.clusterId(cluster_id)
    spark = builder.getOrCreate()
    print(f"[dbconnect] Connected to cluster: {cluster_id}")

    if upload_wheel:
        _upload_wheel(spark)

    return spark
