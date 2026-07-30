"""Project configuration — environment setup, paths, and SparkSession factory.

This module centralizes common boilerplate used across all examples:
- Java 17 environment configuration
- DATA_HOME path resolution
- SparkSession creation with sensible defaults
- Sample data file writing utilities
"""

import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from pys_json._logging import get_logger

logger = get_logger("config")


def _find_project_root() -> Path:
    """Walk up from this file to find the project root (contains pyproject.toml)."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path.cwd()


# Resolve project root and data home once at import time
PROJECT_ROOT: Path = _find_project_root()
DATA_HOME: str = os.environ.get("DATA_HOME", str(PROJECT_ROOT / "data"))


def configure_env() -> None:
    """Set up Java and Python environment variables for PySpark 4.

    Sets JAVA_HOME to JAVA_HOME_17 and PYSPARK_PYTHON to current interpreter.
    Safe to call multiple times.
    """
    if "JAVA_HOME_17" in os.environ:
        os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
        logger.debug("JAVA_HOME set to JAVA_HOME_17: %s", os.environ["JAVA_HOME"])
    else:
        logger.debug("JAVA_HOME_17 not found; using existing JAVA_HOME=%s", os.environ.get("JAVA_HOME", "<unset>"))
    os.environ["PYSPARK_PYTHON"] = sys.executable


def get_spark(
    app_name: str = "pys-json-example",
    log_level: str = "WARN",
    configs: dict[str, str] | None = None,
) -> SparkSession:
    """Create a SparkSession configured for local JSON examples.

    Calls configure_env() automatically before creating the session.

    Args:
        app_name: Application name for Spark UI.
        log_level: Log level for SparkContext (WARN, ERROR, INFO).
        configs: Additional Spark configuration key-value pairs.

    Returns:
        Configured SparkSession instance.
    """
    configure_env()
    master = os.environ.get("SPARK_MASTER", "local[*]")
    logger.info("Creating SparkSession app_name=%r master=%s", app_name, master)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)
        logger.debug("Extra configs applied: %s", configs)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    logger.info("SparkSession ready (version=%s)", spark.version)
    return spark


def get_spark_connect(
    app_name: str = "pys-json-connect",
    url: str | None = None,
) -> SparkSession:
    """Create a SparkSession via Spark Connect (remote cluster / Databricks).

    Args:
        app_name: Application name.
        url: Spark Connect URL. Defaults to SPARK_CONNECT_URL env var or sc://localhost:15002.

    Returns:
        Remote SparkSession instance.
    """
    configure_env()
    connect_url = url or os.environ.get("SPARK_CONNECT_URL", "sc://localhost:15002")
    logger.info("Creating Spark Connect session app_name=%r url=%s", app_name, connect_url)
    return SparkSession.builder.appName(app_name).remote(connect_url).getOrCreate()


def data_path(*parts: str) -> str:
    """Build an absolute path under DATA_HOME.

    Args:
        *parts: Path segments to join after DATA_HOME.

    Returns:
        Absolute path string.

    Example:
        >>> data_path("FileData", "Json", "properties", "comment", "comment.json")
        '/home/user/.../data/FileData/Json/properties/comment/comment.json'
    """
    return str(Path(DATA_HOME).joinpath(*parts))


def output_path(*parts: str) -> str:
    """Build an absolute path under DATA_HOME/output (for write operations).

    Creates the directory if it doesn't exist.

    Args:
        *parts: Path segments to join after DATA_HOME/output.

    Returns:
        Absolute path string.
    """
    p = Path(DATA_HOME) / "output" / Path(*parts) if parts else Path(DATA_HOME) / "output"
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def write_json_lines(path: str, lines: list[str]) -> str:
    """Write JSON lines to a file, creating parent directories as needed.

    Args:
        path: Destination file path.
        lines: List of JSON strings (one per line).

    Returns:
        The path written to (for chaining).
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines) + "\n")
    logger.debug("Wrote %d JSON lines to %s", len(lines), path)
    return path


def write_json_file(path: str, data: list[dict] | dict, multiline: bool = False) -> str:
    """Write Python dicts as a JSON file.

    Args:
        path: Destination file path.
        data: List of dicts (JSON Lines) or a single dict/list (multiline JSON).
        multiline: If True, write as pretty-printed JSON. If False, write one dict per line.

    Returns:
        The path written to.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if multiline:
        p.write_text(json.dumps(data, indent=2) + "\n")
    else:
        if isinstance(data, dict):
            data = [data]
        p.write_text("\n".join(json.dumps(record) for record in data) + "\n")

    logger.debug("Wrote JSON file to %s (multiline=%s)", path, multiline)
    return path


def temp_json_path(name: str = "temp") -> str:
    """Generate a temporary JSON file path under /tmp.

    Args:
        name: Base name for the temp file/directory.

    Returns:
        Path string like /tmp/pys_json/<name>.json.
    """
    import tempfile

    p = Path(tempfile.gettempdir()) / "pys_json" / f"{name}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)
