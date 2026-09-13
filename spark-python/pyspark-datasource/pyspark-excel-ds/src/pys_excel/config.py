"""Project configuration — environment setup, paths, and SparkSession factory.

This module centralizes common boilerplate used across all examples:
- .env file loading (python-dotenv) for local environment overrides
- Java environment configuration
- DATA_HOME path resolution
- SparkSession creation with sensible defaults (Hive support for table demos)
- Sample Excel workbook generation utilities
"""

import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from pys_excel.logs import get_logger

logger = get_logger("config")


def _find_project_root() -> Path:
    """Walk up from this file to find the project root (contains pyproject.toml)."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    return Path.cwd()


# Resolve project root and load its .env file (if present) once at import time,
# before any environment-derived constants (DATA_HOME, etc.) are read. Existing
# process environment variables always take precedence over .env values.
PROJECT_ROOT: Path = _find_project_root()
_dotenv_path = PROJECT_ROOT / ".env"
if _dotenv_path.exists():
    load_dotenv(dotenv_path=_dotenv_path)
    logger.debug("Loaded environment overrides from %s", _dotenv_path)

DATA_HOME: str = os.environ.get("DATA_HOME", str(PROJECT_ROOT / "data"))


def _is_valid_java_home(path: str) -> bool:
    """Check whether ``path`` looks like a real JDK/JRE home (has bin/java)."""
    return bool(path) and (Path(path) / "bin" / "java").exists()


def _resolve_java_home() -> tuple[str, str]:
    """Resolve JAVA_HOME using a priority order, validating each candidate.

    Priority:
        1. An explicit ``JAVA_HOME`` already in the environment (e.g. set via
           ``.env`` or the shell) — never silently overridden.
        2. ``JAVA_HOME_17`` — common on machines managing multiple JDKs via
           SDKMAN/jenv, and matches this project's targeted Java 11 LTS baseline.
        3. Auto-detected by resolving ``java`` on PATH back to its home directory.

    Returns:
        A ``(path, source)`` tuple. ``path`` is ``""`` if nothing could be
        resolved; ``source`` describes where the value came from (for logging).
    """
    candidates = [
        (os.environ.get("JAVA_HOME", ""), "JAVA_HOME"),
        (os.environ.get("JAVA_HOME_17", ""), "JAVA_HOME_17"),
    ]
    java_bin = shutil.which("java")
    if java_bin:
        candidates.append((str(Path(java_bin).resolve().parent.parent), "PATH (java binary)"))

    for path, source in candidates:
        if _is_valid_java_home(path):
            return path, source

    # Nothing validated — fall back to the first non-empty candidate (if any)
    # so downstream tools at least see the user's original intent in errors.
    for path, source in candidates:
        if path:
            return path, source
    return "", "unresolved"


def configure_env() -> None:
    """Set up Java and Python environment variables for PySpark.

    Resolves ``JAVA_HOME`` via :func:`_resolve_java_home` (explicit
    ``JAVA_HOME`` > ``JAVA_HOME_17`` > auto-detected from ``java`` on PATH),
    validating that the resolved directory contains a ``bin/java``
    executable. Logs a warning (without raising) when no valid JDK can be
    found, since Spark may still locate a JVM on its own. Also sets
    ``PYSPARK_PYTHON`` to the current interpreter. Safe to call multiple times.
    """
    resolved, source = _resolve_java_home()
    if resolved and _is_valid_java_home(resolved):
        os.environ["JAVA_HOME"] = resolved
        logger.debug("JAVA_HOME set to %s (source=%s)", resolved, source)
    elif resolved:
        os.environ["JAVA_HOME"] = resolved
        logger.warning(
            "JAVA_HOME=%s (source=%s) has no bin/java executable; PySpark may fail to start.", resolved, source
        )
    else:
        logger.warning(
            "Could not resolve JAVA_HOME (checked JAVA_HOME, JAVA_HOME_17, and PATH); PySpark may fail to start."
        )
    os.environ["PYSPARK_PYTHON"] = sys.executable


def get_spark(
    app_name: str = "pys-excel-example",
    log_level: str = "WARN",
    enable_hive_support: bool = True,
    enable_delta: bool = False,
    configs: dict[str, str] | None = None,
) -> SparkSession:
    """Create a SparkSession configured for local Excel examples.

    Calls configure_env() automatically before creating the session. Hive
    support is enabled by default so that examples can create managed tables
    (`saveAsTable`) backed by a local `spark-warehouse/` directory, with the
    Derby `metastore_db/` also placed under `DATA_HOME` (instead of the
    process's current working directory).

    Args:
        app_name: Application name for Spark UI.
        log_level: Log level for SparkContext (WARN, ERROR, INFO).
        enable_hive_support: Enable the Hive metastore/catalog for table demos.
        enable_delta: Configure Delta Lake support (requires the optional
            ``delta-spark`` dependency — install with ``uv sync --extra delta``).
            Needed for :func:`pys_excel.table.upsert_table_from_excel` (MERGE INTO).
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
        .config("spark.sql.warehouse.dir", str(Path(DATA_HOME) / "spark-warehouse"))
    )
    if enable_hive_support:
        metastore_db_path = Path(DATA_HOME) / "metastore_db"
        builder = builder.enableHiveSupport().config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_db_path};create=true",
        )
        logger.debug("Hive metastore_db path: %s", metastore_db_path)
    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)
        logger.debug("Extra configs applied: %s", configs)

    if enable_delta:
        try:
            from delta import configure_spark_with_delta_pip

            builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            builder = configure_spark_with_delta_pip(builder)
        except ImportError:
            logger.warning(
                "enable_delta=True but 'delta-spark' is not installed; "
                "install with `uv sync --extra delta`. Falling back to non-Delta tables."
            )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    logger.info("SparkSession ready (version=%s)", spark.version)
    return spark


def get_spark_connect(
    app_name: str = "pys-excel-connect",
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
        >>> data_path("file_data", "excel", "employees.xlsx")
        '/home/user/.../data/file_data/excel/employees.xlsx'
    """
    return str(Path(DATA_HOME).joinpath(*parts))


def output_path(*parts: str) -> str:
    """Build an absolute path under DATA_HOME/file_data/excel/output (for write operations).

    Creates the parent directory (not the final path itself) if it doesn't
    exist, so this works equally well for file targets (e.g. ``report.xlsx``)
    and directory targets (e.g. a partitioned table write location).

    Args:
        *parts: Path segments to join after DATA_HOME/file_data/excel/output.

    Returns:
        Absolute path string.
    """
    base = Path(DATA_HOME) / "file_data" / "excel" / "output"
    if not parts:
        base.mkdir(parents=True, exist_ok=True)
        return str(base)
    p = base / Path(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def temp_excel_path(name: str = "temp") -> str:
    """Generate a temporary Excel file path under the system temp directory.

    Args:
        name: Base name for the temp file.

    Returns:
        Path string like /tmp/pys_excel/<name>.xlsx.
    """
    import tempfile

    p = Path(tempfile.gettempdir()) / "pys_excel" / f"{name}.xlsx"
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def generate_sample_workbook(path: str | None = None) -> str:
    """Generate a sample multi-sheet Excel workbook for examples/tests.

    Creates two sheets — "Employees" and "Departments" — with a handful of
    rows each, including a header row and one blank row above the header to
    exercise `skiprows`/`header` options.

    Args:
        path: Destination .xlsx path. Defaults to DATA_HOME/file_data/excel/employees.xlsx.

    Returns:
        The path written to.
    """
    import pandas as pd

    resolved = path or data_path("file_data", "excel", "employees.xlsx")
    p = Path(resolved)
    p.parent.mkdir(parents=True, exist_ok=True)

    employees = pd.DataFrame(
        {
            "emp_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "David", "Erin"],
            "department": ["Engineering", "Sales", "Engineering", "Marketing", "Sales"],
            "salary": [95000.0, 72000.0, 88000.0, 68000.0, 75000.0],
            "hire_date": pd.to_datetime(["2019-03-01", "2020-07-15", "2018-11-20", "2021-01-10", "2022-05-05"]),
        }
    )
    departments = pd.DataFrame(
        {
            "department": ["Engineering", "Sales", "Marketing"],
            "manager": ["Frank", "Grace", "Heidi"],
            "budget": [500000, 250000, 150000],
        }
    )

    with pd.ExcelWriter(resolved, engine="openpyxl") as writer:
        employees.to_excel(writer, sheet_name="Employees", index=False)
        departments.to_excel(writer, sheet_name="Departments", index=False)

    logger.debug("Generated sample workbook at %s", resolved)
    return resolved
