"""Project configuration — environment setup, paths, and SparkSession factory.

This module centralizes common boilerplate used across all examples:
- .env file loading (python-dotenv)
- Portable Java environment detection/configuration
- DATA_HOME path resolution
- SparkSession creation with sensible defaults
- Sample HTML file writing utilities
"""

import os
import re
import shutil
import subprocess  # nosec B404
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

from pys_html._logging import get_logger

logger = get_logger("config")

MIN_JAVA_VERSION = 17


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

# Load variables from a .env file at the project root (if present) without
# overriding any already-exported shell environment variables.
load_dotenv(PROJECT_ROOT / ".env", override=False)

DATA_HOME: str = os.environ.get("DATA_HOME", str(PROJECT_ROOT / "data"))


def _detect_java_home() -> str | None:
    """Best-effort, cross-platform discovery of a usable JAVA_HOME.

    Resolution order (first match wins):
    1. ``JAVA_HOME`` already exported in the environment (or loaded from ``.env``).
    2. A ``java`` executable resolvable on ``PATH`` — JAVA_HOME is derived by resolving
       symlinks and stripping the trailing ``bin/java``.
    3. macOS's ``/usr/libexec/java_home`` helper, if present.

    Returns:
        Absolute path to a JDK home, or None if no Java installation could be located
        (in which case PySpark/JVM startup is left to fail with its own diagnostic).
    """
    existing = os.environ.get("JAVA_HOME")
    if existing:
        return existing

    java_bin = shutil.which("java")
    if java_bin:
        real_java = Path(java_bin).resolve()
        candidate = real_java.parent.parent  # strip the trailing "bin/java"
        if (candidate / "bin" / "java").exists():
            return str(candidate)

    if sys.platform == "darwin":
        java_home_helper = Path("/usr/libexec/java_home")
        if java_home_helper.exists():
            try:
                result = subprocess.run(  # nosec B603
                    [str(java_home_helper)],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                detected = result.stdout.strip()
                if detected:
                    return detected
            except (OSError, subprocess.CalledProcessError) as exc:
                logger.debug("java_home helper failed: %s", exc)

    return None


def _check_java_version(java_home: str) -> None:
    """Log a warning if the JDK at java_home is older than PySpark 4 requires.

    This is advisory only — it never raises, since some environments run Java
    versions unknown to this heuristic but that still work fine.
    """
    java_bin = Path(java_home) / "bin" / "java"
    if not java_bin.exists():
        return
    try:
        result = subprocess.run(  # nosec B603
            [str(java_bin), "-version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        logger.debug("Could not run 'java -version': %s", exc)
        return

    version_output = result.stderr or result.stdout
    match = re.search(r'version "(\d+)', version_output)
    if match and int(match.group(1)) < MIN_JAVA_VERSION:
        logger.warning(
            "Detected Java %s at %s, but PySpark 4 requires Java %d+. "
            "Set JAVA_HOME to a newer JDK if Spark fails to start.",
            match.group(1),
            java_home,
            MIN_JAVA_VERSION,
        )


def configure_env() -> None:
    """Set up Java and Python environment variables for PySpark 4, portably.

    Auto-detects a usable JAVA_HOME (see _detect_java_home()) rather than relying on
    any project-specific environment variable, then sets PYSPARK_PYTHON to the current
    interpreter. Safe to call multiple times.
    """
    java_home = _detect_java_home()
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        logger.debug("JAVA_HOME resolved to %s", java_home)
        _check_java_version(java_home)
    else:
        logger.warning("Could not detect a Java installation; ensure JAVA_HOME is set or 'java' is on PATH")
    os.environ["PYSPARK_PYTHON"] = sys.executable


def get_spark(
    app_name: str = "pys-html-example",
    log_level: str = "WARN",
    configs: dict[str, str] | None = None,
) -> SparkSession:
    """Create a SparkSession configured for local HTML examples.

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
    app_name: str = "pys-html-connect",
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
        >>> data_path("file_data", "html", "tables", "countries.html")
        '/home/user/.../data/file_data/html/tables/countries.html'
    """
    return str(Path(DATA_HOME).joinpath(*parts))


def output_path(*parts: str) -> str:
    """Build an absolute path under DATA_HOME/file_data/html/output (for write operations).

    Creates the directory if it doesn't exist.

    Args:
        *parts: Path segments to join after DATA_HOME/file_data/html/output.

    Returns:
        Absolute path string.
    """
    base = Path(DATA_HOME) / "file_data" / "html" / "output"
    p = base / Path(*parts) if parts else base
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def write_html_file(path: str, content: str) -> str:
    """Write raw HTML content to a file, creating parent directories as needed.

    Args:
        path: Destination file path.
        content: HTML markup to write.

    Returns:
        The path written to (for chaining).
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)
    logger.debug("Wrote HTML file to %s (%d bytes)", path, len(content))
    return path


def temp_html_path(name: str = "temp") -> str:
    """Generate a temporary HTML file path under /tmp.

    Args:
        name: Base name for the temp file.

    Returns:
        Path string like /tmp/pys_html/<name>.html.
    """
    import tempfile

    p = Path(tempfile.gettempdir()) / "pys_html" / f"{name}.html"
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)
