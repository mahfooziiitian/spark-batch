"""Shared Spark environment setup — import before creating a SparkSession.

Detects Java 11 or 17 via SDKMAN / JAVA_HOME and sets JVM flags needed
for Arrow on Java 17+. Must be imported before any PySpark module
triggers JVM startup.
"""

import os
from pathlib import Path

_SDKMAN_JAVA_BASE = Path.home() / ".sdkman" / "candidates" / "java"

def _find_compatible_java() -> str | None:
    """Return the path to a Java 11 or 17 installation, if available."""
    if not _SDKMAN_JAVA_BASE.is_dir():
        return None
    for candidate in sorted(_SDKMAN_JAVA_BASE.iterdir()):
        if candidate.name.startswith(("11.", "17.")):
            java_bin = candidate / "bin" / "java"
            if java_bin.exists():
                return str(candidate)
    return None


# Point PySpark at Java 11/17 when available (Java 21 breaks Arrow memory)
if "JAVA_HOME" not in os.environ or "21" in os.environ.get("JAVA_HOME", ""):
    _java_home = _find_compatible_java()
    if _java_home:
        os.environ["JAVA_HOME"] = _java_home
        os.environ["PATH"] = f"{_java_home}/bin:{os.environ.get('PATH', '')}"

os.environ.setdefault("PYSPARK_PYTHON", "python3")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
