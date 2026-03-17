#!/usr/bin/env bash
# Bootstrap PySpark environment variables for a YARN edge node.
#
# Source this file — do NOT execute it directly:
#   source cluster/setup-yarn-env.sh

# ── Java ──────────────────────────────────────────────────────────────────────
if command -v java &>/dev/null; then
  export JAVA_HOME="${JAVA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which java)")")")}"
fi

# ── Spark ─────────────────────────────────────────────────────────────────────
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PATH="$SPARK_HOME/bin:$PATH"

# ── Python ────────────────────────────────────────────────────────────────────
# Driver and executors must use the same Python binary.
_PYTHON_BIN="$(command -v python3 2>/dev/null || command -v python)"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$_PYTHON_BIN}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$_PYTHON_BIN}"

# ── Hadoop / YARN ─────────────────────────────────────────────────────────────
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_CONF_DIR}"

# ── Optional tuning ───────────────────────────────────────────────────────────
export SPARK_LOG_DIR="${SPARK_LOG_DIR:-/var/log/spark}"
export SPARK_LOCAL_DIRS="${SPARK_LOCAL_DIRS:-/tmp/spark-local}"

echo "JAVA_HOME          = ${JAVA_HOME:-<not set>}"
echo "SPARK_HOME         = $SPARK_HOME"
echo "PYSPARK_PYTHON     = $PYSPARK_PYTHON"
echo "HADOOP_CONF_DIR    = $HADOOP_CONF_DIR"
echo "YARN_CONF_DIR      = $YARN_CONF_DIR"
