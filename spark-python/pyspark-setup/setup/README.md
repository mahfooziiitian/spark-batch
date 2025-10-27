# PySpark Installation Guide

## Python Version Requirements

PySpark requires **Python 3.7 or higher**.

## Installation Methods

### 1. Using PyPI (Recommended)

#### Basic Installation

```bash
pip install pyspark
```

#### With Specific Hadoop Version

```bash
# For Hadoop 2.7
PYSPARK_HADOOP_VERSION=2 pip install pyspark

# With verbose output for tracking
PYSPARK_HADOOP_VERSION=2 pip install pyspark -v
```

#### Using Custom Mirror

```bash
PYSPARK_RELEASE_MIRROR=http://mirror.apache-kr.org PYSPARK_HADOOP_VERSION=2 pip install pyspark
```

**PYSPARK_HADOOP_VERSION Options:**

- `without`: Spark pre-built with user-provided Apache Hadoop
- `2`: Spark pre-built for Apache Hadoop 2.7
- `3`: Spark pre-built for Apache Hadoop 3.3 and later *(default)*

### 2. Using Conda

```bash
conda create -n pyspark_env
conda activate pyspark_env
conda install -c conda-forge pyspark
```

### 3. Manual Installation

#### Prerequisites

1. **Install Java 8 or higher**
      - Set `JAVA_HOME` environment variable
      - Add Java to `PATH`

#### Environment Setup

**Linux/macOS:**

```bash
export SPARK_HOME=/path/to/spark-3.0.0-bin-hadoop2.7
export HADOOP_HOME=$SPARK_HOME
export PATH=$PATH:$SPARK_HOME/bin
export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
```

**Windows:**

```cmd
set SPARK_HOME=C:\apps\spark-3.0.0-bin-hadoop2.7
set HADOOP_HOME=C:\apps\spark-3.0.0-bin-hadoop2.7
set PATH=%PATH%;C:\apps\spark-3.0.0-bin-hadoop2.7\bin
```

#### Windows-Specific Requirements

- Install `winutils.exe` in `%SPARK_HOME%\bin`

## Dependencies

| Package  | Minimum Version | Requirement Level | Purpose |
|----------|----------------|-------------------|---------|
| py4j     | 0.10.9.5       | **Required**      | Java-Python bridge |
| pandas   | 1.0.5          | Optional          | Spark SQL operations |
| pandas   | 1.0.5          | **Required**      | pandas API on Spark |
| pyarrow  | 1.0.0          | Optional          | Spark SQL operations |
| pyarrow  | 1.0.0          | **Required**      | pandas API on Spark |
| numpy    | 1.15           | **Required**      | pandas API on Spark and MLLib |

## Verification

After installation, verify PySpark is working:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
       .appName("Test") \
       .getOrCreate()

print(f"Spark version: {spark.version}")
spark.stop()
```
