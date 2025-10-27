# PySpark Setup

This guide provides instructions for setting up and configuring PySpark for Apache Spark development.

## Prerequisites

- Python 3.7 or higher
- Java 8 or 11
- Apache Spark (optional, can use pip installation)

## Installation

### Option 1: Install via pip

```bash
pip install pyspark
```

### Option 2: Download Apache Spark

1. Download Spark from [official website](https://spark.apache.org/downloads.html)
2. Extract the archive
3. Set environment variables:

```bash
export SPARK_HOME=/path/to/spark
export PATH=$PATH:$SPARK_HOME/bin
```

## Quick Start

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()

# Your PySpark code here
df = spark.createDataFrame([(1, "hello"), (2, "world")], ["id", "text"])
df.show()

# Stop session
spark.stop()
```

## Configuration

Common configuration options can be set when creating the Spark session:

```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
```
