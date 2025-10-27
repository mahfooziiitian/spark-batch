# Spark Local

## 1. Simple Local Mode Script

```bash
#!/bin/bash
# spark_local_basic.sh

export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3

# Basic local mode - uses all available cores
$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --name "LocalSparkJob" \
    pyspark_script.py
```

## 2. Different Local Master URLs (Core Allocation)

```bash
#!/bin/bash
# spark_local_variations.sh

SPARK_HOME=/opt/spark

echo "=== Local mode with all cores ==="
$SPARK_HOME/bin/spark-submit --master local[*] pyspark_script.py

echo "=== Local mode with 2 cores ==="
$SPARK_HOME/bin/spark-submit --master local[2] pyspark_script.py

echo "=== Local mode with 1 core ==="
$SPARK_HOME/bin/spark-submit --master local[1] pyspark_script.py

echo "=== Local mode with specific driver memory ==="
$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --driver-memory 2g \
    pyspark_script.py
```

## 3. Complete Local Mode Example

### Shell Script

```bash
#!/bin/bash
# run_spark_local_complete.sh

# Configuration
SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
PYTHON_SCRIPT="spark_local_demo.py"
MASTER="local[4]"
DRIVER_MEMORY="2g"

# Check if Spark home exists
if [ ! -d "$SPARK_HOME" ]; then
    echo "Error: SPARK_HOME not found at $SPARK_HOME"
    exit 1
fi

echo "Starting Spark in local mode..."
echo "Master: $MASTER"
echo "Driver Memory: $DRIVER_MEMORY"

# Run Spark job
$SPARK_HOME/bin/spark-submit \
    --master $MASTER \
    --driver-memory $DRIVER_MEMORY \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --name "LocalSparkDemo" \
    $PYTHON_SCRIPT

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "Spark job completed successfully!"
else
    echo "Spark job failed with exit code: $EXIT_CODE"
    exit $EXIT_CODE
fi
```

### Python Script (spark_local_demo.py)

```python
#!/usr/bin/env python3
# spark_local_demo.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max
import sys
import time

def create_spark_session():
    """Create and configure Spark session for local mode"""
    return SparkSession.builder \
        .appName("LocalSparkDemo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def demonstrate_spark_operations(spark):
    """Demonstrate common Spark operations in local mode"""
    print("=== Creating sample data ===")
    
    # Create sample DataFrame
    data = [(i, f"name_{i}", i * 10.5, i % 3) for i in range(1, 1001)]
    columns = ["id", "name", "value", "category"]
    df = spark.createDataFrame(data, columns)
    
    print(f"Created DataFrame with {df.count()} rows")
    print("Schema:")
    df.printSchema()
    
    print("\n=== Basic operations ===")
    df.show(10)
    
    print("\n=== Aggregations ===")
    aggregated = df.groupBy("category") \
        .agg(count("*").alias("count"),
             avg("value").alias("avg_value"),
             spark_max("value").alias("max_value"))
    aggregated.show()
    
    print("\n=== Filtering and sorting ===")
    filtered = df.filter(col("value") > 500) \
        .orderBy(col("value").desc())
    filtered.show(10)
    
    return df

def main():
    print("Starting Spark Local Mode Demo")
    start_time = time.time()
    
    spark = create_spark_session()
    
    try:
        # Print Spark configuration
        print(f"Spark Version: {spark.version}")
        print(f"Master: {spark.sparkContext.master}")
        print(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")
        
        # Demonstrate operations
        df = demonstrate_spark_operations(spark)
        
        # Optional: Save results
        output_path = "/tmp/spark_local_demo_output"
        print(f"\n=== Saving results to {output_path} ===")
        df.coalesce(1).write.mode("overwrite").json(output_path)
        print("Results saved successfully")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        
    end_time = time.time()
    print(f"\nDemo completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
```

## 4. Advanced Local Mode Scripts

### Local Mode with External Data

```bash
#!/bin/bash
# spark_local_file_processing.sh

SPARK_HOME=/opt/spark
INPUT_FILE=$1
OUTPUT_DIR=${2:-"/tmp/spark_output"}

if [ -z "$INPUT_FILE" ]; then
    echo "Usage: $0 <input_file> [output_dir]"
    exit 1
fi

echo "Processing file: $INPUT_FILE"
echo "Output directory: $OUTPUT_DIR"

$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --driver-memory 2g \
    --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse" \
    --conf "spark.sql.adaptive.enabled=true" \
    --name "FileProcessor" \
    spark_file_processor.py "$INPUT_FILE" "$OUTPUT_DIR"
```

### Local Mode with Dependencies

```bash
#!/bin/bash
# spark_local_dependencies.sh

SPARK_HOME=/opt/spark

# Include additional Python files or packages
$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --driver-memory 2g \
    --py-files dependencies.zip,utils.py \
    --files config.json,data/lookup.csv \
    --jars external-lib.jar \
    --conf "spark.driver.extraClassPath=external-lib.jar" \
    --name "JobWithDeps" \
    main_spark_job.py
```

### Interactive Local Development (PySpark Shell)

```bash
#!/bin/bash
# start_pyspark_shell.sh

export SPARK_HOME=/opt/spark
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Start PySpark shell with local mode
$SPARK_HOME/bin/pyspark \
    --master local[4] \
    --driver-memory 2g \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0
```

## 5. Performance Tuning for Local Mode

```bash
#!/bin/bash
# spark_local_performance.sh

SPARK_HOME=/opt/spark

$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --driver-max-result-size 2g \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB" \
    --conf "spark.sql.adaptive.skewJoin.enabled=true" \
    --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.kryo.unsafe=true" \
    --conf "spark.memory.fraction=0.8" \
    --conf "spark.memory.storageFraction=0.3" \
    --conf "spark.sql.parquet.compression.codec=snappy" \
    --conf "spark.sql.execution.arrow.pyspark.enabled=true" \
    --name "TunedLocalJob" \
    optimized_spark_job.py
```

## 6. Monitoring and Debugging

### Local Mode with Enhanced Logging

```bash
#!/bin/bash
# spark_local_debug.sh

SPARK_HOME=/opt/spark
LOG_LEVEL=${LOG_LEVEL:-"INFO"}
LOG_DIR="/tmp/spark-logs"

mkdir -p "$LOG_DIR"

$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --driver-memory 2g \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
    --conf "spark.eventLog.enabled=true" \
    --conf "spark.eventLog.dir=$LOG_DIR" \
    --conf "spark.history.fs.logDirectory=$LOG_DIR" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.logLevel=$LOG_LEVEL" \
    --name "DebuggingJob" \
    spark_debug_job.py

echo "Event logs saved to: $LOG_DIR"
echo "Access Spark UI at: http://localhost:4040"
```

### Quick Test Script

```bash
#!/bin/bash
# quick_spark_test.sh

# Quick test to verify Spark local mode is working
SPARK_HOME=${SPARK_HOME:-"/opt/spark"}

echo "Testing Spark Local Mode..."
echo "Spark Home: $SPARK_HOME"

$SPARK_HOME/bin/spark-submit --master local[2] spark_quick.py
```

## 7. Resource Management Scripts

### Memory Configuration Helper

```bash
bash scripts/spark_memory_config.sh
```

## Configuration Reference

### Key Local Mode Configuration Options

| Option | Description | Example |
|--------|-------------|---------|
| `local[*]` | Use all available cores | `--master local[*]` |
| `local[4]` | Use 4 cores | `--master local[4]` |
| `local[K]` | Use K cores | `--master local[2]` |
| `local` | Use 1 core | `--master local` |
| `local[N, M]` | Use N cores, M max failures | `--master local[4,2]` |

### Performance Configuration

| Configuration | Description | Recommended Value |
|---------------|-------------|-------------------|
| `spark.sql.adaptive.enabled` | Enable adaptive query execution | `true` |
| `spark.sql.adaptive.coalescePartitions.enabled` | Coalesce small partitions | `true` |
| `spark.memory.fraction` | Memory for execution/storage | `0.8` |
| `spark.memory.storageFraction` | Storage memory fraction | `0.3` |
| `spark.serializer` | Serialization library | `org.apache.spark.serializer.KryoSerializer` |

## Usage Examples

```bash
# Make scripts executable
chmod +x *.sh

# Run basic local mode
./spark_local_basic.sh

# Run with specific core count
./run_spark_local_complete.sh

# Quick test
./quick_spark_test.sh

# Performance tuned execution
./spark_local_performance.sh

# Debug mode with logging
./spark_local_debug.sh

# Check system resources
./spark_memory_config.sh
```

## Troubleshooting

### Common Issues and Solutions

1. **OutOfMemoryError**: Increase `--driver-memory` or reduce data size
2. **Port conflicts**: Change Spark UI port with `--conf "spark.ui.port=4041"`
3. **Python environment**: Set `PYSPARK_PYTHON` environment variable
4. **Java version**: Ensure Java 8 or 11 is installed and `JAVA_HOME` is set

### Environment Setup

```bash
# ~/.bashrc or ~/.zshrc
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PYSPARK_PYTHON=python3
export PATH=$SPARK_HOME/bin:$PATH
```
