# Spark local
1. Simple Local Mode Script
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
2. Different Local Master URLs (Core Allocation)
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
### Complete Local Mode Example

#### Shell Script
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
#### Python Script (spark_local_demo.py)
```python
#!/usr/bin/env python3
# spark_local_demo.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

def create_sample_data(spark):
    """Create sample data for demonstration"""
    
    # Sample data - employee records
    data = [
        ("Alice", "Engineering", "San Francisco", 75000, 2020),
        ("Bob", "Marketing", "New York", 65000, 2019),
        ("Charlie", "Engineering", "San Francisco", 80000, 2018),
        ("Diana", "Sales", "Chicago", 55000, 2021),
        ("Eve", "Engineering", "New York", 90000, 2017),
        ("Frank", "Marketing", "Chicago", 60000, 2020),
        ("Grace", "Sales", "San Francisco", 70000, 2019),
        ("Henry", "Engineering", "Chicago", 85000, 2018)
    ]
    
    schema = ["name", "department", "city", "salary", "join_year"]
    
    return spark.createDataFrame(data, schema)

def demonstrate_operations(df):
    """Demonstrate various Spark operations"""
    
    print("=== Original Data ===")
    df.show()
    
    print("=== Schema ===")
    df.printSchema()
    
    print("=== Basic Aggregations ===")
    # Department-wise statistics
    dept_stats = df.groupBy("department").agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        min("salary").alias("min_salary"),
        max("salary").alias("max_salary")
    )
    dept_stats.show()
    
    print("=== Filtering and Sorting ===")
    high_earners = df.filter(df.salary > 70000).orderBy(desc("salary"))
    high_earners.show()
    
    print("=== City-wise Engineering Salaries ===")
    city_eng = df.filter(df.department == "Engineering") \
                .groupBy("city") \
                .agg(avg("salary").alias("avg_engineering_salary"))
    city_eng.show()
    
    return dept_stats

def spark_config_demo(spark):
    """Show Spark configuration"""
    print("=== Spark Configuration ===")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {spark.conf.get('spark.master')}")
    print(f"App Name: {spark.conf.get('spark.app.name')}")
    
    # Show available cores
    sc = spark.sparkContext
    print(f"Available Cores: {sc.defaultParallelism}")

def main():
    """Main function"""
    start_time = time.time()
    
    # Initialize Spark Session with local configuration
    spark = SparkSession.builder \
        .appName("LocalSparkDemo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.ui.port", "4041") \
        .getOrCreate()
    
    try:
        # Set log level to WARN to reduce verbose output
        spark.sparkContext.setLogLevel("WARN")
        
        # Show Spark configuration
        spark_config_demo(spark)
        
        # Create and process data
        df = create_sample_data(spark)
        
        # Perform operations
        result_df = demonstrate_operations(df)
        
        # Show execution time
        execution_time = time.time() - start_time
        print(f"=== Execution Summary ===")
        print(f"Total execution time: {execution_time:.2f} seconds")
        print(f"Data processed: {df.count()} records")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        raise
        
    finally:
        # Always stop Spark session
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__": # Entry point for the script
 main()
Advanced Local Mode Scripts
3. Local Mode with External Data
bash
#!/bin/bash
# spark_local_file_processing.sh

SPARK_HOME=/opt/spark
INPUT_FILE=$1
OUTPUT_DIR=${2:-"/tmp/spark_output"}

$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --driver-memory 2g \
    --conf "spark.sql.warehouse.dir=/tmp/spark-warehouse" \
    --name "FileProcessor" \
    spark_file_processor.py $INPUT_FILE $OUTPUT_DIR
```
4. Local Mode with Dependencies (Python files, archives, and configuration)
```bash
#!/bin/bash
# spark_local_dependencies.sh

SPARK_HOME=/opt/spark

# Include additional Python files or packages
$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --py-files dependencies.zip \
    --files config.json \
    --name "JobWithDeps" \
    main_spark_job.py
```
5. Interactive Local Development (PySpark Shell)
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
    --conf "spark.sql.adaptive.enabled=true"
```
### Performance Tuning for Local Mode
```bash
#!/bin/bash
# spark_local_performance.sh

SPARK_HOME=/opt/spark

$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --driver-memory 4g \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB" \
    --conf "spark.sql.sources.partitionOverwriteMode=dynamic" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.memory.fraction=0.8" \
    --conf "spark.memory.storageFraction=0.3" \
    --name "TunedLocalJob" \
    optimized_spark_job.py
```
### Quick Test Script
```bash
#!/bin/bash
# quick_spark_test.sh

# Quick test to verify Spark local mode is working
/opt/spark/bin/spark-submit --master local[2] << 'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QuickTest") \
    .getOrCreate()

# Simple test
df = spark.range(1, 11)
print("Testing Spark Local Mode:")
print(f"Spark Version: {spark.version}")
print(f"Number of records: {df.count()}")
df.show()

spark.stop()
print("✓ Spark local mode is working correctly!")
EOF
Key Local Mode Configuration Options
Option	Description	Example
local[*]	Use all available cores	--master local[*]
local[4]	Use 4 cores	--master local[4]
local[K]	Use K cores	--master local[2]
local	Use 1 core	--master local
local[N, M]	Use N cores, M max failures	--master local[4,2]
Usage Examples
bash
# Make scripts executable
chmod +x *.sh

# Run basic local mode
./spark_local_basic.sh

# Run with specific core count
./run_spark_local_complete.sh

# Quick test
./quick_spark_test.sh
```