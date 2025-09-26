# Set up Apache Spark with Delta Lake

1. **Run interactively:** 

    Start the Spark shell (Scala or Python) with Delta Lake and run the code snippets interactively in the shell.

2. **Run as a project:**
    
    Set up a Maven or SBT project (Scala or Java) with Delta Lake, copy the code snippets into a source file, and run the project.
    
    Alternatively, you can use the examples provided in the Github repository.

3. **Prerequisite: set up Java**

    # Install Java 8 or Java 11 (recommended versions for Spark)
    # For Ubuntu/Debian:
    sudo apt-get install openjdk-11-jdk
    
    # For CentOS/RHEL:
    sudo yum install java-11-openjdk-devel
    
    # Set JAVA_HOME
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
    export PATH=$JAVA_HOME/bin:$PATH

4. **Set up interactive shell**

    # Download and extract Spark
    wget https://downloads.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
    tar xvf spark-3.4.1-bin-hadoop3.tgz
    cd spark-3.4.1-bin-hadoop3

5. **Spark SQL Shell**


    # Start Spark SQL shell with Delta Lake support
    bin/spark-sql \
        --packages io.delta:delta-core_2.12:2.3.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        --conf "spark.sql.warehouse.dir=delta_lake_warehouse" \
        --conf "spark.driver.memory=4g" \
        --conf "spark.executor.memory=4g"

6. **PySpark Shell**

    # Install PySpark with specific version
    pip install pyspark==3.4.1
    pip install delta-spark==2.3.0

    # Start PySpark shell with Delta Lake support
    pyspark \
        --packages io.delta:delta-core_2.12:2.3.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        --conf "spark.sql.warehouse.dir=delta_lake_warehouse" \
        --conf "spark.driver.memory=4g" \
        --conf "spark.executor.memory=4g"

7. **Spark Scala Shell**


    # Start Spark Scala shell with Delta Lake support
    bin/spark-shell \
        --packages io.delta:delta-core_2.12:2.3.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        --conf "spark.sql.warehouse.dir=delta_lake_warehouse" \
        --conf "spark.driver.memory=4g" \
        --conf "spark.executor.memory=4g"

8. **Python**

    # Install required packages
    pip install pyspark==3.4.1
    pip install delta-spark==2.3.0
    pip install pandas numpy

    # Initialize Spark with Delta Lake support
    import pyspark
    from delta import *
    
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "delta_lake_warehouse") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200")
    
    # Create Spark session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Enable Delta Lake logging
    spark.sparkContext.setLogLevel("INFO")

9. **Verify Installation**

    # Test Delta Lake functionality
    data = spark.range(0, 5)
    data.write.format("delta").save("./tmp/delta-table")
    
    # Read it back
    df = spark.read.format("delta").load("./tmp/delta-table")
    df.show() 

    