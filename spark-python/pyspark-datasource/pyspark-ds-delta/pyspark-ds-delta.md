# Set up Apache Spark with Delta Lake

1. **Run interactively:** 

    Start the Spark shell (Scala or Python) with Delta Lake and run the code snippets interactively in the shell.

2. **Run as a project:**
    
    Set up a Maven or SBT project (Scala or Java) with Delta Lake, copy the code snippets into a source file, and run the project.
    
    Alternatively, you can use the examples provided in the Github repository.

3. **Prerequisite: set up Java**

    a

4. **Set up interactive shell**

5. **Spark SQL Shell**


    bin/spark-sql \
        --packages io.delta:delta-core_2.12:2.3.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

6. **PySpark Shell**


    pip install pyspark==<compatible-spark-version>
    pyspark \
        --packages io.delta:delta-core_2.12:2.3.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

7. **Spark Scala Shell**


    bin/spark-shell \
        --packages io.delta:delta-core_2.12:2.3.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

8. **Python**

   pip install delta-spark==2.3.0

    import pyspark
    from delta import *
    
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

10. 

