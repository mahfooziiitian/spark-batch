Compacting small files using Azure Databricks Spark and Synapse Spark. Spark provides a feature called bin packing, via its specialized storage layer called Delta Lake.

# Writing data into delta files

    df.write.mode("overwrite")
        .format("delta")
        .save("abfss://path/to/delta/files")

# Load data from the delta file, as shown here:
    
    val df: DataFrame = spark.read.format("delta")
        .load(abfss://path/to/delta/files)

# Create a table using delta, as shown here:
    Spark.sql("CREATE TABLE CUSTOMER USING 
        DELTA LOCATION "abfss://path/to/delta/files")


# Optimize delta table
    
    Spark.sql("OPTIMIZE delta.' abfss://path/to/delta/files'")

# Creating the tables using the following properties:

    delta.autoOptimize.optimizeWrite = true
    delta.autoOptimize.autoCompact = true


    CREATE TABLE Customer (
        id INT,
        name STRING,
        location STRING
    ) TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = true
    )

