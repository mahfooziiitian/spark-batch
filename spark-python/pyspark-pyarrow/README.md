# Pyarrow

Apache Arrow is an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes.

This currently is most beneficial to Python users that work with Pandas/NumPy data.

Its usage is not automatic and might require some minor changes to configuration or code to take full advantage and ensure compatibility.

This guide will give a high-level description of how to use Arrow in Spark and highlight any differences when working with Arrow-enabled data.

## Enabling for Conversion to/from Pandas

Arrow is available as an optimization when converting a Spark DataFrame to a Pandas DataFrame using the call `DataFrame.toPandas()` and when creating a Spark DataFrame from a Pandas DataFrame with `SparkSession.createDataFrame()`.

To use Arrow when executing these calls, users need to first set the Spark configuration `spark.sql.execution.arrow.pyspark.enabled` to true.

This is disabled by default.
