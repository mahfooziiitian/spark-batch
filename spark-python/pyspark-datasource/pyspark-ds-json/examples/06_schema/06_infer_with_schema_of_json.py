"""Schema inference using schema_of_json().

Demonstrates Spark's built-in schema_of_json() function to automatically
derive a DDL schema string from a sample JSON value. Useful during development
to bootstrap schemas before hardcoding them for production.

Key concepts:
    - schema_of_json() returns a DDL string from a JSON literal
    - Useful for exploratory analysis and schema bootstrapping
    - The inferred schema can be used with spark.read.schema()

Caveats:
    - Only infers from a single sample — may miss optional fields
    - Numeric types default to BIGINT/DOUBLE (not INT/FLOAT)
    - For production, always define schemas explicitly

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.schema_of_json.html
"""

from pyspark.sql import functions as F

from pys_json import get_spark, set_log_level
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.infer_schema")


if __name__ == "__main__":
    spark = get_spark("infer-schema")

    sample_json = '{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}'
    logger.info("Sample JSON: %s", sample_json)

    # Create a DataFrame with the JSON string column
    df = spark.createDataFrame([(1, sample_json)], ["id", "value"])
    df.printSchema()
    df.show(truncate=False)

    # Infer schema using schema_of_json
    inferred_schema = spark.range(1).select(F.schema_of_json(F.lit(sample_json))).collect()[0][0]
    logger.info("Inferred DDL schema: %s", inferred_schema)

    # Parse the JSON column using the inferred schema
    parsed_df = df.withColumn("parsed", F.from_json(F.col("value"), inferred_schema))
    parsed_df.printSchema()
    parsed_df.show(truncate=False)

    spark.stop()
