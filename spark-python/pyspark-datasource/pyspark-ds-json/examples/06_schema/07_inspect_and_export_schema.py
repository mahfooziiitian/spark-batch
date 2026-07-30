"""Print DataFrame schema as JSON.

Demonstrates inspecting and exporting schema information in multiple formats:
DDL simpleString, JSON representation, and tree printSchema(). Useful for
debugging schema mismatches and documenting data contracts.

Key concepts:
    - df.schema.simpleString() → compact DDL representation
    - df.schema.json() → full JSON schema (portable, machine-readable)
    - df.printSchema() → human-readable tree view

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html
"""

from pys_json import get_spark, set_log_level
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.print_schema")


if __name__ == "__main__":
    spark = get_spark("print-schema-json")

    columns = ["language", "fee"]
    data = [("Java", 20000), ("Python", 10000), ("Scala", 10000)]
    df = spark.createDataFrame(data).toDF(*columns)

    # Tree view
    logger.info("Schema tree view:")
    df.printSchema()

    # DDL string (compact)
    ddl_string = df.schema.simpleString()
    logger.info("DDL simpleString: %s", ddl_string)

    # JSON representation (portable)
    json_schema = df.schema.json()
    logger.info("JSON schema:\n%s", json_schema)

    # Field-level inspection
    for field in df.schema.fields:
        logger.debug(
            "Field: name=%s type=%s nullable=%s",
            field.name,
            field.dataType.simpleString(),
            field.nullable,
        )

    spark.stop()
