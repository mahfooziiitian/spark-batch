"""JSON schema string — applying schema via JSON representation.

Demonstrates defining a schema as a JSON string (exported from StructType.json())
and applying it to a JSON read operation. This pattern is useful when schemas are
stored in external configuration files or metadata catalogs.

Key concepts:
    - StructType.json() exports schema as a portable JSON string
    - JSON schema strings can be loaded from files/configs
    - Equivalent to programmatic StructType but serialization-friendly

Use cases:
    - Schema registry integration
    - Configuration-driven pipelines
    - Cross-language schema sharing (Python ↔ Scala ↔ Java)
"""

from pys_json import get_spark, set_log_level
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.json_schema")


if __name__ == "__main__":
    spark = get_spark("json-schema-string")

    # A JSON schema string (as would be stored in a config file or schema registry)
    schema_json = """{
        "type": "struct",
        "fields": [
            {"name": "name", "type": "string", "nullable": true, "metadata": {}},
            {"name": "age", "type": "integer", "nullable": true, "metadata": {}},
            {"name": "city", "type": "string", "nullable": true, "metadata": {}}
        ]
    }"""
    logger.info("Schema JSON loaded from config:\n%s", schema_json)

    # Use the JSON schema with from_json or read operations
    from pyspark.sql.types import StructType

    schema = StructType.fromJson(__import__("json").loads(schema_json))
    logger.info("Parsed StructType: %s", schema.simpleString())

    # Create sample data and apply schema
    data = [
        '{"name": "Alice", "age": 30, "city": "NYC"}',
        '{"name": "Bob", "age": 25, "city": "LA"}',
    ]
    df = spark.read.schema(schema).json(spark.sparkContext.parallelize(data))
    df.printSchema()
    df.show(truncate=False)

    # Round-trip: export schema back to JSON
    exported = df.schema.json()
    logger.info("Exported schema JSON:\n%s", exported)

    spark.stop()
