"""Variable/dynamic keys in JSON using MapType.

Demonstrates reading JSON files where the keys are not fixed (e.g., dynamic IDs,
locale codes, user-defined keys). MapType schema handles this by treating the
unknown key space as a typed map instead of fixed struct fields.

Key concepts:
    - MapType(StringType(), ValueType) for variable keys
    - Nested schemas: Map → Array → Struct
    - multiLine mode for pretty-printed JSON
    - Accessing map values via getItem() or [] syntax

When to use MapType vs StructType:
    - StructType: Keys are known and fixed at schema definition time
    - MapType: Keys vary per record (user IDs, product codes, locale tags)

Reference:
    https://spark.apache.org/docs/latest/sql-ref-datatypes.html#map-type
"""

from pyspark.sql.types import ArrayType, MapType, StringType, StructType

from pys_json import DATA_HOME, get_spark, set_log_level
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.variable_keys")


if __name__ == "__main__":
    spark = get_spark("variable-keys")

    # Schema: {"Items": {"<dynamic_key>": [{"id": ..., "name": ..., "val": ...}]}}
    val_schema = StructType().add("id", StringType()).add("name", StringType()).add("val", StringType())
    val_arr_schema = ArrayType(val_schema, containsNull=True)
    map_schema = MapType(StringType(), val_arr_schema, valueContainsNull=True)
    json_schema = StructType().add("Items", map_schema)

    logger.info("Schema for variable keys: %s", json_schema.simpleString())

    data_file = DATA_HOME + "/file_data/json/dynamic_keys/dynamic_keys.json"
    logger.info("Reading: %s", data_file)

    df = spark.read.format("json").option("multiLine", True).schema(json_schema).load(data_file)

    df.show(truncate=False)
    df.printSchema()
    logger.info("Schema JSON:\n%s", df.schema.json())

    spark.stop()
