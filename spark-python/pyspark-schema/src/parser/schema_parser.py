import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, ArrayType,
    _parse_datatype_string,
)

DDL_EXAMPLES = [
    "bigint",
    "string",
    "array<string>",
    "map<string, bigint>",
    "struct<id:bigint,name:string,score:double>",
    "array<struct<subject:string,score:double>>",
]

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-parser")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # -- _parse_datatype_string -------------------------------------------
    print("=== _parse_datatype_string ===")
    for ddl in DDL_EXAMPLES:
        dtype = _parse_datatype_string(ddl)
        print(f"  {ddl!r:<50} → {dtype}")

    # -- _parse_datatype_string (struct DDL) ------------------------------
    print("\n=== _parse_datatype_string (struct DDL) ===")
    schema = _parse_datatype_string(
        "struct<id:bigint not null,name:string,score:double,tags:array<string>>"
    )
    df = spark.createDataFrame(
        [(1, "Alice", 95.0, ["python", "spark"])],
        schema=schema,
    )
    df.printSchema()
    df.show(truncate=False)

    # -- JSON round-trip --------------------------------------------------
    print("=== JSON round-trip ===")
    schema_back = StructType.fromJson(json.loads(schema.json()))
    print("equal:", schema == schema_back)

    # -- Per-field simpleString -------------------------------------------
    print("\n=== field simpleStrings ===")
    for field in schema.fields:
        print(f"  {field.name:<8} {field.dataType.simpleString():<20} nullable={field.nullable}")

    # -- Nested complex type from string ----------------------------------
    print("\n=== nested complex type from string ===")
    nested = _parse_datatype_string("array<struct<subject:string,score:double>>")
    print(repr(nested))

    spark.stop()
