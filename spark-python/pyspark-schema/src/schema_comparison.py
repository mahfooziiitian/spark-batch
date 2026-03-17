import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, IntegerType,
)


def schema_diff(schema_a: StructType, schema_b: StructType) -> dict:
    """
    Compare two schemas and return a summary dict with four keys:

    - missing_in_b    — fields present in a but absent from b
    - extra_in_b      — fields present in b but absent from a
    - type_mismatches — fields in both but with different DataTypes
    - nullable_changes — fields in both but with different nullable flags
    """
    fields_a = {f.name: f for f in schema_a.fields}
    fields_b = {f.name: f for f in schema_b.fields}

    missing_in_b = [n for n in fields_a if n not in fields_b]
    extra_in_b   = [n for n in fields_b if n not in fields_a]

    type_mismatches = [
        {"column": n,
         "type_a": fields_a[n].dataType.simpleString(),
         "type_b": fields_b[n].dataType.simpleString()}
        for n in fields_a
        if n in fields_b and fields_a[n].dataType != fields_b[n].dataType
    ]

    nullable_changes = [
        {"column": n,
         "nullable_a": fields_a[n].nullable,
         "nullable_b": fields_b[n].nullable}
        for n in fields_a
        if n in fields_b and fields_a[n].nullable != fields_b[n].nullable
    ]

    return {
        "missing_in_b":     missing_in_b,
        "extra_in_b":       extra_in_b,
        "type_mismatches":  type_mismatches,
        "nullable_changes": nullable_changes,
    }


def is_backward_compatible(reader: StructType, writer: StructType) -> bool:
    """
    True when a reader using *reader* schema can safely consume data written
    with *writer* schema.

    Rules:
    - Every non-nullable reader field must exist in the writer with the same type.
    - Nullable reader fields absent from the writer will be null (acceptable).
    - Type mismatches always fail.
    - Extra writer fields are silently ignored.
    """
    writer_fields = {f.name: f for f in writer.fields}
    for field in reader.fields:
        if field.name not in writer_fields:
            if not field.nullable:
                return False        # non-nullable column has no data to fill
        elif writer_fields[field.name].dataType != field.dataType:
            return False
    return True


# -- Schema versions -------------------------------------------------------
SCHEMA_V1 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])

SCHEMA_V2 = StructType([
    StructField("id",     LongType(),   nullable=False),
    StructField("name",   StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),   # added — nullable
])

SCHEMA_BROKEN = StructType([
    StructField("id",     IntegerType(), nullable=False),  # type changed: long → int
    StructField("name",   StringType(),  nullable=True),
    # 'amount' removed
])

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("schema-comparison")
             .master(os.environ.get("SPARK_MASTER", "local[*]"))
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("=== v1 vs v2 ===")
    diff = schema_diff(SCHEMA_V1, SCHEMA_V2)
    for key, val in diff.items():
        print(f"  {key:<18}: {val}")

    print("\n=== v1 vs broken ===")
    diff2 = schema_diff(SCHEMA_V1, SCHEMA_BROKEN)
    for key, val in diff2.items():
        print(f"  {key:<18}: {val}")

    print("\n=== backward compatibility ===")
    cases = [
        ("v1 reads v2 data (extra nullable column)",  SCHEMA_V1, SCHEMA_V2),
        ("v2 reads v1 data (region missing, nullable)", SCHEMA_V2, SCHEMA_V1),
        ("v1 reads broken data (type mismatch)",      SCHEMA_V1, SCHEMA_BROKEN),
        ("v2 reads broken data",                      SCHEMA_V2, SCHEMA_BROKEN),
    ]
    for label, reader, writer in cases:
        ok = is_backward_compatible(reader, writer)
        print(f"  {'✓' if ok else '✗'}  {label}")

    spark.stop()
