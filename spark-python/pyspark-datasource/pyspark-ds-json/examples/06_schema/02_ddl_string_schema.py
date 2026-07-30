"""DDL string schema definition in PySpark.

Demonstrates using DDL (Data Definition Language) formatted strings to define
schemas — the most concise way to express schemas in Spark. DDL strings are
human-readable, configuration-friendly, and supported across Spark languages.

Key concepts:
    - DDL string format: "col_name TYPE, col_name TYPE"
    - NOT NULL constraints in DDL strings
    - Complex types: STRUCT<>, ARRAY<>, MAP<>
    - Converting between StructType ↔ DDL string
    - DDL strings in spark.read.schema() and from_json()
    - Nested and deeply complex DDL expressions

DDL syntax reference:
    Simple:    "name STRING, age INT, active BOOLEAN"
    Not null:  "id LONG NOT NULL, name STRING NOT NULL"
    Array:     "tags ARRAY<STRING>"
    Map:       "props MAP<STRING, STRING>"
    Struct:    "address STRUCT<city: STRING, zip: STRING>"
    Nested:    "data STRUCT<items: ARRAY<STRUCT<id: INT, name: STRING>>>"

Advantages of DDL strings:
    - Compact — single line for simple schemas
    - Readable — resembles SQL CREATE TABLE syntax
    - Portable — works in Python, Scala, Java, SQL
    - Config-friendly — easy to store in YAML/JSON/env vars

Reference:
    https://spark.apache.org/docs/latest/sql-ref-datatypes.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from pys_json import (
    DATA_HOME,
    get_spark,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.ddl_schema")


if __name__ == "__main__":
    spark = get_spark("ddl-string-schema")

    # =========================================================================
    # 1. Basic DDL string — flat schema
    # =========================================================================
    print_header("1. Basic DDL String (flat schema)")

    ddl_basic = "name STRING NOT NULL, age INT, email STRING, salary DOUBLE, active BOOLEAN"
    logger.info("DDL: %s", ddl_basic)

    basic_file = DATA_HOME + "/ddl_schema_basic.json"
    write_json_lines(
        basic_file,
        [
            '{"name": "Alice", "age": 30, "email": "alice@co.com", "salary": 85000.50, "active": true}',
            '{"name": "Bob", "age": 25, "salary": 65000.00, "active": false}',
            '{"name": "Charlie", "age": 35, "email": "charlie@co.com"}',
        ],
    )
    print_path("Input", basic_file)

    df_basic = spark.read.schema(ddl_basic).json(basic_file)
    print_schema(df_basic, title="Basic DDL Schema")
    print_dataframe(df_basic, title="Basic DDL Result")
    print_success("DDL string passed directly to .schema() — no StructType needed")

    # =========================================================================
    # 2. DDL with complex types (ARRAY, MAP, STRUCT)
    # =========================================================================
    print_header("2. Complex Types in DDL")

    ddl_complex = (
        "user STRING NOT NULL, "
        "scores ARRAY<INT>, "
        "tags ARRAY<STRING>, "
        "metadata MAP<STRING, STRING>, "
        "address STRUCT<city: STRING, state: STRING, zip: STRING>"
    )
    logger.info("DDL: %s", ddl_complex)

    complex_file = DATA_HOME + "/ddl_schema_complex.json"
    write_json_lines(
        complex_file,
        [
            '{"user": "Alice", "scores": [95, 87], "tags": ["admin"], "metadata": {"dept": "eng"}, "address": {"city": "NYC", "state": "NY", "zip": "10001"}}',
            '{"user": "Bob", "scores": [78], "tags": ["viewer", "new"], "metadata": {"dept": "sales", "region": "west"}, "address": {"city": "LA", "state": "CA", "zip": "90001"}}',
        ],
    )
    print_path("Input", complex_file)

    df_complex = spark.read.schema(ddl_complex).json(complex_file)
    print_schema(df_complex, title="Complex DDL Schema")
    print_dataframe(df_complex, title="Complex Types Result")

    # =========================================================================
    # 3. Deeply nested DDL (ARRAY of STRUCT)
    # =========================================================================
    print_header("3. Deeply Nested DDL")

    ddl_nested = (
        "order_id STRING NOT NULL, "
        "customer STRUCT<name: STRING, tier: STRING>, "
        "items ARRAY<STRUCT<product: STRING, qty: INT, price: DOUBLE>>, "
        "shipping MAP<STRING, STRING>"
    )
    logger.info("DDL: %s", ddl_nested)

    nested_file = DATA_HOME + "/ddl_schema_nested.json"
    write_json_lines(
        nested_file,
        [
            '{"order_id": "ORD-001", "customer": {"name": "Alice", "tier": "gold"}, "items": [{"product": "Widget", "qty": 2, "price": 9.99}, {"product": "Gadget", "qty": 1, "price": 24.99}], "shipping": {"method": "express", "carrier": "FedEx"}}',
            '{"order_id": "ORD-002", "customer": {"name": "Bob", "tier": "silver"}, "items": [{"product": "Widget", "qty": 5, "price": 9.99}], "shipping": {"method": "standard", "carrier": "USPS"}}',
        ],
    )
    print_path("Input", nested_file)

    df_nested = spark.read.schema(ddl_nested).json(nested_file)
    print_schema(df_nested, title="Nested DDL Schema")
    print_dataframe(df_nested, title="Nested DDL Result")

    # =========================================================================
    # 4. DDL in from_json() for parsing JSON columns
    # =========================================================================
    print_header("4. DDL String with from_json()")

    raw_df = spark.createDataFrame(
        [
            (1, '{"name": "Alice", "age": 30, "city": "NYC"}'),
            (2, '{"name": "Bob", "age": 25, "city": "LA"}'),
        ],
        ["id", "json_payload"],
    )

    ddl_from_json = "name STRING, age INT, city STRING"
    parsed_df = raw_df.withColumn(
        "parsed",
        F.from_json(F.col("json_payload"), ddl_from_json),
    )
    print_schema(parsed_df, title="from_json() with DDL schema")
    parsed_df.select("id", "parsed.name", "parsed.age", "parsed.city").show()
    print_success("DDL strings work seamlessly with from_json()")

    # =========================================================================
    # 5. Converting between StructType and DDL
    # =========================================================================
    print_header("5. StructType ↔ DDL Conversion")

    # DDL → StructType
    struct_from_ddl = StructType.fromDDL(ddl_basic)
    logger.info("DDL → StructType: %s", struct_from_ddl)

    # StructType → DDL (via simpleString, strip 'struct<...>')
    struct_ddl = struct_from_ddl.simpleString()
    logger.info("StructType → simpleString: %s", struct_ddl)

    # Round-trip verification
    round_trip = StructType.fromDDL(ddl_basic)
    logger.info("Round-trip field names: %s", round_trip.fieldNames())
    print_success("StructType.fromDDL() enables DDL → StructType conversion")

    # =========================================================================
    # 6. DDL with all supported types
    # =========================================================================
    print_header("6. All Supported Types in DDL")

    ddl_all_types = (
        "col_string STRING, "
        "col_int INT, "
        "col_long LONG, "
        "col_float FLOAT, "
        "col_double DOUBLE, "
        "col_decimal DECIMAL(10,2), "
        "col_boolean BOOLEAN, "
        "col_date DATE, "
        "col_timestamp TIMESTAMP, "
        "col_binary BINARY, "
        "col_array ARRAY<STRING>, "
        "col_map MAP<STRING, INT>, "
        "col_struct STRUCT<x: INT, y: INT>"
    )
    logger.info("Full type catalog DDL:\n%s", ddl_all_types)

    all_types_schema = StructType.fromDDL(ddl_all_types)
    print_schema(
        spark.createDataFrame([], all_types_schema),
        title="All Supported DDL Types",
    )

    # =========================================================================
    # Summary
    # =========================================================================
    print_header("Summary")
    logger.info("DDL strings: most concise schema definition format")
    logger.info("Use for: config files, from_json(), simple-to-moderate schemas")
    logger.info("For very complex schemas, StructType gives more control")
    logger.info("StructType.fromDDL() bridges DDL strings ↔ Python types")

    spark.stop()
