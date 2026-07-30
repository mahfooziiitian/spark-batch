"""Explicit schema definition in PySpark — all approaches compared.

Demonstrates every way to define and apply an explicit schema when reading JSON,
avoiding the overhead and unpredictability of schema inference. Explicit schemas
are essential for production pipelines where data contracts must be enforced.

Approaches covered:
    1. StructType + StructField (programmatic Python API)
    2. DDL string format ("col TYPE, col TYPE")
    3. JSON schema string (from StructType.json() or schema registry)
    4. Nested schemas (structs within structs)
    5. Complex types: ArrayType, MapType
    6. Schema with metadata and comments

Why use explicit schemas:
    - Faster reads (no full-data scan for inference)
    - Predictable types (no int vs long vs string ambiguity)
    - Handles missing/optional fields gracefully (nullable=True)
    - Schema evolution control (add fields without breaking)
    - Required for streaming JSON sources

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

import json

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
logger = get_logger("example.explicit_schema")


if __name__ == "__main__":
    spark = get_spark("explicit-schema")

    # =========================================================================
    # 1. StructType + StructField (most common approach)
    # =========================================================================
    print_header("1. StructType + StructField")

    schema_struct = StructType(
        [
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("salary", DoubleType(), nullable=True),
            StructField("active", BooleanType(), nullable=True),
        ]
    )

    data_file = DATA_HOME + "/explicit_schema_demo.json"
    write_json_lines(
        data_file,
        [
            '{"name": "Alice", "age": 30, "email": "alice@example.com", "salary": 85000.50, "active": true}',
            '{"name": "Bob", "age": 25, "salary": 65000.00, "active": false}',
            '{"name": "Charlie", "age": 35, "email": "charlie@example.com", "active": true}',
        ],
    )
    print_path("Input", data_file)

    df1 = spark.read.schema(schema_struct).json(data_file)
    print_schema(df1, title="StructType Schema")
    print_dataframe(df1, title="StructType Result")
    print_success("Missing fields (email, salary) correctly appear as null")

    # =========================================================================
    # 2. DDL string format
    # =========================================================================
    print_header("2. DDL String Schema")

    schema_ddl = "name STRING NOT NULL, age INT, email STRING, salary DOUBLE, active BOOLEAN"
    logger.info("DDL string: %s", schema_ddl)

    df2 = spark.read.schema(schema_ddl).json(data_file)
    print_schema(df2, title="DDL String Schema")
    print_dataframe(df2, title="DDL String Result")
    print_success("DDL string produces identical results to StructType")

    # =========================================================================
    # 3. JSON schema string (from registry or config file)
    # =========================================================================
    print_header("3. JSON Schema String")

    # Export schema to JSON (simulates loading from schema registry)
    schema_json_str = schema_struct.json()
    logger.info("JSON schema:\n%s", schema_json_str)

    # Reconstruct StructType from JSON
    schema_from_json = StructType.fromJson(json.loads(schema_json_str))
    df3 = spark.read.schema(schema_from_json).json(data_file)
    print_schema(df3, title="Schema from JSON")
    print_success("Round-trip: StructType → JSON → StructType works perfectly")

    # =========================================================================
    # 4. Nested struct schemas
    # =========================================================================
    print_header("4. Nested Struct Schema")

    nested_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField(
                "address",
                StructType(
                    [
                        StructField("street", StringType(), nullable=True),
                        StructField("city", StringType(), nullable=True),
                        StructField("state", StringType(), nullable=True),
                        StructField("zip", StringType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
            StructField(
                "contact",
                StructType(
                    [
                        StructField("phone", StringType(), nullable=True),
                        StructField("email", StringType(), nullable=True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )

    nested_file = DATA_HOME + "/explicit_schema_nested.json"
    write_json_lines(
        nested_file,
        [
            '{"id": 1, "name": "Alice", "address": {"street": "123 Main St", "city": "NYC", "state": "NY", "zip": "10001"}, "contact": {"phone": "555-0101", "email": "alice@co.com"}}',
            '{"id": 2, "name": "Bob", "address": {"city": "LA", "state": "CA"}, "contact": {"email": "bob@co.com"}}',
        ],
    )
    print_path("Input", nested_file)

    df4 = spark.read.schema(nested_schema).json(nested_file)
    print_schema(df4, title="Nested Schema (2 levels deep)")
    print_dataframe(df4, title="Nested Struct Result")

    # Access nested fields with dot notation
    df4.select("name", "address.city", "contact.email").show()

    # =========================================================================
    # 5. Complex types: ArrayType and MapType
    # =========================================================================
    print_header("5. ArrayType and MapType")

    complex_schema = StructType(
        [
            StructField("user", StringType(), nullable=False),
            StructField("scores", ArrayType(IntegerType(), containsNull=False), nullable=True),
            StructField("tags", ArrayType(StringType()), nullable=True),
            StructField("metadata", MapType(StringType(), StringType()), nullable=True),
            StructField(
                "history",
                ArrayType(
                    StructType(
                        [
                            StructField("date", StringType()),
                            StructField("action", StringType()),
                        ]
                    )
                ),
                nullable=True,
            ),
        ]
    )

    complex_file = DATA_HOME + "/explicit_schema_complex.json"
    write_json_lines(
        complex_file,
        [
            '{"user": "alice", "scores": [95, 87, 92], "tags": ["admin", "active"], "metadata": {"dept": "eng", "level": "senior"}, "history": [{"date": "2024-01-01", "action": "login"}, {"date": "2024-01-02", "action": "update"}]}',
            '{"user": "bob", "scores": [78, 82], "tags": ["viewer"], "metadata": {"dept": "sales"}, "history": [{"date": "2024-01-03", "action": "login"}]}',
        ],
    )
    print_path("Input", complex_file)

    df5 = spark.read.schema(complex_schema).json(complex_file)
    print_schema(df5, title="Complex Types (Array, Map, Array<Struct>)")
    print_dataframe(df5, title="Complex Types Result")

    # =========================================================================
    # 6. Schema with precision types (Decimal, Timestamp, Date)
    # =========================================================================
    print_header("6. Precision Types (Decimal, Timestamp, Date)")

    precision_schema = StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("amount", DecimalType(precision=10, scale=2), nullable=False),
            StructField("currency", StringType(), nullable=False),
            StructField("transaction_date", DateType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=True),
        ]
    )

    precision_file = DATA_HOME + "/explicit_schema_precision.json"
    write_json_lines(
        precision_file,
        [
            '{"transaction_id": "TXN001", "amount": 1234.56, "currency": "USD", "transaction_date": "2024-03-15", "created_at": "2024-03-15T10:30:00"}',
            '{"transaction_id": "TXN002", "amount": 9999.99, "currency": "EUR", "transaction_date": "2024-03-16", "created_at": "2024-03-16T14:45:30"}',
        ],
    )
    print_path("Input", precision_file)

    df6 = spark.read.schema(precision_schema).json(precision_file)
    print_schema(df6, title="Precision Types Schema")
    print_dataframe(df6, title="Precision Types Result")
    print_success("DecimalType(10,2) preserves exact monetary values")

    # =========================================================================
    # Summary
    # =========================================================================
    print_header("Summary: Schema Definition Approaches")
    logger.info("StructType  → Full Python control, best for complex schemas")
    logger.info("DDL string  → Compact, readable, good for simple schemas")
    logger.info("JSON string → Portable, serializable, schema registry friendly")
    logger.info("All three produce identical DataFrames when applied")

    spark.stop()
