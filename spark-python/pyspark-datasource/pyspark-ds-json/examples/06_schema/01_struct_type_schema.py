"""StructType class-based schema definition.

Demonstrates building schemas programmatically using StructType and StructField —
the most powerful and flexible way to define schemas in PySpark. This approach
gives full control over field names, types, nullability, and nested structures.

Key concepts:
    - StructType / StructField for explicit schema definition
    - Nullable vs non-nullable fields and their effect on data loading
    - Nested StructType for hierarchical JSON
    - ArrayType and MapType for collections
    - Applying schema to JSON reads (bypasses inference)
    - Schema composition: building complex schemas from reusable parts
    - .add() fluent API vs StructField list constructor

When to use StructType:
    - Production pipelines where schema must be enforced
    - Complex nested JSON with arrays and maps
    - When you need nullable/not-null constraints
    - Schema composition (reusing sub-schemas across datasets)

Reference:
    https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
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
logger = get_logger("example.struct_type_schema")


if __name__ == "__main__":
    spark = get_spark("struct-type-schema")

    # =========================================================================
    # 1. Basic flat schema using StructField list
    # =========================================================================
    print_header("1. Basic Flat Schema (StructField list)")

    flat_schema = StructType(
        [
            StructField("name", StringType(), nullable=False),
            StructField("age", IntegerType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("active", BooleanType(), nullable=True),
        ]
    )

    flat_file = DATA_HOME + "/file_data/json/schema/struct_type_flat.json"
    write_json_lines(
        flat_file,
        [
            '{"name": "Alice", "age": 30, "email": "alice@example.com", "active": true}',
            '{"name": "Bob", "age": 25, "active": false}',
            '{"name": "Charlie", "age": 35, "email": "charlie@example.com"}',
        ],
    )
    print_path("Input", flat_file)

    df_flat = spark.read.schema(flat_schema).json(flat_file)
    print_schema(df_flat, title="Flat Schema (4 fields)")
    print_dataframe(df_flat, title="Flat Schema Result")
    print_success("Missing fields resolve to null when nullable=True")

    # =========================================================================
    # 2. Fluent .add() API (alternative syntax)
    # =========================================================================
    print_header("2. Fluent .add() API")

    fluent_schema = (
        StructType()
        .add("id", LongType(), nullable=False)
        .add("product", StringType(), nullable=False)
        .add("price", DoubleType(), nullable=True)
        .add("quantity", IntegerType(), nullable=True)
    )
    logger.info("Schema via .add(): %s", fluent_schema.simpleString())
    print_schema(
        spark.createDataFrame([], fluent_schema),
        title="Fluent .add() Schema",
    )
    print_success(".add() returns StructType — chainable and concise")

    # =========================================================================
    # 3. Nested struct schemas
    # =========================================================================
    print_header("3. Nested StructType (hierarchical JSON)")

    address_schema = StructType(
        [
            StructField("street", StringType(), nullable=True),
            StructField("city", StringType(), nullable=False),
            StructField("state", StringType(), nullable=True),
            StructField("zip", StringType(), nullable=True),
        ]
    )

    contact_schema = StructType(
        [
            StructField("phone", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
        ]
    )

    # Compose top-level schema from reusable sub-schemas
    person_schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("name", StringType(), nullable=False),
            StructField("address", address_schema, nullable=True),
            StructField("contact", contact_schema, nullable=True),
        ]
    )

    nested_file = DATA_HOME + "/file_data/json/schema/struct_type_nested.json"
    write_json_lines(
        nested_file,
        [
            '{"id": 1, "name": "Alice", "address": {"street": "123 Main St", "city": "NYC", "state": "NY", "zip": "10001"}, "contact": {"phone": "555-0101", "email": "alice@co.com"}}',
            '{"id": 2, "name": "Bob", "address": {"city": "LA", "state": "CA"}, "contact": {"email": "bob@co.com"}}',
            '{"id": 3, "name": "Charlie", "address": {"city": "Chicago"}}',
        ],
    )
    print_path("Input", nested_file)

    df_nested = spark.read.schema(person_schema).json(nested_file)
    print_schema(df_nested, title="Nested Schema (address + contact)")
    print_dataframe(df_nested, title="Nested Struct Result")

    # Dot notation access
    logger.info("Dot notation access:")
    df_nested.select("name", "address.city", "contact.email").show()

    # =========================================================================
    # 4. ArrayType schemas
    # =========================================================================
    print_header("4. ArrayType (lists/arrays)")

    array_schema = StructType(
        [
            StructField("user", StringType(), nullable=False),
            StructField("scores", ArrayType(IntegerType(), containsNull=False), nullable=True),
            StructField("tags", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField(
                "friends",
                ArrayType(
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("since", StringType()),
                        ]
                    ),
                ),
                nullable=True,
            ),
        ]
    )

    array_file = DATA_HOME + "/file_data/json/schema/struct_type_array.json"
    write_json_lines(
        array_file,
        [
            '{"user": "Alice", "scores": [95, 87, 92], "tags": ["admin", "active"], "friends": [{"name": "Bob", "since": "2020"}, {"name": "Charlie", "since": "2021"}]}',
            '{"user": "Bob", "scores": [78, 82], "tags": ["viewer"], "friends": [{"name": "Alice", "since": "2020"}]}',
        ],
    )
    print_path("Input", array_file)

    df_array = spark.read.schema(array_schema).json(array_file)
    print_schema(df_array, title="ArrayType Schema")
    print_dataframe(df_array, title="Array Fields Result")

    # =========================================================================
    # 5. MapType schemas (key-value pairs)
    # =========================================================================
    print_header("5. MapType (dynamic key-value pairs)")

    map_schema = StructType(
        [
            StructField("user", StringType(), nullable=False),
            StructField("preferences", MapType(StringType(), StringType()), nullable=True),
            StructField("metrics", MapType(StringType(), DoubleType()), nullable=True),
        ]
    )

    map_file = DATA_HOME + "/file_data/json/schema/struct_type_map.json"
    write_json_lines(
        map_file,
        [
            '{"user": "Alice", "preferences": {"theme": "dark", "lang": "en"}, "metrics": {"cpu": 0.85, "mem": 0.62}}',
            '{"user": "Bob", "preferences": {"theme": "light", "lang": "fr", "timezone": "CET"}, "metrics": {"cpu": 0.45}}',
        ],
    )
    print_path("Input", map_file)

    df_map = spark.read.schema(map_schema).json(map_file)
    print_schema(df_map, title="MapType Schema")
    print_dataframe(df_map, title="Map Fields Result")

    # =========================================================================
    # 6. Schema introspection
    # =========================================================================
    print_header("6. Schema Introspection")

    logger.info("Field names: %s", person_schema.fieldNames())
    logger.info("Number of fields: %d", len(person_schema.fields))
    logger.info("DDL: %s", person_schema.simpleString())

    # Check if field exists
    field_names = set(person_schema.fieldNames())
    logger.info("Has 'address' field: %s", "address" in field_names)
    logger.info("Has 'phone' field: %s", "phone" in field_names)

    # Access specific field metadata
    address_field = person_schema["address"]
    logger.info("address type: %s", address_field.dataType.simpleString())
    logger.info("address nullable: %s", address_field.nullable)

    # Export formats
    logger.info("JSON export:\n%s", person_schema.json())
    print_success("StructType supports full introspection and serialization")

    spark.stop()
