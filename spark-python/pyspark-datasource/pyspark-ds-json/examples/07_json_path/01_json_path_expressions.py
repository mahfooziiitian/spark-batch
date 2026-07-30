"""JSONPath expressions — extract values from JSON strings using get_json_object.

Demonstrates all JSONPath syntax supported by Spark's get_json_object():
    - $ (root), .key (child), ['key'] (bracket), [n] (index), [*] (wildcard)
    - Nested traversal, array access, wildcard extraction
    - Comparison with from_json and json_tuple approaches
    - Real-world patterns: API responses, nested configs, event payloads

Reference:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.get_json_object.html
"""

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from pys_json import get_spark, print_dataframe, print_header, print_success, set_log_level
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.json_path")


if __name__ == "__main__":
    spark = get_spark("json-path-expressions")

    # =========================================================================
    # 1. Basic field extraction with $ and .key
    # =========================================================================
    print_header("1. Basic Field Extraction")

    data = [
        (1, '{"name":"Alice","age":30,"city":"NYC"}'),
        (2, '{"name":"Bob","age":25,"city":"London"}'),
        (3, '{"name":"Charlie","age":35,"city":"Tokyo"}'),
    ]
    df = spark.createDataFrame(data, ["id", "json_str"])

    # Extract top-level fields
    df_basic = df.select(
        "id",
        F.get_json_object("json_str", "$.name").alias("name"),
        F.get_json_object("json_str", "$.age").alias("age"),
        F.get_json_object("json_str", "$.city").alias("city"),
    )
    print_dataframe(df_basic, title="Top-Level Field Extraction ($.key)")

    # =========================================================================
    # 2. Nested object traversal
    # =========================================================================
    print_header("2. Nested Object Traversal")

    nested_data = [
        (1, '{"user":{"name":"Alice","address":{"street":"123 Main St","city":"NYC","zip":"10001"}}}'),
        (2, '{"user":{"name":"Bob","address":{"street":"456 Oak Ave","city":"London","zip":"SW1A"}}}'),
    ]
    df_nested = spark.createDataFrame(nested_data, ["id", "payload"])

    df_extracted = df_nested.select(
        "id",
        F.get_json_object("payload", "$.user.name").alias("name"),
        F.get_json_object("payload", "$.user.address.city").alias("city"),
        F.get_json_object("payload", "$.user.address.zip").alias("zip"),
    )
    print_dataframe(df_extracted, title="Nested Traversal ($.user.address.city)")

    # =========================================================================
    # 3. Array index access
    # =========================================================================
    print_header("3. Array Index Access")

    array_data = [
        (1, '{"items":["apple","banana","cherry"],"scores":[95,87,92]}'),
        (2, '{"items":["dog","cat","bird"],"scores":[88,91,76]}'),
    ]
    df_array = spark.createDataFrame(array_data, ["id", "json_str"])

    df_index = df_array.select(
        "id",
        F.get_json_object("json_str", "$.items[0]").alias("first_item"),
        F.get_json_object("json_str", "$.items[2]").alias("third_item"),
        F.get_json_object("json_str", "$.scores[0]").alias("top_score"),
    )
    print_dataframe(df_index, title="Array Index ($.items[0])")

    # =========================================================================
    # 4. Array wildcard [*]
    # =========================================================================
    print_header("4. Array Wildcard [*]")

    orders_data = [
        (
            1,
            '{"order_id":"ORD-001","items":[{"sku":"A1","name":"Widget","qty":2},{"sku":"B2","name":"Gadget","qty":1}]}',
        ),
        (
            2,
            '{"order_id":"ORD-002","items":[{"sku":"C3","name":"Doohickey","qty":5},{"sku":"A1","name":"Widget","qty":3}]}',
        ),
    ]
    df_orders = spark.createDataFrame(orders_data, ["id", "json_str"])

    # Wildcard extracts all matching values as a JSON array string
    df_wildcard = df_orders.select(
        "id",
        F.get_json_object("json_str", "$.order_id").alias("order"),
        F.get_json_object("json_str", "$.items[*].name").alias("all_names"),
        F.get_json_object("json_str", "$.items[*].sku").alias("all_skus"),
    )
    print_dataframe(df_wildcard, title="Wildcard ($.items[*].name)")

    # =========================================================================
    # 5. Bracket notation for special keys
    # =========================================================================
    print_header("5. Bracket Notation for Special Keys")

    special_data = [
        (1, '{"first name":"Alice","last-name":"Smith","@type":"person","123":"numeric_key"}'),
        (2, '{"first name":"Bob","last-name":"Jones","@type":"employee","123":"another_key"}'),
    ]
    df_special = spark.createDataFrame(special_data, ["id", "json_str"])

    # Keys with spaces, hyphens, or special chars need bracket notation
    df_bracket = df_special.select(
        "id",
        F.get_json_object("json_str", "$['first name']").alias("first_name"),
        F.get_json_object("json_str", "$['last-name']").alias("last_name"),
        F.get_json_object("json_str", "$['@type']").alias("type"),
        F.get_json_object("json_str", "$['123']").alias("numeric_key"),
    )
    print_dataframe(df_bracket, title="Bracket Notation ($['first name'])")

    # =========================================================================
    # 6. Nested arrays and objects
    # =========================================================================
    print_header("6. Nested Arrays and Objects")

    complex_data = [
        (
            1,
            '{"company":"Acme","departments":[{"name":"Engineering","teams":[{"lead":"Alice","size":5},{"lead":"Bob","size":3}]},{"name":"Sales","teams":[{"lead":"Charlie","size":8}]}]}',
        ),
    ]
    df_complex = spark.createDataFrame(complex_data, ["id", "json_str"])

    df_deep = df_complex.select(
        "id",
        F.get_json_object("json_str", "$.company").alias("company"),
        F.get_json_object("json_str", "$.departments[0].name").alias("first_dept"),
        F.get_json_object("json_str", "$.departments[0].teams[0].lead").alias("eng_lead_1"),
        F.get_json_object("json_str", "$.departments[0].teams[1].lead").alias("eng_lead_2"),
        F.get_json_object("json_str", "$.departments[1].teams[0].size").alias("sales_team_size"),
    )
    print_dataframe(df_deep, title="Deep Nested Access")

    # =========================================================================
    # 7. Handling nulls and missing paths
    # =========================================================================
    print_header("7. Handling Nulls and Missing Paths")

    nullable_data = [
        (1, '{"name":"Alice","email":"alice@example.com"}'),
        (2, '{"name":"Bob"}'),
        (3, None),
        (4, '{"name":"Diana","email":null}'),
    ]
    df_null = spark.createDataFrame(nullable_data, ["id", "json_str"])

    df_null_check = df_null.select(
        "id",
        F.get_json_object("json_str", "$.name").alias("name"),
        F.get_json_object("json_str", "$.email").alias("email"),
        F.get_json_object("json_str", "$.phone").alias("phone_missing"),
    )
    print_dataframe(df_null_check, title="Missing Paths Return NULL")

    # Coalesce for defaults
    df_defaults = df_null.select(
        "id",
        F.coalesce(F.get_json_object("json_str", "$.email"), F.lit("N/A")).alias("email"),
    )
    print_dataframe(df_defaults, title="Coalesce for Default Values")

    # =========================================================================
    # 8. Real-world: API response parsing
    # =========================================================================
    print_header("8. Real-World: API Response Parsing")

    api_responses = [
        Row(
            request_id="req-001",
            response='{"status":"success","data":{"user":{"id":42,"name":"Alice","roles":["admin","editor"]},"token":"abc123"}}',
        ),
        Row(
            request_id="req-002",
            response='{"status":"error","error":{"code":404,"message":"User not found"},"data":null}',
        ),
        Row(
            request_id="req-003",
            response='{"status":"success","data":{"user":{"id":99,"name":"Bob","roles":["viewer"]},"token":"xyz789"}}',
        ),
    ]
    df_api = spark.createDataFrame(api_responses)

    df_parsed_api = df_api.select(
        "request_id",
        F.get_json_object("response", "$.status").alias("status"),
        F.get_json_object("response", "$.data.user.name").alias("user_name"),
        F.get_json_object("response", "$.data.user.roles[0]").alias("primary_role"),
        F.get_json_object("response", "$.error.message").alias("error_msg"),
    )
    print_dataframe(df_parsed_api, title="API Response Extraction")

    # =========================================================================
    # 9. JSONPath vs from_json vs json_tuple — comparison
    # =========================================================================
    print_header("9. JSONPath vs from_json vs json_tuple")

    compare_data = [
        (1, '{"name":"Alice","age":30,"city":"NYC"}'),
        (2, '{"name":"Bob","age":25,"city":"London"}'),
    ]
    df_compare = spark.createDataFrame(compare_data, ["id", "json_str"])

    # Method 1: get_json_object (JSONPath) — one call per field
    df_jsonpath = df_compare.select(
        "id",
        F.get_json_object("json_str", "$.name").alias("name"),
        F.get_json_object("json_str", "$.age").cast("int").alias("age"),
    )
    print_dataframe(df_jsonpath, title="Method 1: get_json_object (JSONPath)")

    # Method 2: json_tuple — multiple fields in one call (efficient for top-level)
    df_tuple = df_compare.select(
        "id",
        F.json_tuple("json_str", "name", "age", "city").alias("name", "age", "city"),
    )
    print_dataframe(df_tuple, title="Method 2: json_tuple (multi-field)")

    # Method 3: from_json — full struct parsing (typed, most powerful)
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("age", StringType()),
            StructField("city", StringType()),
        ]
    )
    df_from_json = df_compare.select("id", F.from_json("json_str", schema).alias("data")).select("id", "data.*")
    print_dataframe(df_from_json, title="Method 3: from_json (full struct)")

    logger.info("Comparison Summary:")
    logger.info("  get_json_object: Best for 1-2 deeply nested fields, returns STRING")
    logger.info("  json_tuple:      Best for multiple TOP-LEVEL keys, returns STRINGs")
    logger.info("  from_json:       Best for full parsing into typed struct columns")
    print_success("Choose based on: depth of access, number of fields, and type requirements")

    # =========================================================================
    # 10. Performance tip: extract then filter
    # =========================================================================
    print_header("10. Performance: Extract Then Filter")

    events = [
        (1, '{"event":"click","page":"/home","user_id":42}'),
        (2, '{"event":"purchase","page":"/checkout","user_id":42,"amount":99.99}'),
        (3, '{"event":"click","page":"/products","user_id":99}'),
        (4, '{"event":"purchase","page":"/checkout","user_id":99,"amount":149.50}'),
    ]
    df_events = spark.createDataFrame(events, ["id", "json_str"])

    # Extract + filter pattern (efficient for selective processing)
    df_purchases = (
        df_events.withColumn("event_type", F.get_json_object("json_str", "$.event"))
        .filter(F.col("event_type") == "purchase")
        .select(
            "id",
            F.get_json_object("json_str", "$.user_id").cast("int").alias("user_id"),
            F.get_json_object("json_str", "$.amount").cast("double").alias("amount"),
        )
    )
    print_dataframe(df_purchases, title="Filtered Purchases via JSONPath")

    # For repeated access to same JSON, from_json is more efficient
    logger.info("Tip: If accessing 3+ fields from same JSON, prefer from_json over multiple get_json_object calls")

    # =========================================================================
    # 11. Extracting entire sub-objects as JSON strings
    # =========================================================================
    print_header("11. Extract Sub-Objects as JSON Strings")

    config_data = [
        (
            "app1",
            '{"db":{"host":"localhost","port":5432,"credentials":{"user":"admin","pass":"secret"}},"cache":{"ttl":300}}',
        ),
        (
            "app2",
            '{"db":{"host":"prod-db","port":3306,"credentials":{"user":"app","pass":"prod123"}},"cache":{"ttl":600}}',
        ),
    ]
    df_config = spark.createDataFrame(config_data, ["app", "config"])

    # Extract sub-objects — returned as JSON strings
    df_sub = df_config.select(
        "app",
        F.get_json_object("config", "$.db").alias("db_config_json"),
        F.get_json_object("config", "$.db.credentials").alias("creds_json"),
        F.get_json_object("config", "$.cache.ttl").alias("cache_ttl"),
    )
    print_dataframe(df_sub, title="Sub-Object Extraction (returns JSON string)")

    # Chain: extract sub-object, then parse it
    creds_schema = StructType(
        [
            StructField("user", StringType()),
            StructField("pass", StringType()),
        ]
    )
    df_creds = df_sub.select(
        "app",
        F.from_json("creds_json", creds_schema).alias("creds"),
    ).select("app", "creds.*")
    print_dataframe(df_creds, title="Chained: JSONPath → from_json")

    # =========================================================================
    # 12. Dynamic JSONPath with column values
    # =========================================================================
    print_header("12. Dynamic JSONPath (UDF approach)")

    # get_json_object requires a literal path — for dynamic paths, use a UDF
    import json

    from pyspark.sql.types import StringType as ST

    @F.udf(ST())
    def dynamic_json_extract(json_str: str, path_key: str) -> str | None:
        """Extract a top-level key dynamically from a JSON string."""
        if not json_str or not path_key:
            return None
        try:
            obj = json.loads(json_str)
            return str(obj.get(path_key)) if path_key in obj else None
        except (json.JSONDecodeError, TypeError):
            return None

    dynamic_data = [
        (1, '{"name":"Alice","age":30}', "name"),
        (2, '{"name":"Bob","age":25}', "age"),
        (3, '{"name":"Charlie","city":"NYC"}', "city"),
    ]
    df_dynamic = spark.createDataFrame(dynamic_data, ["id", "json_str", "target_key"])

    df_dynamic_result = df_dynamic.select(
        "id",
        "target_key",
        dynamic_json_extract("json_str", "target_key").alias("extracted_value"),
    )
    print_dataframe(df_dynamic_result, title="Dynamic Key Extraction via UDF")
    logger.info("Note: UDFs are slower than built-in functions — use only when path is truly dynamic")

    print_success("JSONPath examples complete — 12 patterns demonstrated")
    spark.stop()
