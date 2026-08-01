"""Case sensitivity issues — handling JSON fields with mixed case.

Demonstrates how Spark handles JSON fields that differ only in case (ID vs id),
the effect of spark.sql.caseSensitive setting, and normalization strategies.

Key concepts:
    - spark.sql.caseSensitive=false (default): fields merge, last value wins
    - spark.sql.caseSensitive=true: separate columns for ID and id
    - Both modes can produce unexpected results with mixed-case data
    - Normalize to lowercase immediately after ingestion
    - Pre-processing raw text for controlled case normalization

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
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
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.case_sensitivity")


if __name__ == "__main__":
    spark = get_spark("case-sensitivity")

    # =========================================================================
    # 1. The problem — mixed case field names
    # =========================================================================
    print_header("1. The Problem — Mixed Case Fields")

    case_file = DATA_HOME + "/case_sensitivity.json"
    write_json_lines(
        case_file,
        [
            '{"ID": 1, "Name": "Alice", "STATUS": "active"}',
            '{"id": 2, "name": "Bob", "status": "inactive"}',
            '{"Id": 3, "NAME": "Charlie", "Status": "pending"}',
        ],
    )
    print_path("Input", case_file)
    print_warning("Same logical fields with different case: ID/id/Id, Name/name/NAME")

    # =========================================================================
    # 2. Default behavior (caseSensitive=false)
    # =========================================================================
    print_header("2. Default: caseSensitive=false")

    current_setting = spark.conf.get("spark.sql.caseSensitive")
    logger.info("spark.sql.caseSensitive = %s", current_setting)

    df_default = spark.read.json(case_file)
    print_schema(df_default, title="Schema with caseSensitive=false")
    print_dataframe(df_default, title="Data — fields merged (case-insensitive)")
    print_warning(
        "With caseSensitive=false, Spark merges fields differing only in case — "
        "but the chosen column name may be unpredictable"
    )

    # =========================================================================
    # 3. Case-sensitive mode
    # =========================================================================
    print_header("3. caseSensitive=true — Separate Columns")

    spark.conf.set("spark.sql.caseSensitive", "true")
    logger.info("Set spark.sql.caseSensitive = true")

    df_sensitive = spark.read.json(case_file)
    print_schema(df_sensitive, title="Schema with caseSensitive=true")
    print_dataframe(df_sensitive, title="Data — each case variant is a separate column")
    print_warning(
        "caseSensitive=true creates separate columns: ID, Id, id — "
        "most records have nulls in 'wrong' columns"
    )

    # Reset to default
    spark.conf.set("spark.sql.caseSensitive", "false")

    # =========================================================================
    # 4. Coalesce case variants
    # =========================================================================
    print_header("4. Coalesce Case Variants")

    spark.conf.set("spark.sql.caseSensitive", "true")
    df_variants = spark.read.json(case_file)

    # Identify all case variants of each logical field
    df_coalesced = df_variants.select(
        F.coalesce(F.col("ID"), F.col("Id"), F.col("id")).alias("id"),
        F.coalesce(F.col("NAME"), F.col("Name"), F.col("name")).alias("name"),
        F.coalesce(F.col("STATUS"), F.col("Status"), F.col("status")).alias("status"),
    )
    print_dataframe(df_coalesced, title="Coalesced case variants")
    print_success("Use coalesce() to merge case variants into a single canonical column")

    spark.conf.set("spark.sql.caseSensitive", "false")

    # =========================================================================
    # 5. Pre-process: normalize keys in raw text
    # =========================================================================
    print_header("5. Best Approach — Explicit Schema (Case-Insensitive Match)")

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )

    # With caseSensitive=false and explicit schema, fields match regardless of case
    df_normalized = spark.read.schema(schema).json(case_file)
    print_schema(df_normalized, title="Explicit schema (case-insensitive match)")
    print_dataframe(df_normalized, title="All case variants matched to lowercase schema")
    print_success(
        "Explicit schema with caseSensitive=false: schema field 'name' matches "
        "'Name', 'NAME', 'name' in the JSON"
    )

    # =========================================================================
    # 6. UDF-based key normalization for complex cases
    # =========================================================================
    print_header("6. UDF Key Normalization")

    import json

    @F.udf(StringType())
    def normalize_json_keys(json_str: str) -> str:
        """Lowercase all keys in a JSON object string."""
        if not json_str:
            return json_str
        try:
            obj = json.loads(json_str)
            if isinstance(obj, dict):
                return json.dumps({k.lower(): v for k, v in obj.items()})
            return json_str
        except (json.JSONDecodeError, TypeError):
            return json_str

    raw_df = spark.read.text(case_file)
    df_udf_normalized = raw_df.select(normalize_json_keys(F.col("value")).alias("value"))
    print_dataframe(df_udf_normalized, title="Keys normalized via UDF")

    # Parse the normalized JSON
    df_parsed = spark.read.schema(schema).json(
        df_udf_normalized.select("value").rdd.map(lambda r: r[0])
    )
    print_dataframe(df_parsed, title="Parsed after key normalization")
    print_success("UDF normalization gives full control over key transformation")

    # =========================================================================
    # 7. Detection — find case conflicts in your data
    # =========================================================================
    print_header("7. Detect Case Conflicts")

    spark.conf.set("spark.sql.caseSensitive", "true")
    df_detect = spark.read.json(case_file)

    columns = df_detect.columns
    lower_map: dict[str, list[str]] = {}
    for c in columns:
        lower_map.setdefault(c.lower(), []).append(c)

    conflicts = {k: v for k, v in lower_map.items() if len(v) > 1}
    if conflicts:
        for canonical, variants in conflicts.items():
            logger.warning("Case conflict for '%s': %s", canonical, variants)
        conflict_data = [(k, ", ".join(v)) for k, v in conflicts.items()]
        df_conflicts = spark.createDataFrame(conflict_data, ["canonical", "variants"])
        print_dataframe(df_conflicts, title="Detected case conflicts")
        print_warning("Resolve these before downstream processing")
    else:
        print_success("No case conflicts detected")

    spark.conf.set("spark.sql.caseSensitive", "false")

    # =========================================================================
    # 8. Best practice summary
    # =========================================================================
    print_header("8. Best Practices")

    practices = [
        ("1. Explicit schema", "Schema fields match case-insensitively by default"),
        ("2. Normalize early", "Lowercase all column names at bronze layer"),
        ("3. Detect conflicts", "Read with caseSensitive=true to find variants"),
        ("4. Coalesce variants", "Merge ID/Id/id into single 'id' column"),
        ("5. Document", "Record canonical field names in data contracts"),
    ]
    df_practices = spark.createDataFrame(practices, ["Practice", "Why"])
    print_dataframe(df_practices, title="Case Sensitivity Best Practices")
    print_success(
        "Keep caseSensitive=false (default) and use explicit schemas — "
        "Spark matches fields case-insensitively to your schema"
    )

    spark.stop()
