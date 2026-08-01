"""Bad JSON that looks almost valid — handling non-standard JSON with Spark options.

Demonstrates how to read JSON that violates the strict spec but is common in
real-world data: Python-style booleans/None, unquoted field names, leading zeros,
single quotes, trailing commas, comments, and NaN/Infinity.

Key concepts:
    - allowUnquotedFieldNames: {id: 1} instead of {"id": 1}
    - allowNumericLeadingZeros: {"id": 001}
    - allowSingleQuotes: {'name': 'Alice'}
    - allowNonNumericNumbers: NaN, Infinity, -Infinity
    - allowBackslashEscapingAnyCharacter: non-standard escapes
    - allowComments: // and /* */ in JSON
    - Python-style True/False/None requires pre-processing (not directly supported)

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
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
logger = get_logger("example.bad_json")


if __name__ == "__main__":
    import os

    spark = get_spark("bad-json")

    # =========================================================================
    # 1. Unquoted field names
    # =========================================================================
    print_header("1. Unquoted Field Names")

    unquoted_file = DATA_HOME + "/bad_json_unquoted.json"
    # Write raw (not via write_json_lines which might escape)
    with open(unquoted_file, "w") as f:
        f.write('{id: 1, name: "Alice"}\n')
        f.write('{id: 2, name: "Bob"}\n')
    print_path("Input (unquoted keys)", unquoted_file)

    # Without option — produces corrupt records
    fail_schema = "id BIGINT, name STRING, _corrupt_record STRING"
    df_fail = spark.read.schema(fail_schema).json(unquoted_file).cache()
    corrupt_count = df_fail.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.warning("Without option: %s/%s records corrupt", corrupt_count, df_fail.count())
    df_fail.unpersist()

    # With option — works
    df_unquoted = spark.read.option("allowUnquotedFieldNames", "true").json(unquoted_file)
    print_dataframe(df_unquoted, title="With allowUnquotedFieldNames=true")
    print_success("allowUnquotedFieldNames handles {id: 1} without quotes on keys")

    # =========================================================================
    # 2. Leading zeros in numbers
    # =========================================================================
    print_header("2. Leading Zeros")

    zeros_file = DATA_HOME + "/bad_json_zeros.json"
    with open(zeros_file, "w") as f:
        f.write('{"id": 001, "code": 007, "value": 042}\n')
        f.write('{"id": 010, "code": 099, "value": 100}\n')
    print_path("Input (leading zeros)", zeros_file)

    # Without option
    zeros_fail_schema = "id BIGINT, code BIGINT, value BIGINT, _corrupt_record STRING"
    df_zeros_fail = spark.read.schema(zeros_fail_schema).json(zeros_file).cache()
    corrupt_zeros = df_zeros_fail.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.warning("Without option: %s/%s records corrupt", corrupt_zeros, df_zeros_fail.count())
    df_zeros_fail.unpersist()

    # With option
    df_zeros = spark.read.option("allowNumericLeadingZeros", "true").json(zeros_file)
    print_dataframe(df_zeros, title="With allowNumericLeadingZeros=true")
    print_success("Leading zeros parsed as regular integers: 001 → 1, 007 → 7")

    # =========================================================================
    # 3. Single quotes
    # =========================================================================
    print_header("3. Single Quotes")

    single_file = DATA_HOME + "/bad_json_single_quotes.json"
    with open(single_file, "w") as f:
        f.write("{'name': 'Alice', 'city': 'New York'}\n")
        f.write("{'name': 'Bob', 'city': 'Los Angeles'}\n")
    print_path("Input (single quotes)", single_file)

    df_single = spark.read.option("allowSingleQuotes", "true").json(single_file)
    print_dataframe(df_single, title="With allowSingleQuotes=true")
    print_success("allowSingleQuotes handles Python/JavaScript-style single-quoted strings")

    # =========================================================================
    # 4. NaN, Infinity, -Infinity
    # =========================================================================
    print_header("4. Non-Numeric Numbers (NaN, Infinity)")

    nan_file = DATA_HOME + "/bad_json_nan.json"
    with open(nan_file, "w") as f:
        f.write('{"id": 1, "value": NaN}\n')
        f.write('{"id": 2, "value": Infinity}\n')
        f.write('{"id": 3, "value": -Infinity}\n')
        f.write('{"id": 4, "value": 42.5}\n')
    print_path("Input (NaN/Infinity)", nan_file)

    df_nan = spark.read.option("allowNonNumericNumbers", "true").json(nan_file)
    print_schema(df_nan, title="Schema with NaN/Infinity")
    print_dataframe(df_nan, title="With allowNonNumericNumbers=true")

    # Filter and handle NaN
    df_clean = df_nan.withColumn(
        "value_clean",
        F.when(F.isnan(F.col("value")), F.lit(None))
        .when(F.col("value") == float("inf"), F.lit(None))
        .when(F.col("value") == float("-inf"), F.lit(None))
        .otherwise(F.col("value")),
    )
    print_dataframe(df_clean, title="After cleaning NaN/Infinity → null")
    print_success("allowNonNumericNumbers parses NaN/Infinity; clean them with isnan() + filters")

    # =========================================================================
    # 5. Comments in JSON
    # =========================================================================
    print_header("5. Comments in JSON")

    comment_file = DATA_HOME + "/bad_json_comments.json"
    with open(comment_file, "w") as f:
        f.write('// This is a comment\n')
        f.write('{"id": 1, "name": "Alice"}\n')
        f.write('/* Multi-line comment */\n')
        f.write('{"id": 2, "name": "Bob"}\n')
    print_path("Input (with comments)", comment_file)

    df_comments = spark.read.option("allowComments", "true").json(comment_file)
    print_dataframe(df_comments, title="With allowComments=true")
    print_success("allowComments ignores // and /* */ comment lines")

    # =========================================================================
    # 6. Python-style True/False/None (requires pre-processing)
    # =========================================================================
    print_header("6. Python-Style Booleans (True/False/None)")

    python_file = DATA_HOME + "/bad_json_python.json"
    with open(python_file, "w") as f:
        f.write('{"active": True, "value": None, "name": "Alice"}\n')
        f.write('{"active": False, "value": None, "name": "Bob"}\n')
        f.write('{"active": True, "value": 42, "name": "Charlie"}\n')
    print_path("Input (Python-style)", python_file)

    # Direct read fails — True/False/None are not valid JSON
    python_fail_schema = "active STRING, name STRING, value STRING, _corrupt_record STRING"
    df_python_fail = spark.read.schema(python_fail_schema).json(python_file).cache()
    corrupt_py = df_python_fail.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.warning("Python-style JSON: %s/%s records corrupt", corrupt_py, df_python_fail.count())
    df_python_fail.unpersist()
    print_warning("True/False/None are NOT valid JSON — no Spark option fixes this")

    # Solution: pre-process with text replacement
    raw_df = spark.read.text(python_file)
    fixed_df = raw_df.select(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("value"), r"\bTrue\b", "true"),
                r"\bFalse\b",
                "false",
            ),
            r"\bNone\b",
            "null",
        ).alias("value")
    )
    print_dataframe(fixed_df, title="After text replacement (True→true, None→null)")

    # Parse the fixed JSON
    python_schema = StructType(
        [
            StructField("active", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("value", LongType(), True),
        ]
    )
    df_python = spark.read.schema(python_schema).json(
        fixed_df.select("value").rdd.map(lambda r: r[0])
    )
    print_dataframe(df_python, title="Parsed after pre-processing")
    print_success("Python-style JSON needs text pre-processing: True→true, False→false, None→null")

    # =========================================================================
    # 7. Trailing commas
    # =========================================================================
    print_header("7. Trailing Commas")

    trailing_file = DATA_HOME + "/bad_json_trailing.json"
    with open(trailing_file, "w") as f:
        f.write('{"id": 1, "name": "Alice",}\n')
        f.write('{"id": 2, "name": "Bob",}\n')
    print_path("Input (trailing commas)", trailing_file)

    # Spark doesn't have a native option for trailing commas
    trailing_fail_schema = "id BIGINT, name STRING, _corrupt_record STRING"
    df_trailing_fail = spark.read.schema(trailing_fail_schema).json(trailing_file).cache()
    corrupt_trail = df_trailing_fail.filter(F.col("_corrupt_record").isNotNull()).count()
    logger.warning("Trailing commas: %s/%s records corrupt", corrupt_trail, df_trailing_fail.count())
    df_trailing_fail.unpersist()

    # Fix with regex
    raw_trailing = spark.read.text(trailing_file)
    fixed_trailing = raw_trailing.select(
        F.regexp_replace(F.col("value"), r",\s*}", "}").alias("value")
    )
    df_trailing = spark.read.json(fixed_trailing.select("value").rdd.map(lambda r: r[0]))
    print_dataframe(df_trailing, title="After removing trailing commas")
    print_success("Trailing commas need regex pre-processing: ',}' → '}'")

    # =========================================================================
    # 8. Combining multiple options
    # =========================================================================
    print_header("8. Combining Multiple Relaxed Options")

    combo_file = DATA_HOME + "/bad_json_combo.json"
    with open(combo_file, "w") as f:
        f.write("// Configuration data\n")
        f.write("{id: 001, 'name': 'Alice', score: NaN}\n")
        f.write("{id: 002, 'name': 'Bob', score: 95.5}\n")
    print_path("Input (multiple issues)", combo_file)

    df_combo = (
        spark.read.option("allowUnquotedFieldNames", "true")
        .option("allowNumericLeadingZeros", "true")
        .option("allowSingleQuotes", "true")
        .option("allowNonNumericNumbers", "true")
        .option("allowComments", "true")
        .json(combo_file)
    )
    print_dataframe(df_combo, title="All relaxed options combined")
    print_success("Stack multiple options for heavily non-standard JSON sources")

    # =========================================================================
    # 9. Options reference
    # =========================================================================
    print_header("9. Non-Standard JSON Options Reference")

    options = [
        ("allowUnquotedFieldNames", "{id: 1}", "Spark option"),
        ("allowNumericLeadingZeros", '{"id": 001}', "Spark option"),
        ("allowSingleQuotes", "{'k': 'v'}", "Spark option"),
        ("allowNonNumericNumbers", "NaN, Infinity", "Spark option"),
        ("allowComments", "// comment", "Spark option"),
        ("allowBackslashEscapingAnyCharacter", "\\x escape", "Spark option"),
        ("True/False/None", "Python booleans", "Pre-process text"),
        ("Trailing commas", '{"k": "v",}', "Pre-process text"),
    ]
    df_options = spark.createDataFrame(options, ["Issue", "Example", "Fix"])
    print_dataframe(df_options, title="Non-Standard JSON — Options vs Pre-Processing")
    print_success(
        "Spark options handle many non-standard cases. "
        "Python-style and trailing commas need text pre-processing."
    )

    spark.stop()
