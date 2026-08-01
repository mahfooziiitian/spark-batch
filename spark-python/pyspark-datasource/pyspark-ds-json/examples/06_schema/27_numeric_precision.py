"""Numeric precision issues — handling large numbers and high-precision decimals in JSON.

Demonstrates precision loss with LongType and DoubleType, and safe approaches
using STRING and DECIMAL types for identifiers and financial data.

Key concepts:
    - JSON numbers exceeding Long.MAX_VALUE (9223372036854775807) overflow
    - DoubleType has ~15-17 significant digits — precision loss for financial data
    - Identifiers should ALWAYS be STRING (even if they look numeric)
    - DECIMAL(38,18) preserves exact precision for monetary amounts
    - prefersDecimal option infers decimals instead of doubles
    - primitivesAsString avoids all numeric coercion

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
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
logger = get_logger("example.numeric_precision")


if __name__ == "__main__":
    spark = get_spark("numeric-precision")

    # =========================================================================
    # 1. The problem — precision loss with inference
    # =========================================================================
    print_header("1. Precision Loss with Type Inference")

    precision_file = DATA_HOME + "/numeric_precision.json"
    write_json_lines(
        precision_file,
        [
            '{"transaction_id": 9999999999999999999, "amount": 1234567890.123456789}',
            '{"transaction_id": 1234567890123456789, "amount": 0.1}',
            '{"transaction_id": 99, "amount": 99999999999999.99}',
        ],
    )
    print_path("Input", precision_file)

    # Let Spark infer types
    df_inferred = spark.read.json(precision_file)
    print_schema(df_inferred, title="Inferred schema")
    print_dataframe(df_inferred, title="Inferred types — check for precision loss")
    print_warning(
        "Large integers may overflow LongType (max 9223372036854775807). "
        "Decimals lose precision with DoubleType (~15 significant digits)."
    )

    # =========================================================================
    # 2. LongType overflow
    # =========================================================================
    print_header("2. LongType Overflow")

    overflow_file = DATA_HOME + "/numeric_overflow.json"
    write_json_lines(
        overflow_file,
        [
            '{"id": 9223372036854775807}',
            '{"id": 9223372036854775808}',
            '{"id": 99999999999999999999}',
        ],
    )

    long_schema = StructType([StructField("id", LongType(), True)])
    df_long = spark.read.schema(long_schema).option("mode", "PERMISSIVE").json(overflow_file)
    print_dataframe(df_long, title="LongType — values exceeding MAX overflow to null")

    string_schema = StructType([StructField("id", StringType(), True)])
    df_string = spark.read.schema(string_schema).json(overflow_file)
    print_dataframe(df_string, title="StringType — all values preserved exactly")
    print_success("Use STRING for IDs that may exceed Long.MAX_VALUE (2^63 - 1)")

    # =========================================================================
    # 3. DoubleType precision loss
    # =========================================================================
    print_header("3. DoubleType Precision Loss")

    double_file = DATA_HOME + "/numeric_double.json"
    write_json_lines(
        double_file,
        [
            '{"amount": 1234567890.123456789}',
            '{"amount": 0.1}',
            '{"amount": 0.2}',
            '{"amount": 99999999999999.99}',
            '{"amount": 123456789012345.678}',
        ],
    )

    double_schema = StructType([StructField("amount", DoubleType(), True)])
    df_double = spark.read.schema(double_schema).json(double_file)

    # Show precision loss
    df_double_show = df_double.withColumn(
        "amount_str", F.format_string("%.18f", F.col("amount"))
    )
    print_dataframe(df_double_show, title="DoubleType — note precision loss in trailing digits")
    print_warning(
        "DoubleType (IEEE 754) has ~15-17 significant digits. "
        "1234567890.123456789 becomes 1234567890.123456716..."
    )

    # =========================================================================
    # 4. DECIMAL for exact precision
    # =========================================================================
    print_header("4. DECIMAL — Exact Precision")

    decimal_schema = StructType([StructField("amount", DecimalType(38, 18), True)])
    df_decimal = spark.read.schema(decimal_schema).json(double_file)
    print_dataframe(df_decimal, title="DECIMAL(38,18) — exact values preserved")
    print_success("DECIMAL(precision, scale) preserves exact numeric values — use for financial data")

    # =========================================================================
    # 5. Safe schema for financial data
    # =========================================================================
    print_header("5. Safe Schema for Financial Data")

    finance_file = DATA_HOME + "/numeric_finance.json"
    write_json_lines(
        finance_file,
        [
            '{"txn_id": "TXN-9999999999999999999", "amount": 1500.50, "fee": 0.015, "balance": 99999999.99}',
            '{"txn_id": "TXN-1234567890123456789", "amount": 250000.00, "fee": 0.025, "balance": 100250000.99}',
        ],
    )

    # Safe schema: IDs as STRING, amounts as DECIMAL
    safe_schema = StructType(
        [
            StructField("txn_id", StringType(), True),
            StructField("amount", DecimalType(18, 2), True),
            StructField("fee", DecimalType(10, 6), True),
            StructField("balance", DecimalType(18, 2), True),
        ]
    )

    df_finance = spark.read.schema(safe_schema).json(finance_file)
    print_schema(df_finance, title="Safe financial schema")
    print_dataframe(df_finance, title="Financial data — exact precision")

    # Arithmetic is exact with DECIMAL
    df_calc = df_finance.withColumn(
        "fee_amount", (F.col("amount") * F.col("fee")).cast(DecimalType(18, 2))
    )
    print_dataframe(df_calc.select("txn_id", "amount", "fee", "fee_amount"), title="Exact arithmetic")
    print_success("DECIMAL arithmetic is exact — no floating-point surprises")

    # =========================================================================
    # 6. prefersDecimal option
    # =========================================================================
    print_header("6. prefersDecimal Option")

    df_prefer = spark.read.option("prefersDecimal", "true").json(double_file)
    print_schema(df_prefer, title="With prefersDecimal=true")
    print_dataframe(df_prefer, title="Inferred as DecimalType")
    print_success("prefersDecimal=true infers floating-point as DecimalType instead of DoubleType")

    # =========================================================================
    # 7. primitivesAsString — nuclear option
    # =========================================================================
    print_header("7. primitivesAsString — Read All as String")

    df_strings = spark.read.option("primitivesAsString", "true").json(precision_file)
    print_schema(df_strings, title="primitivesAsString=true")
    print_dataframe(df_strings, title="All values as strings — zero precision loss")
    print_warning(
        "primitivesAsString preserves everything but requires explicit casting later"
    )

    # =========================================================================
    # 8. Comparison: Double vs Decimal arithmetic
    # =========================================================================
    print_header("8. Double vs Decimal Arithmetic")

    # Classic floating-point problem: 0.1 + 0.2 != 0.3
    arith_data = [("0.1 + 0.2",)]
    df_arith = spark.createDataFrame(arith_data, ["expression"])

    df_comparison = df_arith.select(
        F.lit(0.1).cast("double").alias("a_double"),
        F.lit(0.2).cast("double").alias("b_double"),
        (F.lit(0.1).cast("double") + F.lit(0.2).cast("double")).alias("sum_double"),
        F.lit("0.1").cast(DecimalType(38, 18)).alias("a_decimal"),
        F.lit("0.2").cast(DecimalType(38, 18)).alias("b_decimal"),
        (F.lit("0.1").cast(DecimalType(38, 18)) + F.lit("0.2").cast(DecimalType(38, 18))).alias("sum_decimal"),
    )
    print_dataframe(df_comparison, title="0.1 + 0.2: Double vs Decimal")
    print_success(
        "Double: 0.1 + 0.2 = 0.30000000000000004. "
        "Decimal: 0.1 + 0.2 = 0.3 exactly."
    )

    # =========================================================================
    # 9. Decision guide
    # =========================================================================
    print_header("9. Type Decision Guide")

    guide = [
        ("Identifiers (IDs, codes)", "STRING", "Never overflow, exact match"),
        ("Monetary amounts", "DECIMAL(18,2)", "Exact arithmetic, no rounding"),
        ("High-precision rates", "DECIMAL(38,18)", "Maximum precision"),
        ("Scientific measurements", "DOUBLE", "Range more important than precision"),
        ("Counters (< 2^63)", "BIGINT (LongType)", "Fast integer math"),
        ("Unknown/mixed numbers", "STRING", "Parse later with explicit casting"),
    ]
    df_guide = spark.createDataFrame(guide, ["Use Case", "Type", "Why"])
    print_dataframe(df_guide, title="Numeric Type Decision Guide")
    print_success(
        "Rule: IDs → STRING, money → DECIMAL, science → DOUBLE, counters → BIGINT"
    )

    spark.stop()
