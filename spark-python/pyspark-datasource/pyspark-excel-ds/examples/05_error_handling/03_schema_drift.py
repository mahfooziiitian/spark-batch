"""Schema drift in recurring Excel extracts — added, renamed, and reordered columns.

Key concepts:
    - Business users routinely add new columns, rename existing ones, or
      reorder columns between monthly submissions of the "same" template
    - Reading by header name (not position) already tolerates reordering —
      the real risk is renamed/added/missing columns silently breaking
      downstream `unionByName`/table appends
    - Diff the incoming DataFrame's columns against an expected schema before
      writing: apply known rename aliases, null-fill missing expected
      columns, and quarantine genuinely unexpected new columns
"""

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pys_excel import (
    ExcelReader,
    get_spark,
    print_dataframe,
    print_header,
    print_warning,
    set_log_level,
    temp_excel_path,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.schema_drift")

EXPECTED_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", StringType(), True),
    ]
)

# Known historical rename aliases: old header name -> canonical expected name
KNOWN_RENAMES = {"dept": "department"}


def reconcile_schema(df: DataFrame) -> DataFrame:
    """Align an incoming DataFrame to EXPECTED_SCHEMA, tolerating drift."""
    expected_cols = [f.name for f in EXPECTED_SCHEMA.fields]

    for old, new in KNOWN_RENAMES.items():
        if old in df.columns and new not in df.columns:
            logger.info("Applying known rename alias: %s -> %s", old, new)
            df = df.withColumnRenamed(old, new)

    incoming_cols = set(df.columns)
    missing = [c for c in expected_cols if c not in incoming_cols]
    unexpected = [c for c in df.columns if c not in expected_cols]

    if missing:
        print_warning(f"Missing expected column(s), filling with null: {missing}")
        for col in missing:
            df = df.withColumn(col, F.lit(None).cast("string"))

    if unexpected:
        print_warning(f"Quarantining unexpected/new column(s) not in target schema: {unexpected}")
        df = df.drop(*unexpected)

    return df.select(*expected_cols)


if __name__ == "__main__":
    spark = get_spark("excel-schema-drift")

    print_header("1. March extract — matches the expected template")
    march_path = temp_excel_path("march_extract")
    pd.DataFrame(
        {
            "emp_id": ["001", "002"],
            "name": ["Alice", "Bob"],
            "department": ["Engineering", "Sales"],
            "salary": ["95000", "82000"],
        }
    ).to_excel(march_path, index=False, engine="openpyxl")
    march_df = ExcelReader(spark).read(march_path)
    print_dataframe(march_df, title="March (baseline)")

    print_header("2. April extract — 'department' renamed to 'dept', new 'email' column, reordered")
    april_path = temp_excel_path("april_extract")
    pd.DataFrame(
        {
            "name": ["Carol", "Dave"],
            "email": ["carol@example.com", "dave@example.com"],
            "emp_id": ["003", "004"],
            "dept": ["Marketing", "Engineering"],
            "salary": ["77000", "91000"],
        }
    ).to_excel(april_path, index=False, engine="openpyxl")
    april_df = ExcelReader(spark).read(april_path)
    print_dataframe(april_df, title="April (drifted)")

    print_header("3. Reconcile April against the expected schema")
    reconciled = reconcile_schema(april_df)
    print_dataframe(reconciled, title="April (reconciled)")

    print_header("4. Combined, schema-aligned dataset")
    combined = march_df.select(*[f.name for f in EXPECTED_SCHEMA.fields]).unionByName(reconciled)
    print_dataframe(combined, title="March + April")

    spark.stop()
