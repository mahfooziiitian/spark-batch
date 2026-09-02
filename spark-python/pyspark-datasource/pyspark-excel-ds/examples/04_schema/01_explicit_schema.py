"""Explicit schema via StructType and DDL strings — avoiding inference surprises.

Key concepts:
    - Excel cells are loosely typed; pandas/Spark inference can guess wrong
      (e.g. an ID column with leading zeros becomes an integer and loses them)
    - with_schema() pins the exact Spark types you expect
"""

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from pys_excel import (
    ExcelReader,
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    set_log_level,
    temp_excel_path,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.explicit_schema")


if __name__ == "__main__":
    import pandas as pd

    spark = get_spark("excel-explicit-schema")

    workbook = temp_excel_path("explicit_schema_demo")
    pd.DataFrame({"emp_id": ["007", "042"], "name": ["Alice", "Bob"], "salary": [95000.5, 72000.0]}).to_excel(
        workbook, index=False, engine="openpyxl"
    )

    print_header("1. Without a schema, emp_id loses its leading zeros")
    inferred = ExcelReader(spark).read(workbook)
    print_dataframe(inferred, title="Inferred")

    print_header("2. With an explicit StringType schema, leading zeros are preserved")
    schema = StructType(
        [
            StructField("emp_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("salary", DoubleType(), True),
        ]
    )
    explicit = ExcelReader(spark).with_schema(schema).dtype({"emp_id": str}).read(workbook)
    print_schema(explicit)
    print_dataframe(explicit, title="Explicit Schema")

    print_header("3. DDL string schema (equivalent, more concise)")
    ddl = (
        ExcelReader(spark)
        .with_schema("emp_id STRING, name STRING, salary DOUBLE")
        .dtype({"emp_id": str})
        .read(workbook)
    )
    print_schema(ddl)

    spark.stop()
