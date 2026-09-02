"""Read Excel with an explicit Spark schema, skipping type inference.

Key concepts:
    - with_schema() accepts a StructType or a DDL string
    - Explicit schemas avoid pandas' type-inference surprises (e.g. mixed int/float columns)
"""

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from pys_excel import (
    ExcelReader,
    generate_sample_workbook,
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    set_log_level,
)
from pys_excel._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.read_with_schema")


if __name__ == "__main__":
    spark = get_spark("read-excel-with-schema")

    workbook = generate_sample_workbook()

    print_header("1. Explicit StructType schema")
    schema = StructType(
        [
            StructField("emp_id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", DoubleType(), True),
            StructField("hire_date", StringType(), True),
        ]
    )
    df = ExcelReader(spark).sheet("Employees").with_schema(schema).read(workbook)
    print_schema(df, title="Explicit Schema")
    print_dataframe(df, title="Employees")

    print_header("2. DDL string schema")
    df2 = (
        ExcelReader(spark)
        .sheet("Departments")
        .with_schema("department STRING, manager STRING, budget LONG")
        .read(workbook)
    )
    print_schema(df2, title="DDL Schema")

    spark.stop()
