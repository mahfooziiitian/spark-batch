"""spark-excel option reference — every kind of distributed read/write in one place.

`01_spark_excel_distributed_io.py` shows the minimal happy path; this example
exercises the full range of `com.crealytics.spark.excel` options exposed by
`read_spark_excel()`/`write_spark_excel()` so you can see, side by side, how
each one changes the result:

Key concepts:
    - data_address: an unbounded starting cell (e.g. "'Sheet1'!A1", reads the
      whole sheet from there) vs. a bounded cell range (e.g. "'Sheet1'!A1:D4")
      vs. an offset starting cell for writes (e.g. "'Report'!C3")
    - header/infer_schema: header=False + infer_schema=False for raw,
      untyped reads (e.g. legacy exports with no header row)
    - options dict passthrough: connector-specific tuning such as
      maxRowsInMemory (streaming reader for huge workbooks), dateFormat,
      and timestampFormat
    - write modes: overwrite / append / ignore / error, and what each does
      when the destination already exists
    - reading a second/named sheet from the same multi-sheet workbook

Reference: https://github.com/crealytics/spark-excel#configuration
"""

from py4j.protocol import Py4JJavaError

from pys_excel import (
    generate_sample_workbook,
    output_path,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
)
from pys_excel.logs import get_logger
from pys_excel.spark_excel import (
    get_spark_with_excel_package,
    read_spark_excel,
    resolve_spark_excel_package,
    write_spark_excel,
)

set_log_level("DEBUG")
logger = get_logger("example.spark_excel_option_reference")


if __name__ == "__main__":
    print_header("Setup: SparkSession with the matching spark-excel package")
    print_path("Maven coordinate", resolve_spark_excel_package())
    try:
        spark = get_spark_with_excel_package(app_name="spark-excel-option-reference")
    except Exception as exc:
        print_warning(f"Could not initialize spark-excel (likely no network access): {exc}")
        raise SystemExit(0) from None

    workbook = generate_sample_workbook()
    print_path("Sample workbook", workbook)

    print_header("1. Whole-sheet read (data_address is a starting cell, not a bounded range)")
    df_whole = read_spark_excel(spark, workbook, data_address="'Employees'!A1")
    print_dataframe(df_whole, title="Employees (whole sheet from A1)")

    print_header("2. Bounded range read (data_address with A1:D4)")
    df_range = read_spark_excel(spark, workbook, data_address="'Employees'!A1:D4")
    print_dataframe(df_range, title="Employees!A1:D4 (3 data rows, 4 columns)")

    print_header("3. Read a different named sheet from the same workbook")
    df_departments = read_spark_excel(spark, workbook, data_address="'Departments'!A1")
    print_dataframe(df_departments, title="Departments")

    print_header("4. Raw read: header=False, infer_schema=False")
    df_raw = read_spark_excel(spark, workbook, data_address="'Employees'!A1", header=False, infer_schema=False)
    print_schema(df_raw, title="header=False, infer_schema=False -> _c0.._cN, all strings")
    print_dataframe(df_raw.limit(3), title="Raw rows (header row included as data)")

    print_header("5. Connector options passthrough: date/timestamp formats")
    df_formatted = read_spark_excel(
        spark,
        workbook,
        data_address="'Employees'!A1",
        options={"dateFormat": "yyyy-MM-dd", "timestampFormat": "yyyy-MM-dd HH:mm:ss"},
    )
    print_schema(df_formatted, title="hire_date parsed with an explicit timestampFormat")

    print_header("6. Streaming reader for huge workbooks: maxRowsInMemory")
    df_streamed = read_spark_excel(
        spark,
        workbook,
        data_address="'Employees'!A1",
        options={"maxRowsInMemory": 100},
    )
    print_success(f"Read {df_streamed.count()} row(s) with maxRowsInMemory=100 (streaming SAX reader)")

    print_header("7. Write modes: overwrite -> append -> ignore -> error")
    top3_path = output_path("spark_excel_top3.xlsx")

    write_spark_excel(df_whole.limit(3), top3_path, sheet_name="Top3", mode="overwrite")
    print_success(f"overwrite: wrote 3 row(s) to {top3_path}")

    # Unlike partitioned sinks (Parquet/Delta), a single .xlsx file cannot be
    # incrementally appended to — spark-excel's "append" mode leaves an
    # existing file untouched (same as "ignore"), it does not merge rows.
    write_spark_excel(df_whole.limit(2), top3_path, sheet_name="Top3", mode="append")
    after_append = read_spark_excel(spark, top3_path, data_address="'Top3'!A1")
    print_success(f"append: {after_append.count()} row(s) present — unchanged (no true append for a single Excel file)")

    write_spark_excel(df_whole.limit(5), top3_path, sheet_name="Top3", mode="ignore")
    after_ignore = read_spark_excel(spark, top3_path, data_address="'Top3'!A1")
    print_success(f"ignore: row count unchanged at {after_ignore.count()} (existing file left as-is)")

    try:
        write_spark_excel(df_whole.limit(1), top3_path, sheet_name="Top3", mode="error")
        print_warning("error: expected a failure since the file already exists")
    except Py4JJavaError as exc:
        print_success(f"error: raised as expected — {exc.java_exception}")

    print_header("8. Write starting at an offset cell (dataAddress != A1)")
    offset_path = output_path("spark_excel_offset_report.xlsx")
    write_spark_excel(
        df_range, offset_path, sheet_name="Report", mode="overwrite", options={"dataAddress": "'Report'!C3"}
    )
    df_offset = read_spark_excel(spark, offset_path, data_address="'Report'!C3")
    print_dataframe(df_offset, title="Read back from the same C3 offset")

    spark.stop()
