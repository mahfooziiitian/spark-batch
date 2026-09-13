"""Databricks built-in Excel format (DBR 17.1+) — no library install required.

Key concepts:
    - The native ``excel`` data source format is only available on
      **Databricks Runtime 17.1+** — it ships in the runtime, so no Maven
      library (crealytics or otherwise) needs to be attached to the cluster
    - Uses ``headerRows``/``dataAddress`` options (not ``header``/
      ``inferSchema`` like the crealytics connector) — `read_spark_excel()`/
      `write_spark_excel()` translate these automatically when
      ``excel_format=NATIVE_EXCEL_FORMAT`` (or when `resolve_excel_format()`
      auto-selects it for you on a qualifying runtime)
    - This example forces ``excel_format=NATIVE_EXCEL_FORMAT`` to demonstrate
      the format explicitly; in practice, prefer letting
      `resolve_excel_format()` choose automatically so the same code also
      runs correctly on older runtimes/local Spark

Running this example:
    - **On Databricks Runtime 17.1+**: run as-is, in a notebook or job.
    - **Everywhere else** (local Spark, DBR < 17.1): the ``excel`` format
      isn't available, so this script detects that and skips gracefully.

Reference: https://docs.databricks.com/en/query/formats/excel.html
"""

from pys_excel import (
    generate_sample_workbook,
    get_spark,
    output_path,
    print_dataframe,
    print_header,
    print_path,
    print_success,
    print_warning,
    set_log_level,
)
from pys_excel.logs import get_logger
from pys_excel.spark_excel import (
    NATIVE_EXCEL_FORMAT,
    NATIVE_EXCEL_MIN_DBR,
    is_databricks_runtime,
    read_spark_excel,
    write_spark_excel,
)

set_log_level("DEBUG")
logger = get_logger("example.native_excel_format")


def _dbr_version_tuple(dbr: str) -> tuple[int, int]:
    try:
        major, minor = (int(p) for p in dbr.split(".")[:2])
    except ValueError:
        return (0, 0)
    return (major, minor)


if __name__ == "__main__":
    dbr = is_databricks_runtime()
    if dbr is None:
        print_warning(
            "Not running on Databricks — the native 'excel' format requires Databricks Runtime "
            f"{NATIVE_EXCEL_MIN_DBR[0]}.{NATIVE_EXCEL_MIN_DBR[1]}+. Skipping. "
            "See crealytics_local/ for a local equivalent."
        )
        raise SystemExit(0)
    if _dbr_version_tuple(dbr) < NATIVE_EXCEL_MIN_DBR:
        print_warning(
            f"Databricks Runtime {dbr} is below {NATIVE_EXCEL_MIN_DBR[0]}.{NATIVE_EXCEL_MIN_DBR[1]}; "
            "the native 'excel' format isn't available. Skipping. See crealytics_databricks/ instead."
        )
        raise SystemExit(0)

    print_header(f"Databricks Runtime {dbr} detected — using the native '{NATIVE_EXCEL_FORMAT}' format")
    spark = get_spark(app_name="databricks-native-excel-format")

    workbook = generate_sample_workbook()
    print_path("Sample workbook", workbook)

    print_header("1. Read via the built-in format (no library attachment needed)")
    df = read_spark_excel(spark, workbook, data_address="'Employees'!A1", excel_format=NATIVE_EXCEL_FORMAT)
    print_dataframe(df, title="Employees")

    print_header("2. Write via the same built-in format")
    out_path = output_path("native_excel_output.xlsx")
    write_spark_excel(df.limit(3), out_path, sheet_name="Top3", mode="overwrite", excel_format=NATIVE_EXCEL_FORMAT)
    print_success(f"Wrote 3 row(s) to {out_path} via '{NATIVE_EXCEL_FORMAT}'")

    spark.stop()
