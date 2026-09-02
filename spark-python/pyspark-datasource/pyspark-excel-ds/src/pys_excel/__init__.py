"""PySpark Excel Datasource library — reusable utilities for reading, writing,
and table-integrating Excel workbooks with Apache Spark / Databricks.
"""

from pys_excel._logging import (
    console,
    get_logger,
    print_dataframe,
    print_error,
    print_header,
    print_path,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
)
from pys_excel.config import (
    DATA_HOME,
    PROJECT_ROOT,
    configure_env,
    data_path,
    generate_sample_workbook,
    get_spark,
    get_spark_connect,
    output_path,
    temp_excel_path,
)
from pys_excel.reader import ExcelReader
from pys_excel.session import create_spark_session
from pys_excel.spark_excel import (
    CREALYTICS_EXCEL_FORMAT,
    NATIVE_EXCEL_FORMAT,
    SPARK_EXCEL_PACKAGE_SCALA_2_12,
    get_spark_with_excel_package,
    is_databricks_runtime,
    read_spark_excel,
    resolve_excel_format,
    write_spark_excel,
)
from pys_excel.table import excel_to_table, table_to_excel, upsert_table_from_excel
from pys_excel.writer import ExcelWriter

__all__ = [
    "CREALYTICS_EXCEL_FORMAT",
    "DATA_HOME",
    "NATIVE_EXCEL_FORMAT",
    "PROJECT_ROOT",
    "SPARK_EXCEL_PACKAGE_SCALA_2_12",
    "ExcelReader",
    "ExcelWriter",
    "configure_env",
    "console",
    "create_spark_session",
    "data_path",
    "excel_to_table",
    "generate_sample_workbook",
    "get_logger",
    "get_spark",
    "get_spark_connect",
    "get_spark_with_excel_package",
    "is_databricks_runtime",
    "output_path",
    "print_dataframe",
    "print_error",
    "print_header",
    "print_path",
    "print_schema",
    "print_success",
    "print_warning",
    "read_spark_excel",
    "resolve_excel_format",
    "set_log_level",
    "table_to_excel",
    "temp_excel_path",
    "upsert_table_from_excel",
    "write_spark_excel",
]
