"""PySpark JSON Datasource library — reusable utilities for reading, writing, and parsing JSON."""

from pys_json._logging import (
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
from pys_json.config import (
    DATA_HOME,
    PROJECT_ROOT,
    configure_env,
    data_path,
    get_spark,
    get_spark_connect,
    output_path,
    temp_json_path,
    write_json_file,
    write_json_lines,
)
from pys_json.reader import JsonReader
from pys_json.session import create_spark_session
from pys_json.writer import COMPRESSION_CODECS, JsonWriter

__all__ = [
    "COMPRESSION_CODECS",
    "DATA_HOME",
    "PROJECT_ROOT",
    "JsonReader",
    "JsonWriter",
    "configure_env",
    "console",
    "create_spark_session",
    "data_path",
    "get_logger",
    "get_spark",
    "get_spark_connect",
    "output_path",
    "print_dataframe",
    "print_error",
    "print_header",
    "print_path",
    "print_schema",
    "print_success",
    "print_warning",
    "set_log_level",
    "temp_json_path",
    "write_json_file",
    "write_json_lines",
]
