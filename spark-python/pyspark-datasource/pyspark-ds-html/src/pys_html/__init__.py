"""PySpark HTML Datasource library — reusable utilities for reading, writing, and parsing HTML."""

from pys_html._logging import (
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
from pys_html.cli import (
    add_multi_source_args,
    add_parser_arg,
    add_search_args,
    add_source_args,
    build_arg_parser,
    parse_attrs,
    print_args,
    resolve_html_source,
    resolve_html_sources,
)
from pys_html.config import (
    DATA_HOME,
    PROJECT_ROOT,
    configure_env,
    data_path,
    get_spark,
    get_spark_connect,
    output_path,
    temp_html_path,
    write_html_file,
)
from pys_html.i18n import bilingual_header, has_devanagari, transliterate_devanagari
from pys_html.reader import HtmlReader
from pys_html.search import SCORERS, semantic_search
from pys_html.session import create_spark_session
from pys_html.writer import HtmlWriter

__all__ = [
    "DATA_HOME",
    "PROJECT_ROOT",
    "SCORERS",
    "HtmlReader",
    "HtmlWriter",
    "add_multi_source_args",
    "add_parser_arg",
    "add_search_args",
    "add_source_args",
    "bilingual_header",
    "build_arg_parser",
    "configure_env",
    "console",
    "create_spark_session",
    "data_path",
    "get_logger",
    "get_spark",
    "get_spark_connect",
    "has_devanagari",
    "output_path",
    "parse_attrs",
    "print_args",
    "print_dataframe",
    "print_error",
    "print_header",
    "print_path",
    "print_schema",
    "print_success",
    "print_warning",
    "resolve_html_source",
    "resolve_html_sources",
    "semantic_search",
    "set_log_level",
    "temp_html_path",
    "transliterate_devanagari",
    "write_html_file",
]
