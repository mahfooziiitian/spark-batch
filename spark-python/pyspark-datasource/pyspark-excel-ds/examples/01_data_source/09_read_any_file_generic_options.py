"""Read any Excel workbook with a generic, reusable options dict.

Key concepts:
    - with_options() applies an arbitrary dict of pandas.read_excel kwargs at
      once, so a single reader function can be reused across unrelated
      workbooks/callers without hardcoding sheet/header/etc. per file.
    - A generic ``read_excel_generic()`` helper accepts any path plus a
      free-form options mapping — useful for config-driven ingestion where
      the options come from a job parameter, YAML/JSON config, or CLI args.
    - Unknown/extra keys are passed straight through to pandas, so the same
      helper keeps working as new pandas.read_excel options are needed.
    - An argparse CLI (build_arg_parser()) lets any workbook be read from the
      command line with the same generic options, without editing the script.
    - ``--mode`` chooses which *library/backend* to use at runtime, without
      editing the script: ``pandas`` (the driver-collected ExcelReader bridge,
      default) or ``distributed`` (the cluster-scale ``spark-excel``
      connector via :func:`pys_excel.read_spark_excel`). Both paths share the
      same ``--path``/glob/directory resolution and CLI options where they
      overlap (sheet, header).

Run:
    # Built-in demo (generates a sample workbook, runs 5 example sections)
    python 09_read_any_file_generic_options.py

    # Read an arbitrary workbook via CLI options (pandas backend, default)
    python 09_read_any_file_generic_options.py --path /data/report.xlsx \\
        --sheet Employees --header 0 --skiprows 1 --usecols A:C \\
        --na-values "N/A,n/a" --dtype "emp_id=str" --engine openpyxl

    # --path also accepts a glob pattern or a directory (all .xlsx/.xls/.xlsm
    # files in it), reading each match with the same options
    python 09_read_any_file_generic_options.py --path "/data/excel/*.xlsx"
    python 09_read_any_file_generic_options.py --path /data/excel/

    # Switch to the distributed spark-excel connector at runtime — no code
    # changes, just --mode distributed (auto-loads the Maven package locally;
    # on Databricks the cluster-installed/native connector is used instead)
    python 09_read_any_file_generic_options.py --mode distributed \\
        --path /data/report.xlsx --sheet Employees --data-address "'Employees'!A1:E100"
"""

import argparse
import glob as glob_module
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from pys_excel import (
    ExcelReader,
    generate_sample_workbook,
    get_spark,
    get_spark_with_excel_package,
    is_databricks_runtime,
    print_dataframe,
    print_header,
    print_path,
    print_schema,
    read_spark_excel,
    set_log_level,
)
from pys_excel.logs import get_logger

set_log_level("DEBUG")
logger = get_logger("example.read_any_file_generic_options")


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for generic, config-driven Excel reads.

    Every flag maps to a ``pandas.read_excel`` option and is optional — when
    ``--path`` is omitted, the script falls back to its built-in demo using a
    generated sample workbook instead of requiring a real file.

    Returns:
        Configured ArgumentParser.
    """
    parser = argparse.ArgumentParser(
        description="Read any Excel workbook into a Spark DataFrame using generic, CLI-driven options.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--path",
        help="Path to a .xlsx/.xls/.xlsm workbook, a glob pattern (e.g. '/data/*.xlsx'), "
        "or a directory of workbooks. Omit to run the built-in demo.",
    )
    parser.add_argument(
        "--mode",
        choices=["pandas", "distributed"],
        default="pandas",
        help="Library/backend to use at runtime: 'pandas' (ExcelReader, driver-collected via "
        "pandas.read_excel) or 'distributed' (spark-excel connector, reads across executors "
        "like any other Spark format). Switch with a flag — no code changes needed.",
    )
    parser.add_argument(
        "--data-address",
        help="Distributed mode only: cell range/sheet address, e.g. \"'Employees'!A1:F100\". "
        "Defaults to the whole --sheet (or 'Sheet1').",
    )
    parser.add_argument(
        "--infer-schema",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Distributed mode only: infer column types by sampling data.",
    )
    parser.add_argument("--sheet", help="Sheet name or zero-based index (e.g. 'Employees' or 0).")
    parser.add_argument("--header", help="Header row index (0-based), or 'none' for headerless sheets.")
    parser.add_argument("--skiprows", type=int, help="Number of rows to skip before parsing headers/data.")
    parser.add_argument("--nrows", type=int, help="Limit the number of data rows read.")
    parser.add_argument("--usecols", help="Columns to read — Excel range string (e.g. 'A:C') or comma-separated list.")
    parser.add_argument("--names", help="Comma-separated column names (use with --header none).")
    parser.add_argument("--na-values", help="Comma-separated extra strings to treat as NA/NaN.")
    parser.add_argument(
        "--dtype", help="Comma-separated column=type pairs (e.g. 'emp_id=str,active=bool'). Types apply per-column."
    )
    parser.add_argument("--keep-default-na", action=argparse.BooleanOptionalAction, help="Toggle default NA parsing.")
    parser.add_argument("--engine", help="pandas Excel engine ('openpyxl' for .xlsx, 'xlrd' for legacy .xls).")
    return parser


_DTYPE_CASTS: dict[str, type] = {"str": str, "int": int, "float": float, "bool": bool}


def _parse_sheet(value: str) -> str | int:
    """Parse --sheet into an int index when numeric, else keep as a sheet name."""
    return int(value) if value.lstrip("-").isdigit() else value


def _parse_header(value: str) -> int | None:
    """Parse --header, treating 'none'/'None' as headerless (pandas header=None)."""
    return None if value.strip().lower() == "none" else int(value)


def _header_as_bool(value: str | None) -> bool:
    """Translate the shared --header CLI value into the distributed reader's bool flag.

    The pandas backend takes a header *row index* (or None); the distributed
    ``spark-excel`` backend only takes a header on/off flag — so 'none' maps
    to False and any row index (including 0) maps to True.
    """
    return True if value is None else _parse_header(value) is not None


def _data_address(args: argparse.Namespace) -> str:
    """Build the distributed reader's data_address from --data-address or --sheet."""
    if args.data_address:
        return str(args.data_address)
    sheet_name = args.sheet or "Sheet1"
    return f"'{sheet_name}'!A1"


def _parse_dtype(value: str) -> dict[str, type]:
    """Parse 'col=type,col2=type2' into {col: python_type} for pandas dtype=."""
    mapping: dict[str, type] = {}
    for pair in value.split(","):
        column, _, type_name = pair.partition("=")
        mapping[column.strip()] = _DTYPE_CASTS.get(type_name.strip(), str)
    return mapping


def options_from_args(args: argparse.Namespace) -> dict[str, Any]:
    """Translate parsed CLI args into a generic pandas.read_excel options dict.

    Only flags explicitly supplied on the command line are included, so
    unset options fall back to ExcelReader's own defaults.

    Args:
        args: Namespace produced by ``build_arg_parser().parse_args()``.

    Returns:
        Options dict suitable for ``read_excel_generic()``/``with_options()``.
    """
    options: dict[str, Any] = {}
    if args.sheet is not None:
        options["sheet_name"] = _parse_sheet(args.sheet)
    if args.header is not None:
        options["header"] = _parse_header(args.header)
    if args.skiprows is not None:
        options["skiprows"] = args.skiprows
    if args.nrows is not None:
        options["nrows"] = args.nrows
    if args.usecols is not None:
        options["usecols"] = args.usecols if ":" in args.usecols else args.usecols.split(",")
    if args.names is not None:
        options["names"] = args.names.split(",")
    if args.na_values is not None:
        options["na_values"] = args.na_values.split(",")
    if args.dtype is not None:
        options["dtype"] = _parse_dtype(args.dtype)
    if args.keep_default_na is not None:
        options["keep_default_na"] = args.keep_default_na
    if args.engine is not None:
        options["engine"] = args.engine
    return options


_GLOB_CHARS = ("*", "?", "[")
_EXCEL_EXTENSION_PATTERNS = ("*.xlsx", "*.xls", "*.xlsm")
_EXCEL_SUFFIXES = {".xlsx", ".xls", ".xlsm"}


def resolve_paths(path: str) -> list[str]:
    """Resolve --path into a sorted list of workbook file paths.

    Accepts three input styles so a single ``--path`` flag stays generic for
    "any file":
        - A single file path (e.g. "/data/report.xlsx").
        - A glob pattern (e.g. "/data/*.xlsx", "/data/2024-*.xlsx"). Handy when
          the shell/IDE run config passes the pattern through unexpanded
          (e.g. a literal trailing ``*``). Matches are filtered down to
          ``.xlsx``/``.xls``/``.xlsm`` files, so a bare ``*`` won't pick up
          unrelated files or subdirectories.
        - A directory, in which case every ``.xlsx``/``.xls``/``.xlsm`` file
          directly inside it is matched.

    Args:
        path: File path, glob pattern, or directory.

    Returns:
        Sorted list of resolved workbook paths.

    Raises:
        FileNotFoundError: If no files match ``path``.
    """
    candidate = Path(path)
    if candidate.is_dir():
        matches = [str(p) for pattern in _EXCEL_EXTENSION_PATTERNS for p in candidate.glob(pattern) if p.is_file()]
    elif any(char in path for char in _GLOB_CHARS):
        matches = [p for p in glob_module.glob(path) if Path(p).suffix.lower() in _EXCEL_SUFFIXES and Path(p).is_file()]
    else:
        matches = [path] if candidate.is_file() else []

    if not matches:
        msg = f"No Excel workbook(s) found for --path={path!r}"
        raise FileNotFoundError(msg)
    return sorted(matches)


def read_excel_generic(spark: SparkSession, path: str, options: dict[str, Any] | None = None) -> DataFrame:
    """Read any Excel workbook using a generic, caller-supplied options dict.

    Args:
        spark: Active SparkSession.
        path: Path to the .xlsx/.xls/.xlsm workbook — any file, any layout.
        options: Arbitrary pandas.read_excel keyword options (e.g. sheet_name,
            header, skiprows, usecols, dtype, na_values, engine). Defaults to
            ``None`` (treated as an empty dict), which falls back to
            ExcelReader's own defaults (first sheet, header row 0, openpyxl
            engine).

    Returns:
        DataFrame with the parsed sheet contents.
    """
    logger.info("Reading %s with generic options=%s", path, options)
    return ExcelReader(spark).with_options(**(options or {})).read(path)


if __name__ == "__main__":
    cli_args = build_arg_parser().parse_args()

    # Choose the SparkSession to match the runtime library: the distributed
    # spark-excel connector needs its Maven package preloaded for local/OSS
    # Spark (on Databricks the cluster already has the connector installed).
    if cli_args.mode == "distributed" and not is_databricks_runtime():
        spark = get_spark_with_excel_package("read-excel-generic-options-distributed")
    else:
        spark = get_spark("read-excel-generic-options")

    if cli_args.path:
        print_header(f"CLI-driven read (mode={cli_args.mode}): {cli_args.path}")
        workbook_paths = resolve_paths(cli_args.path)
        logger.info("Resolved %d workbook(s) from --path=%r: %s", len(workbook_paths), cli_args.path, workbook_paths)

        for workbook_path in workbook_paths:
            print_path("Workbook", workbook_path)
            if cli_args.mode == "distributed":
                df_cli = read_spark_excel(
                    spark,
                    workbook_path,
                    data_address=_data_address(cli_args),
                    header=_header_as_bool(cli_args.header),
                    infer_schema=cli_args.infer_schema,
                )
            else:
                df_cli = read_excel_generic(spark, workbook_path, options_from_args(cli_args))
            title = f"CLI Options ({Path(workbook_path).name})"
            print_schema(df_cli, title=title)
            print_dataframe(df_cli, title=title)

        spark.stop()
        raise SystemExit(0)

    workbook = generate_sample_workbook()
    print_path("Workbook", workbook)

    print_header("1. Defaults only (no options)")
    df_defaults = read_excel_generic(spark, workbook)
    print_schema(df_defaults, title="Default Options")
    print_dataframe(df_defaults, title="Default Options")

    print_header("2. Sheet selected by name")
    df_by_name = read_excel_generic(spark, workbook, {"sheet_name": "Departments"})
    print_dataframe(df_by_name, title="Sheet by Name")

    print_header("3. Sheet selected by zero-based index")
    df_by_index = read_excel_generic(spark, workbook, {"sheet_name": 0})
    print_dataframe(df_by_index, title="Sheet by Index")

    print_header("4. Combined generic options: usecols, dtype, na_values")
    df_combined = read_excel_generic(
        spark,
        workbook,
        {
            "sheet_name": "Employees",
            "usecols": "A:C",
            "dtype": {"emp_id": str},
            "na_values": ["N/A", "n/a"],
        },
    )
    print_schema(df_combined, title="Combined Options")
    print_dataframe(df_combined, title="Combined Options")

    print_header("5. Same helper reused for a second, differently-shaped workbook")
    second_workbook = generate_sample_workbook(path=workbook.replace("employees.xlsx", "employees_copy.xlsx"))
    print_path("Second Workbook", second_workbook)
    df_second = read_excel_generic(spark, second_workbook, {"sheet_name": "Employees", "header": 0})
    print_dataframe(df_second, title="Second Workbook")

    spark.stop()
