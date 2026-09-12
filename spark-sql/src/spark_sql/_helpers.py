"""SQL test helpers for executing and validating the ``sql/`` examples.

Provides comment-aware ``.sql`` file parsing (splitting a file into individual
statements, stripping ``--`` line comments while respecting string literals) plus
small execution/assertion utilities used by the pytest suite, ``examples/`` scripts,
and :mod:`spark_sql.runner` to run those statements against a :class:`SparkSession`.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

REPO_ROOT = Path(__file__).resolve().parents[2]


def repo_path(relative_path: str) -> Path:
    """Resolve *relative_path* against the repository root.

    Args:
        relative_path: Path relative to the repo root (e.g. ``"sql/scd/type2/expire.sql"``).

    Returns:
        The absolute :class:`~pathlib.Path`.
    """
    return REPO_ROOT / relative_path


def read_sql_text(sql_path: str) -> str:
    """Read the full contents of a repo-relative ``.sql`` file.

    Args:
        sql_path: Path relative to the repo root.

    Returns:
        The file's text, decoded as UTF-8.
    """
    return repo_path(sql_path).read_text(encoding="utf-8")


def _strip_line_comment(line: str) -> str:
    """Return *line* with any trailing ``--`` comment removed.

    Tracks single- and double-quote state so a ``--`` inside a string literal is
    not mistaken for the start of a comment.

    Args:
        line: A single line of SQL text (no embedded newlines).

    Returns:
        The line with any unquoted trailing ``--`` comment stripped.
    """
    in_single = False
    in_double = False
    index = 0
    while index < len(line) - 1:
        current = line[index]
        nxt = line[index + 1]
        if current == "'" and not in_double:
            in_single = not in_single
        elif current == '"' and not in_single:
            in_double = not in_double
        elif current == "-" and nxt == "-" and not in_single and not in_double:
            return line[:index]
        index += 1
    return line


def clean_sql_statement(statement: str) -> str:
    """Strip comment-only lines, line comments, and blank lines from a statement.

    Args:
        statement: A single (possibly multi-line) SQL statement.

    Returns:
        The statement with ``--``-comment lines, inline ``--`` comments, Databricks
        notebook ``-- COMMAND`` markers, and blank lines removed; surrounding
        whitespace stripped.
    """
    cleaned_lines: list[str] = []
    for raw_line in statement.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped == "---" or stripped.startswith("-- COMMAND"):
            continue
        without_comment = _strip_line_comment(raw_line).strip()
        if without_comment:
            cleaned_lines.append(without_comment)
    return "\n".join(cleaned_lines).strip()


def split_sql_statements(sql_text: str) -> list[str]:
    """Split raw ``.sql`` file text into individual statements.

    Splits on unquoted, uncommented semicolons, tracking single/double quotes and
    ``--`` line comments so semicolons inside string literals or comments do not
    trigger a split.

    Args:
        sql_text: The full text of a ``.sql`` file (possibly containing many
            semicolon-terminated statements).

    Returns:
        Each statement's raw text (still possibly containing comments/whitespace;
        pass through :func:`clean_sql_statement` to normalize), in file order.
    """
    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    index = 0

    while index < len(sql_text):
        character = sql_text[index]
        nxt = sql_text[index + 1] if index + 1 < len(sql_text) else ""

        if in_line_comment:
            current.append(character)
            if character == "\n":
                in_line_comment = False
            index += 1
            continue

        if character == "-" and nxt == "-" and not in_single and not in_double:
            in_line_comment = True
            current.append(character)
            current.append(nxt)
            index += 2
            continue

        if character == "'" and not in_double:
            in_single = not in_single
        elif character == '"' and not in_single:
            in_double = not in_double

        if character == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(character)
        index += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def read_sql_statements(sql_path: str) -> list[str]:
    """Read a repo-relative ``.sql`` file and return its cleaned statements.

    Combines :func:`read_sql_text`, :func:`split_sql_statements`, and
    :func:`clean_sql_statement`; statements that clean down to nothing (e.g. a
    file containing only comments) are omitted.

    Args:
        sql_path: Path relative to the repo root.

    Returns:
        Each non-empty, cleaned statement in file order.
    """
    return [
        cleaned for statement in split_sql_statements(read_sql_text(sql_path)) if (cleaned := clean_sql_statement(statement))
    ]


def execute_sql_file(
    spark: SparkSession,
    sql_path: str,
    *,
    skip_predicate: Callable[[str], bool] | None = None,
    transform: Callable[[str], str | None] | None = None,
) -> list[DataFrame]:
    """Execute every statement in a repo-relative ``.sql`` file.

    Args:
        spark: The :class:`SparkSession` to execute against (never created here;
            callers own its lifecycle).
        sql_path: Path relative to the repo root.
        skip_predicate: Optional filter; statements for which it returns ``True``
            are skipped entirely.
        transform: Optional rewrite applied to each statement before execution;
            returning ``None``/empty skips the statement.

    Returns:
        One :class:`DataFrame` per executed query statement (``SELECT``/``WITH``),
        in file order. DDL/DML statements still execute but are not returned.
    """
    results: list[DataFrame] = []
    for statement in read_sql_statements(sql_path):
        if skip_predicate and skip_predicate(statement):
            continue
        executable = transform(statement) if transform else statement
        if not executable:
            continue
        dataframe = spark.sql(executable)
        if executable.upper().startswith(("SELECT", "WITH")):
            results.append(dataframe)
    return results


def statement_containing(sql_path: str, needle: str) -> str:
    """Return the first statement in *sql_path* containing *needle* (case-insensitive).

    Useful for re-running a single named query from a multi-statement file after
    :func:`execute_sql_file`/``run_file`` has already created its TEMP VIEWs.

    Args:
        sql_path: Path relative to the repo root.
        needle: Substring to search for (matched case-insensitively).

    Returns:
        The matching statement's cleaned text.

    Raises:
        ValueError: If no statement in the file contains *needle*.
    """
    for statement in read_sql_statements(sql_path):
        if needle.lower() in statement.lower():
            return statement
    raise ValueError(f"No statement containing {needle!r} found in {sql_path}")


def assert_query_in_source(relative_path: str, query: str) -> None:
    """Assert that *query* (or its distinctive keywords) appears in *relative_path*.

    First checks for an exact whitespace-insensitive substring match; if that
    fails, falls back to checking that every keyword longer than 3 characters
    from *query* appears somewhere in the source, tolerating punctuation and
    reformatting differences (e.g. a query reconstructed from an AST).

    Args:
        relative_path: Path relative to the repo root, containing the SQL source
            to check against.
        query: The query text (or a fragment of it) expected to appear in the file.

    Raises:
        AssertionError: If neither the exact match nor the keyword fallback succeeds.
    """
    compact_source = "".join(read_sql_text(relative_path).split()).lower()
    compact_query = "".join(query.split()).lower()
    if compact_query in compact_source:
        return
    normalized = query.replace("(", " ").replace(")", " ").replace(",", " ")
    keywords = [token.lower() for token in normalized.split() if len(token) > 3]
    source = read_sql_text(relative_path).lower()
    if not (keywords and all(keyword in source for keyword in keywords)):
        raise AssertionError(f"Query not found in source {relative_path!r}: {query!r}")


def create_view(
    spark: SparkSession,
    name: str,
    rows: Sequence[tuple[Any, ...]],
    schema: StructType | str,
) -> DataFrame:
    """Create an in-memory DataFrame and register it as a temp view.

    Convenience for building small fixture tables inline in tests/examples,
    without a separate ``createDataFrame`` + ``createOrReplaceTempView`` call pair.

    Args:
        spark: The :class:`SparkSession` to create the DataFrame with.
        name: Name to register the temp view under (usable in subsequent ``spark.sql``
            calls).
        rows: Row data as tuples, in column order matching *schema*.
        schema: A :class:`StructType`, or its DDL string form (e.g. ``"id INT, name STRING"``).

    Returns:
        The created :class:`DataFrame`, already registered as *name*.
    """
    dataframe = spark.createDataFrame(rows, schema=schema)
    dataframe.createOrReplaceTempView(name)
    return dataframe
