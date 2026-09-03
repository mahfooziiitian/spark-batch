from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

REPO_ROOT = Path(__file__).resolve().parents[1]


def repo_path(relative_path: str) -> Path:
    return REPO_ROOT / relative_path


def read_sql_text(sql_path: str) -> str:
    return repo_path(sql_path).read_text(encoding="utf-8")


def _strip_line_comment(line: str) -> str:
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
    for statement in read_sql_statements(sql_path):
        if needle.lower() in statement.lower():
            return statement
    raise ValueError(f"No statement containing {needle!r} found in {sql_path}")


def assert_query_in_source(relative_path: str, query: str) -> None:
    compact_source = "".join(read_sql_text(relative_path).split()).lower()
    compact_query = "".join(query.split()).lower()
    if compact_query in compact_source:
        return
    normalized = query.replace("(", " ").replace(")", " ").replace(",", " ")
    keywords = [token.lower() for token in normalized.split() if len(token) > 3]
    assert keywords and all(keyword in read_sql_text(relative_path).lower() for keyword in keywords)


def create_view(
    spark: SparkSession,
    name: str,
    rows: Sequence[tuple[Any, ...]],
    schema: StructType | str,
) -> DataFrame:
    dataframe = spark.createDataFrame(rows, schema=schema)
    dataframe.createOrReplaceTempView(name)
    return dataframe
