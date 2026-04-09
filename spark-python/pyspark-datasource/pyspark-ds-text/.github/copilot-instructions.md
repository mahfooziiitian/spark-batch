# Copilot Instructions — pyspark-ds-text

This project demonstrates how to read, write, parse, and query plain-text files
using the PySpark text datasource (`spark.read.text` / `df.write.text`).
Every example is self-contained and runnable locally — no cluster required.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark-text.instructions.md` | `src/**/*.py` | PySpark text datasource read/write/parse patterns |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions, SparkSession fixture, assertions |
| `instructions/mkdocs.instructions.md` | `docs/**/*.md`, `mkdocs.yml` | MkDocs Material documentation style |
| `instructions/project-config.instructions.md` | `pyproject.toml`, `uv.lock`, `.python-version` | Package manager and project metadata |

## Project Overview

The PySpark text datasource reads each line of a text file into a single-column
DataFrame with column name `value` (type `StringType`). This project covers:

- Basic and multi-file text reading (`spark.read.text`)
- Read options: `wholetext`, `lineSep`, `encoding`, `pathGlobFilter`, `recursiveFileLookup`
- Compressed file reading and writing (gzip, bzip2)
- Parsing text into structured columns (split, substring, regex)
- SQL interface via temp views and `text.` path syntax
- RDD vs DataFrame text APIs
- Word count and text analytics
- Writing DataFrames as text (single string column requirement)

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | < 4.0.0 (3.5.x preferred) |
| Package manager | uv |
| Testing | pytest ≥ 8.0 |
| Documentation | MkDocs Material ≥ 9.5 |

## Project Structure

```
pyspark-ds-text/
├── .github/
│   ├── copilot-instructions.md          ← you are here
│   └── instructions/
│       ├── python.instructions.md
│       ├── pyspark-text.instructions.md
│       ├── testing.instructions.md
│       ├── mkdocs.instructions.md
│       └── project-config.instructions.md
├── src/
│   └── text/
│       ├── __init__.py
│       ├── read/                        # spark.read.text variations
│       │   ├── __init__.py
│       │   ├── read_text_basic.py           # one row per line
│       │   ├── read_text_compressed.py      # gzip, bzip2 auto-detect
│       │   ├── read_text_encoding.py        # UTF-8, Latin-1, UTF-16
│       │   ├── read_text_line_separator.py  # custom lineSep option
│       │   ├── read_text_multifile.py       # directories, globs, lists
│       │   ├── read_text_path_glob_filter.py # pathGlobFilter option
│       │   ├── read_text_recursive.py       # recursiveFileLookup
│       │   └── read_text_wholetext.py       # one row per file
│       ├── write/                       # df.write.text output
│       │   ├── __init__.py
│       │   ├── write_text_basic.py          # text writer basics
│       │   └── write_text_compressed.py     # gzip, bzip2 compression
│       ├── parse/                       # text → structured columns
│       │   ├── __init__.py
│       │   ├── parse_text_columns.py        # split, substring, cast
│       │   └── parse_text_regex.py          # regexp_extract patterns
│       └── usage/                       # advanced usage patterns
│           ├── __init__.py
│           ├── text_rdd_vs_dataframe.py     # textFile vs spark.read.text
│           ├── text_sql_view.py             # temp views + SQL queries
│           └── text_word_count.py           # classic word count
├── tests/
│   ├── __init__.py
│   ├── conftest.py                      # session-scoped SparkSession
│   └── test_text_datasource.py          # all text datasource tests
├── docs/                                # MkDocs Material site
├── pyproject.toml
├── README.md
└── uv.lock
```

## Quick Reference

```bash
uv sync                                    # install dependencies
uv run pytest                              # run all tests
uv run pytest tests/ -v --tb=short         # verbose test output
uv run python src/text/read/read_text_basic.py  # run an example
```

## Things to Avoid

- Do **not** use `from pyspark.sql.functions import *` — always `import functions as F`.
- Do **not** leave `spark.stop()` out of standalone scripts.
- Do **not** use `print(df.schema)` — use `df.printSchema()` instead.
- Do **not** forget that `df.write.text()` requires exactly one `StringType` column.
- Do **not** assume text encoding — always specify `encoding` option for non-UTF-8 files.
- Do **not** use `len(df.collect())` — use `df.count()` for row counts.
- Do **not** use RDD APIs when a DataFrame equivalent exists — prefer `spark.read.text` over `sc.textFile`.
