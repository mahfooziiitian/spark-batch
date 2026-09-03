# GitHub Copilot Instructions — Spark XPath

> **This is the global instruction file.** Topic-specific conventions live in
> modular files under `.github/instructions/` and are auto-applied based on
> the file you are editing. Refer to them for full details.

## Modular Instruction Files

| File | Scope (`applyTo`) | What It Covers |
| ---- | ------------------ | -------------- |
| [`python.instructions.md`](instructions/python.instructions.md) | `**/*.py` | PEP 8, type hints, imports, Google-style docstrings |
| [`pyspark.instructions.md`](instructions/pyspark.instructions.md) | `src/**/*.py`, `examples/**/*.py` | SparkSession, DataFrame creation, XPath functions, SQL patterns |
| [`pytest.instructions.md`](instructions/pytest.instructions.md) | `tests/**/*.py` | Fixtures, test naming, arrange-act-assert, assertions |
| [`mkdocs.instructions.md`](instructions/mkdocs.instructions.md) | `docs/**/*.md`, `mkdocs.yml` | Page structure, admonitions, code blocks, page template |
| [`github-actions.instructions.md`](instructions/github-actions.instructions.md) | `.github/workflows/**/*.yml` | Pipeline structure, uv setup, matrix, permissions |
| [`project-config.instructions.md`](instructions/project-config.instructions.md) | `pyproject.toml`, `uv.lock`, `.python-version` | uv commands, dependency groups, lockfile rules |

When editing a `.py` file in `src/` or `examples/`, both **python** and
**pyspark** instructions activate. When editing a test, both **python** and
**pytest** instructions apply.

---

## Project Overview

This project demonstrates how to extract and transform XML data inside PySpark
DataFrames using Spark's built-in XPath SQL functions (`xpath_string`,
`xpath_boolean`, `xpath`, etc.). It targets **Python ≥ 3.11** with
**PySpark >= 4.0**.

---

## Project Structure

```
spark-xpath/
├── .github/
│   ├── copilot-instructions.md        # ← you are here (global)
│   ├── instructions/                  # modular instruction files
│   │   ├── python.instructions.md
│   │   ├── pyspark.instructions.md
│   │   ├── pytest.instructions.md
│   │   ├── mkdocs.instructions.md
│   │   ├── github-actions.instructions.md
│   │   └── project-config.instructions.md
│   └── workflows/
│       └── ci.yml                     # GitHub Actions CI pipeline
├── src/
│   └── spark_xpath/                # library / helper / utility code
│       └── __init__.py
├── examples/                       # runnable usage / demo scripts
│   ├── xml_xpath.py                # credit-evaluation example
│   ├── xml_data_parsing.py         # parse XML column with xpath functions
│   ├── text/                       # xpath() array text-node extraction
│   └── netsted/                    # xpath() on nested columns with namespaces
├── tests/                          # pytest suite
├── docs/                           # MkDocs documentation (Markdown)
├── mkdocs.yml                      # MkDocs + Material theme config
└── pyproject.toml                  # Project metadata & dependencies
```

## Folder Conventions

| Folder | Holds | Import? |
|--------|-------|---------|
| `src/spark_xpath/` | **Library / helper / utility** code — reusable, importable modules. | Yes — `from spark_xpath import ...` |
| `examples/` | **Usage / demo** scripts — self-contained, runnable XPath examples. | No — run directly |

Reusable helpers go in `src/spark_xpath/`; runnable demonstrations go in
`examples/`. Example scripts build their own SparkSession, embed sample XML
inline, and call `spark.stop()`.

---

## Technology Stack

| Component        | Technology                    |
| ---------------- | ----------------------------- |
| Language         | Python 3.11+                  |
| Spark            | PySpark >= 4.0                |
| Package Manager  | [uv](https://docs.astral.sh/uv/) |
| Testing          | pytest, pytest-mock, pytest-sugar |
| Documentation    | MkDocs + mkdocs-material      |
| CI/CD            | GitHub Actions                |

---

## Quick Reference

The sections below are **summaries**. See the linked instruction file for
complete rules and examples.

### Python Style → [`python.instructions.md`](instructions/python.instructions.md)

- PEP 8, explicit imports, type hints, `snake_case`.
- **Google-style docstrings** with `Args`, `Returns`, `Raises` sections.

### PySpark Patterns → [`pyspark.instructions.md`](instructions/pyspark.instructions.md)

- `SparkSession.builder.master("local[*]")` for local examples.
- XML as `StringType` column → temp view → `spark.sql()` with XPath functions.
- Strip namespace prefixes; use `[@attr=value]` for indexed elements.

### Testing → [`pytest.instructions.md`](instructions/pytest.instructions.md)

- Session-scoped `spark` fixture; inline XML data; unique temp view names.
- Arrange → Act → Assert pattern; prefer value assertions over count checks.

### Documentation → [`mkdocs.instructions.md`](instructions/mkdocs.instructions.md)

- mkdocs-material theme; admonitions, tabbed content, code highlighting.
- Register every new page in `mkdocs.yml` `nav:` section.

### CI/CD → [`github-actions.instructions.md`](instructions/github-actions.instructions.md)

- uv-based setup; Python 3.11 + 3.12 matrix; `--strict` docs build.

### Project Config → [`project-config.instructions.md`](instructions/project-config.instructions.md)

- uv for dependency management; runtime deps vs dev dependency-groups.

---

## Common Commands

```bash
uv sync                              # install all dependencies
uv run pytest tests/ -v              # run tests
uv run mkdocs serve                  # preview docs locally
uv run mkdocs build --strict         # build docs (CI mode)
uv run python examples/xml_xpath.py # run credit-evaluation example
```

---

## Things to Avoid

- **Do not** use `df.select("/xpath/expression")` — that is a column reference, not XPath. Use `spark.sql("SELECT xpath_string(col, 'expr') ...")` instead.
- **Do not** add the namespace prefix in XPath expressions — Spark strips them automatically.
- **Do not** use `scope="module"` or narrower scope for the Spark fixture in tests — starting/stopping Spark per test is very slow.
- **Do not** use `from pyspark.sql.functions import *` in new code — use explicit imports.
- **Do not** use reST (`:param:`), NumPy, or Epydoc docstring styles — use **Google style** only.
