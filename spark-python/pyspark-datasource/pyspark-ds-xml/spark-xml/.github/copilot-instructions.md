# Copilot Instructions — spark-xml

This project demonstrates reading, writing, and transforming XML with PySpark
using the **built-in Spark 4 `xml` data source** and the native
`from_xml` / `schema_of_xml` SQL functions. No external JARs are required —
the old `com.databricks:spark-xml` package has been fully replaced by the
native source.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark.instructions.md` | `src/**/*.py`, `examples/**/*.py` | SparkSession, native XML source, `from_xml`/`schema_of_xml` |
| `instructions/mkdocs.instructions.md` | `docs/**/*.md`, `mkdocs.yml` | MkDocs Material documentation style |
| `instructions/project-config.instructions.md` | `pyproject.toml`, `uv.lock`, `.python-version` | uv, dependencies, native-XML version pin |

## Folder Conventions

This project separates **reusable code** from **runnable demonstrations**:

| Folder | Holds | Import? |
|--------|-------|---------|
| `src/spark_xml/` | **Library / helper / utility** code — reusable, importable modules (XML data generation, XSD generation, XSD validation under `util/`). | Yes — `from spark_xml.util import ...` |
| `examples/` | **Usage / demo / notebook** scripts — self-contained, runnable examples grouped by feature (`nested/`, `schema/`, `compression/`, `encoding/`, `namespace/`, `sql/`, `writer/`, …). | No — run directly, don't import |

Rules:

- Put anything **reusable** (helpers, validators, generators, shared config) in
  `src/spark_xml/` as a proper package module with an `__init__.py`.
- Put anything that is a **demonstration** of a feature — a script a reader runs
  to see output — in `examples/<feature>/`.
- Example scripts are standalone: they build their own `SparkSession`, embed
  sample XML inline, and call `spark.stop()` at the end. They must **not**
  import from one another.
- When adding a new example, create `examples/<feature>/<script>.py` and, if it
  shows a documented pattern, add a matching page under `docs/guide/`.

## Project Overview

Use the native XML source when you need to read/write whole XML files as
DataFrames or parse XML string columns:

- `spark.read.format("xml").option("rowTag", "...").load(path)`
- `from_xml(col, schema, options)` to parse an XML string column into a struct.
- `schema_of_xml(lit(sample_xml), options)` to infer a schema from a sample.
- `df.write.format("xml").option("rootTag", ...).option("rowTag", ...).save(path)`

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | ≥ 4.0.0 (native `xml` source requires Spark 4) |
| Java | 17+ (required by Spark 4) |
| XML tooling | `lxml`, `xmlschema`, `xmltoxsd` (helpers only) |
| Package manager | uv |
| Documentation | MkDocs Material ≥ 9.5 |

!!! note "Spark 4 across the board"
    All three sub-projects now require `pyspark>=4.0.0` (Java 17+). This project
    in particular depends on the built-in `xml` data source, a Spark 4 feature.

## Project Structure

```
spark-xml/
├── .github/
│   ├── copilot-instructions.md          ← you are here
│   └── instructions/
│       ├── python.instructions.md
│       ├── pyspark.instructions.md
│       ├── mkdocs.instructions.md
│       └── project-config.instructions.md
├── src/
│   └── spark_xml/                        # library / helper / utility code
│       ├── __init__.py
│       └── util/                         # data gen, XSD gen, XSD validation
│           ├── __init__.py
│           ├── generate_xml_data.py
│           ├── validate_xml_xsd.py
│           ├── xml_to_xsd.py
│           └── xml_to_xsd_trang.py
├── examples/                             # runnable usage / demo scripts
│   ├── attribute/  collection/  compression/  encoding/  error/
│   ├── instruction/  namespace/  nested/  reader/  schema/
│   ├── sql/  stack_overlfow/  surrounding_space/  value_tag/  writer/
│   └── spark-dbx-xml.py
├── docs/                                 # MkDocs documentation
├── mkdocs.yml
└── pyproject.toml
```

## Quick Reference

```bash
uv sync                                          # install dependencies
uv run python examples/nested/parsing_xml_column.py   # run an example
uv run python -m spark_xml.util.generate_xml_data     # run a library helper
uv run mkdocs serve                              # preview docs
uv run mkdocs build --strict                     # build docs (CI mode)
```

## Things to Avoid

- Do **not** use `.format("com.databricks.spark.xml")` or add the
  `com.databricks:spark-xml` JAR — use the built-in `.format("xml")`.
- Do **not** put demo/example scripts in `src/` — they belong in `examples/`.
- Do **not** put reusable helpers in `examples/` — they belong in `src/spark_xml/`.
- Do **not** use `from pyspark.sql.functions import *` — use explicit imports.
- Do **not** omit `spark.stop()` in standalone example scripts.
- Do **not** use `len(df.collect())` — use `df.count()`.
