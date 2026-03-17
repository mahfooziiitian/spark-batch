# GitHub Copilot Instructions — PySpark Schema

> **Global instruction file.** Topic-specific conventions live in
> `.github/instructions/` and are auto-applied based on the file you are editing.

## Modular Instruction Files

| File | Scope (`applyTo`) | What It Covers |
| ---- | ----------------- | -------------- |
| [`pyspark-schema.instructions.md`](instructions/pyspark-schema.instructions.md) | `src/**/*.py` | Schema types, StructType builder, complex types, validation, evolution |
| [`testing.instructions.md`](instructions/testing.instructions.md) | `{**/test_*.py,**/*_test.py}` | Session fixture, schema assertion helpers, test class layout |
| [`mkdocs.instructions.md`](instructions/mkdocs.instructions.md) | `{docs/**/*.md,mkdocs.yml}` | Material theme, schema page template, type-tree diagrams |

---

## Project Overview

This project is a **PySpark schema reference** that demonstrates every idiomatic
way to define, validate, evolve, and introspect DataFrame schemas in PySpark 3.5.x.
All examples are self-contained and runnable with `local[*]` — no cluster required.

| Area | Module path | Key API |
| ---- | ----------- | ------- |
| Schema definition — StructField list | `src/spark_schema.py` | `StructType(fields=[...])` |
| Schema definition — builder | `src/definition/schema_definition_builder.py` | `StructType().add(...)` |
| Schema definition — DDL string | `src/definition/schema_definition_type2.py` | `StructType.fromDDL(...)` |
| Array schema | `src/arrays/PysparkArraySchema*.py` | `ArrayType`, `StructType` |
| Column introspection | `src/column/` | `df.schema`, `AnalysisException` |
| Schema evolution | `src/evolution/SchemaMerge.py` | `mergeSchema`, Delta Lake |
| Schema parsing | `src/parser/schema_parser.py` | `_parse_datatype_string` |

---

## Project Structure

```
pyspark-schema/
├── .github/
│   ├── copilot-instructions.md          # ← you are here (global)
│   └── instructions/
│       ├── pyspark-schema.instructions.md
│       ├── testing.instructions.md
│       └── mkdocs.instructions.md
├── src/
│   ├── spark_schema.py                  # Entry-point overview example
│   ├── arrays/                          # ArrayType + nested struct schemas
│   │   ├── PysparkArraySchemaCreate.py
│   │   ├── PysparkArraySchemaFields.py
│   │   ├── PysparkArraySchemaJson.py
│   │   ├── PysparkArraySchemaPrint.py
│   │   ├── PysparkArraySchemaRead.py
│   │   ├── PysparkArraySchemaUpdate.py
│   │   └── PysparkArraySchemaValidate.py
│   ├── column/                          # Column existence & inspection
│   │   ├── ColumnExistence.py
│   │   ├── HasColumn.py
│   │   └── PrintColumns.py
│   ├── definition/                      # Schema definition styles
│   │   ├── schema_definition_builder.py # StructType().add() builder
│   │   └── schema_definition_type2.py   # DDL-string / fromDDL
│   ├── evolution/                       # Schema merge & evolution
│   │   └── SchemaMerge.py
│   └── parser/                          # Type-string parsing
│       ├── __init__.py
│       └── schema_parser.py
└── docs/                                # MkDocs Material site
```

---

## Tech Stack

| Component | Version |
| --------- | ------- |
| PySpark | 3.5.x |
| Python | ≥ 3.11 |
| Java | 11 (LTS) |
| Testing | pytest |
| Documentation | MkDocs Material ≥ 9.5 |

---

## Key Conventions

- **Never use `from pyspark.sql.functions import *`** — always `import functions as F`.
- **Never use `from pyspark.sql import *`** — import `SparkSession` and types explicitly.
- **Explicit `nullable`** — always set `nullable=True/False` on every `StructField`; never rely on defaults.
- **`SPARK_MASTER` env var** with `local[*]` fallback — every script runs locally without changes.
- **`INPUT_PATH` / `OUTPUT_PATH` env vars** with `/tmp/...` fallbacks — no hard-coded paths.
- **`spark.stop()`** at the end of every standalone script.
- **Schema first** — define the schema before reading data; never rely on schema inference in production code.

---

## SparkSession Pattern

```python
import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("descriptive-job-name")
         .master(os.environ.get("SPARK_MASTER", "local[*]"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

---

## Common Commands

```bash
# Run a specific example
SPARK_MASTER=local[*] python src/definition/schema_definition_builder.py

# Run the full test suite
pytest src/ -v

# Preview docs
mkdocs serve

# Build docs (strict)
mkdocs build --strict
```

---

## Things to Avoid

- **Do not** use `from pyspark.sql import *` — produces name collisions and hides intent.
- **Do not** rely on schema inference (`spark.read.json(path)` without `schema=`) in production or test code.
- **Do not** hard-code file paths or Windows-style separators (`C:\\`, `E:\\`).
- **Do not** call `df.collect()` to count rows — use `df.count()`.
- **Do not** omit `nullable` on `StructField` — always be explicit.
- **Do not** use `AnalysisException` for control flow beyond column existence checks.
