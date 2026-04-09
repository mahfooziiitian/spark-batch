---
applyTo: "pyproject.toml"
---

# Project Configuration

## Format

- This project uses the **`[project]` table** format (PEP 621) — compatible with `uv`, `pip`, and standards-based tooling.
- It does **not** use Poetry's `[tool.poetry]` format.
- There is **no `[build-system]`** section defined.

## Python Version

- Requires **Python ≥ 3.11** (`requires-python = ">=3.11"`).
- The `.python-version` file pins the exact interpreter version for version managers.

## Dependencies

- The `dependencies` list is **empty** (`dependencies = []`).
- PySpark and Kafka/MySQL connectors are **not** declared as Python package dependencies — they are loaded at runtime via `spark.jars.packages` in the Spark configuration.
- If adding Python dependencies in the future, add them to the `dependencies` list (not a Poetry-specific section).

## Project Metadata

- `name`: `"pyspark-kafka"`
- `version`: `"0.1.0"`
- `readme`: `"README.md"`

## Conventions

- Do not add a `[tool.poetry]` section — this is not a Poetry project.
- Do not add a `[build-system]` section unless the project needs to become an installable package.
- Keep Spark/Kafka JAR versions synchronized between `pyproject.toml` metadata and the `spark.jars.packages` configuration in source files.
- If adding dev dependencies, use `[project.optional-dependencies]` with a `dev` extra:
  ```toml
  [project.optional-dependencies]
  dev = ["pytest>=7.0", "ruff>=0.4"]
  ```
