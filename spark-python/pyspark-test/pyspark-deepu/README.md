# PySpark PyDeequ

A PySpark data quality reference project using [PyDeequ](https://github.com/awslabs/python-deequ) — the Python wrapper for AWS Deequ.

## Features

- **Analyzers** — compute metrics (size, completeness, mean, distinct counts)
- **Constraint Verification** — validate DataFrames against defined checks
- **Constraint Suggestions** — auto-suggest constraints from data patterns
- **Column Profiling** — statistical profiles for each column
- **Metrics Repository** — persist and query metrics over time

## Quick Start

```bash
uv sync --group dev
uv run task test
```

## Available Tasks

| Command | Description |
|---------|-------------|
| `uv run task test` | Run tests |
| `uv run task test_verbose` | Verbose test output |
| `uv run task lint` | Lint with ruff |
| `uv run task format` | Format with ruff |
| `uv run task typecheck` | Type check with mypy |
| `uv run task check` | Full CI pipeline |
| `uv run task docs` | Build documentation |
| `uv run task docs_serve` | Serve docs locally |