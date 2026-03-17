---
applyTo: "pyproject.toml,.github/**,src/**,tests/**"
---

# Quality & CI Instructions

## Taskipy — Task Runner

All development tasks are run via `uv run task <name>`. Tasks are defined in
`[tool.taskipy.tasks]` in `pyproject.toml`. **Do not create Makefiles or shell
scripts for tasks that already exist here.**

### Quality Tasks

```bash
uv run task quality      # Full quality pipeline (import → format → lint → type → sql)
uv run task import       # isort import ordering
uv run task format       # ruff format
uv run task format_check # ruff check (lint)
uv run task lint         # flake8
uv run task type_check   # mypy
uv run task sql          # sqlfluff fix + lint
uv run task sql_format   # sqlfluff fix only
uv run task sql_lint     # sqlfluff lint only
```

### Test Tasks

```bash
uv run task test              # pytest -vv tests/
uv run task report_html       # HTML report
uv run task report_xml        # JUnit XML
uv run task report_json       # JSON report + coverage
uv run task report_cov_html   # Coverage HTML report
uv run task report_cov_xml    # Coverage XML report
```

### Security Tasks

```bash
uv run task secure   # bandit + safety
uv run task bandit   # static security scan (src/ only)
uv run task safety   # dependency vulnerability check
```

### Docs Tasks

```bash
uv run task docs_build   # mkdocs build
uv run task docs_serve   # mkdocs serve on 0.0.0.0:8080
uv run task docs         # clean → build → serve
uv run task lint_md      # pymarkdownlnt on docs/ and README.md
```

### Clean Tasks

```bash
uv run task clean   # remove __pycache__, .pytest_cache, site/
```

## Tool Configuration Reference

All tools are configured **exclusively in `pyproject.toml`**. Never create:
`.flake8`, `setup.cfg`, `.mypy.ini`, `.bandit`, `.isort.cfg`, or separate `ruff.toml`.

| Tool | Config section |
|------|---------------|
| pytest | `[tool.pytest.ini_options]` |
| coverage | `[tool.coverage.run]`, `[tool.coverage.report]` |
| mypy | `[tool.mypy]` |
| flake8 | `[tool.flake8]` |
| ruff | `[tool.ruff]` (add if needed) |
| bandit | `[tool.bandit]` |
| sqlfluff | `[tool.sqlfluff.*]` |
| isort | inline via `--profile black` flag |

## Key Settings Summary

| Setting | Value |
|---------|-------|
| Max line length | 128 |
| Python version | 3.11 |
| SQL dialect | Databricks |
| SQL keyword style | UPPER |
| Test paths | `tests/`, `integration/` |
| Source paths | `src/` |
| Bandit scope | `src/` (excludes `tests/`, `.venv/`) |
| Bandit severity threshold | medium |
| Bandit confidence threshold | high |

## Security Policy

- Bandit scans `src/` only — it excludes `tests/`, `migrations/`, `.venv/`
- Safety checks live dependencies against known CVE databases
- A `.safety-policy.yml` file must exist for `uv run task safety` to run
- Never suppress bandit findings without a documented justification comment

## Docs Quality

```bash
uv run task lint_md   # pymarkdownlnt fix -r docs README.md
```

Markdown lint targets: `docs/` and `README.md`. Fix all warnings before committing.

## MkDocs Build Gate

MkDocs is run in **strict mode** — any warning is treated as an error:

```bash
NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict
```

The `NO_MKDOCS_2_WARNING=1` env var suppresses the Material theme's MkDocs 2.x
compatibility warning (acceptable because we intentionally pin `mkdocs<2`).

## Dependency Management

- Add runtime dependencies: `uv add <package>`
- Add dev dependencies: `uv add --group dev <package>`
- Lock file is `uv.lock` — commit it
- Pin versions that have breaking changes (e.g., `mkdocs>=1.6,<2`)

## Pre-commit Checklist

Before every commit, run:

```bash
uv run task quality          # Python + SQL quality
NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict  # Docs build
uv run task test             # Tests pass
```
