---
applyTo: "pyproject.toml,.github/**,src/**,tests/**"
---

# Quality & CI Instructions

## Taskipy — Task Runner

All development tasks run via `uv run task <name>`. Tasks are defined in
`[tool.taskipy.tasks]` in `pyproject.toml`. **Do not create Makefiles or shell
scripts for tasks that already exist here.**

### Quality Tasks

```bash
uv run task quality        # Full pipeline: import → format → format_check → lint → type_check → sql
uv run task import         # isort import ordering (--profile black)
uv run task format         # ruff format
uv run task format_check   # ruff check (lint + auto-fix suggestions)
uv run task lint           # flake8 style lint
uv run task type_check     # mypy static analysis
uv run task complexity     # radon cyclomatic complexity (src/ only, no-coverage flag)
uv run task sql            # sqlfluff fix + lint (combined)
uv run task sql_format     # sqlfluff fix only
uv run task sql_lint       # sqlfluff lint only
```

### Test Tasks

```bash
uv run task test                # pytest -vv tests/
uv run task report_html         # HTML report → reports/reports.html
uv run task report_xml          # JUnit XML → reports/report.xml
uv run task report_json         # JSON report + coverage term summary
uv run task report_cov_html     # Coverage HTML → htmlcov/
uv run task report_cov_xml      # Coverage XML (for CI upload)
uv run task report_cov_annotate # Annotated source coverage
```

### Security Tasks

```bash
uv run task secure   # bandit + safety (combined)
uv run task bandit   # static security scan of src/ only
uv run task safety   # dependency CVE check (requires .safety-policy.yml)
```

### Docs Tasks

```bash
uv run task docs_build   # NO_MKDOCS_2_WARNING=1 mkdocs build --strict
uv run task docs_serve   # mkdocs serve on 0.0.0.0:8080
uv run task docs         # clean → build → serve
uv run task lint_md      # pymarkdownlnt fix -r docs README.md
```

### Build & Clean Tasks

```bash
uv run task build         # uv build (wheel + sdist)
uv run task clean         # remove __pycache__, .pytest_cache, site/
```

## Tool Configuration Reference

All tools are configured **exclusively in `pyproject.toml`**. Never create:
`.flake8`, `setup.cfg`, `.mypy.ini`, `.bandit`, `.isort.cfg`, or `ruff.toml`.

| Tool | Config section |
|------|---------------|
| pytest | `[tool.pytest.ini_options]` |
| coverage | `[tool.coverage.run]`, `[tool.coverage.report]`, `[tool.coverage.html]` |
| mypy | `[tool.mypy]` |
| flake8 | `[tool.flake8]` |
| ruff | `[tool.ruff]`, `[tool.ruff.lint]` |
| bandit | `[tool.bandit]` |
| sqlfluff | `[tool.sqlfluff.core]`, `[tool.sqlfluff.indentation]`, `[tool.sqlfluff.rules.*]` |
| isort | inline via `--profile black` flag in the taskipy task |

## Key Settings Summary

| Setting | Value |
|---------|-------|
| Max line length | 128 |
| Python version | 3.11 |
| SQL dialect | Databricks |
| SQL keyword style | UPPER |
| SQL identifier style | lower |
| Test paths | `tests/`, `integration/` |
| Source paths | `src/` |
| Coverage minimum | 60% |
| Coverage source | `src/` |
| Bandit scope | `src/` (excludes `tests/`, `.venv/`) |
| Bandit severity threshold | medium |
| Bandit confidence threshold | high |
| Ruff lint rules | E, F, W, I, UP |

## Security Policy

- Bandit scans `src/` only — explicitly excludes `tests/`, `migrations/`, `.venv/`.
- Safety reads `.safety-policy.yml` — this file **must** exist or the task fails.
- Never suppress bandit findings without a `# nosec: <justification>` comment.
- Never commit secrets — use environment variables or a secrets manager.

## Docs Quality

```bash
uv run task lint_md   # pymarkdownlnt fix -r docs README.md
```

Markdown lint targets: `docs/` and `README.md`. Fix all pymarkdownlnt warnings before committing.

## MkDocs Build Gate

MkDocs runs in **strict mode** — any warning is an error:

```bash
NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict
```

`NO_MKDOCS_2_WARNING=1` suppresses the Material theme's MkDocs 2.x compatibility nag
(acceptable because `mkdocs<2` is intentionally pinned).

## Dependency Management

```bash
uv add <package>              # add runtime dependency
uv add --group dev <package>  # add dev dependency
```

- Lock file is `uv.lock` — always commit it.
- Pin packages with breaking major versions (e.g. `mkdocs>=1.6,<2`).
- Do not pin patch versions for dev tools unless a specific bug requires it.

## Pre-commit Checklist

Run all three gates before every commit:

```bash
uv run task quality                                      # Python + SQL quality
NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict       # Docs build — zero warnings
uv run task test                                         # All tests pass
```

For security-sensitive changes also run:

```bash
uv run task secure   # bandit + safety
```
