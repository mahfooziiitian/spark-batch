---
applyTo: "pyproject.toml,.github/**,src/**,tests/**"
---

# Quality & CI

## Task Runner

All tasks: `uv run task <name>`. Defined in `[tool.taskipy.tasks]` in `pyproject.toml`.

### Key Commands

| Command | Purpose |
|---------|---------|
| `uv run task quality` | Full pipeline: import → format → lint → type_check → sql |
| `uv run task test` | pytest -vv tests/ |
| `uv run task docs_build` | MkDocs strict build |
| `uv run task secure` | bandit + safety |

## Pre-commit Gate

```bash
uv run task quality
uv run task docs_build
uv run task test
```

## Configuration

All tool config lives **exclusively in `pyproject.toml`**. Never create:
`.flake8`, `setup.cfg`, `.mypy.ini`, `.bandit`, `.isort.cfg`, or `ruff.toml`.

| Setting | Value |
|---------|-------|
| Max line length | 128 |
| Python target | 3.11 |
| SQL dialect | Databricks |
| Coverage minimum | 60% |
| Ruff rules | E, F, W, I, UP |

## Dependency Management

```bash
uv add <package>              # runtime
uv add --group dev <package>  # dev
```

- Always commit `uv.lock`.
- Pin breaking majors (e.g. `mkdocs>=1.6,<2`).

## Security

- Bandit scans `src/` only (excludes tests, .venv).
- Never suppress findings without `# nosec: <justification>`.
- Never commit secrets.
