# Contributing

## Setup

```bash
uv sync                     # install runtime + dev dependencies
uv run pre-commit install   # enable git hooks (formatting/lint/security on commit)
```

The project uses **uv** exclusively for dependency management — never call
`pip` directly. Runtime dependencies live under `[project.dependencies]` in
`pyproject.toml`; dev-only tools (test/lint/type-check/security/docs) live
under `[dependency-groups.dev]`.

## Code quality

| Tool | Command | Scope |
|---|---|---|
| [black](https://black.readthedocs.io/) | `uv run black src tests examples` | Formatting |
| [isort](https://pycqa.github.io/isort/) | `uv run isort src tests examples` | Import ordering |
| [flake8](https://flake8.pycqa.org/) | `uv run flake8 src examples` | Style/lint (`max-line-length=128`) |
| [mypy](https://mypy-lang.org/) | `uv run mypy src/rest_ds` | Static type checking (library only) |
| [pytest](https://docs.pytest.org/) | `uv run pytest -v` | Unit/integration tests |

## Security

| Tool | Command | Purpose |
|---|---|---|
| [bandit](https://bandit.readthedocs.io/) | `uv run bandit -r src -c pyproject.toml` | Static security analysis (SQL injection, weak hashes, missing timeouts, etc.) |
| [pip-audit](https://pypi.org/project/pip-audit/) | `uv run pip-audit` | Scans installed dependencies against known CVEs |

Suppress a specific bandit finding only with an inline `# nosec <TEST_ID>`
comment and a one-line justification above it — never a bare `# nosec`
that silences every check on the line.

## Pre-commit

`.pre-commit-config.yaml` wires black, isort, flake8, mypy, and bandit into
git hooks so issues are caught before they reach CI:

```bash
uv run pre-commit install        # one-time setup
uv run pre-commit run --all-files  # run against the whole repo on demand
```

## Documentation

This site is built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
and [mkdocstrings](https://mkdocstrings.github.io/) (for the auto-generated
[API reference](reference.md), pulled directly from docstrings in
`src/rest_ds/`).

```bash
uv run mkdocs serve   # live-reload preview at http://127.0.0.1:8000
uv run mkdocs build --strict  # fail on broken links/nav — run before merging
```

## Directory conventions

- **`src/rest_ds/`** — library code only. No mock servers, no
  `if __name__ == "__main__"` demo scripts, no scenario-specific YAML/JSON.
- **`examples/`** — usage/demo code, one runnable scenario per strategy.
- **`tests/`** — pytest suite; only imports from `src/rest_ds`.

See [Architecture](index.md#project-layout) for the full rationale.
