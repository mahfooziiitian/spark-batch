---
applyTo: ".github/workflows/**/*.yml,.github/workflows/**/*.yaml"
---

# GitHub Actions CI Conventions

## Workflow Location

All workflows live in `.github/workflows/`. The primary workflow is `ci.yml`.

## Pipeline Structure

```
push / PR to main
      │
      ▼
  ┌────────┐    ┌────────┐
  │  test   │───▶│  docs  │
  │ (matrix)│    │ (build)│
  └────────┘    └────────┘
```

- **test** — runs pytest across the Python version matrix.
- **docs** — builds MkDocs site with `--strict` (depends on test).

## Conventions

### Runner

- Use `ubuntu-latest` for all jobs.

### Python Versions

- Test against **3.11** and **3.12** via matrix strategy:
  ```yaml
  strategy:
    matrix:
      python-version: ["3.11", "3.12"]
  ```

### Dependency Installation

- Use **uv** (via `astral-sh/setup-uv@v5`), never pip:
  ```yaml
  - name: Install uv
    uses: astral-sh/setup-uv@v5

  - name: Set up Python ${{ matrix.python-version }}
    run: uv python install ${{ matrix.python-version }}

  - name: Install dependencies
    run: uv sync
  ```

### Test Step

```yaml
- name: Run tests
  run: uv run pytest tests/ -v --tb=short
```

### Docs Build Step

```yaml
- name: Build docs
  run: uv run mkdocs build --strict
```

### Permissions

- Use minimal permissions — `contents: read` for checkout-only workflows.

### Action Versions

- Pin actions to **major version tags** (e.g., `@v4`, `@v5`).
- Use `actions/checkout@v4` for repository checkout.

## Adding a New Job

1. Define the job in `ci.yml` under the `jobs:` key.
2. Set `needs:` to declare dependencies on other jobs.
3. Use the same uv setup pattern shown above.
4. Keep job names short and descriptive (e.g., `test`, `docs`, `lint`).

## Secrets & Environment Variables

- Never hard-code secrets — use `${{ secrets.NAME }}`.
- For `DATA_HOME` or similar paths, set them in the `env:` block of the step
  that needs them.
