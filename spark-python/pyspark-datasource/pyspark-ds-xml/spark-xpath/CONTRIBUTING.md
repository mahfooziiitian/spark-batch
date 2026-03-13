# Contributing to Spark XPath

Thank you for your interest in contributing! Here's how to get started.

---

## Development Setup

```bash
# 1. Fork & clone the repository
git clone https://github.com/<your-fork>/spark-xpath.git
cd spark-xpath

# 2. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 3. Install all dependencies (runtime + dev)
uv sync

# 4. Verify everything works
uv run pytest tests/ -v
```

---

## Project Layout

| Directory | Purpose |
|---|---|
| `src/xpath/` | Source code — XPath + PySpark examples |
| `tests/xml/` | Pytest test suite |
| `docs/` | MkDocs documentation (Markdown) |

---

## Making Changes

1. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/my-change
   ```

2. **Write code** in `src/xpath/` and **add tests** in `tests/xml/`.

3. **Run the test suite** before pushing:
   ```bash
   uv run pytest tests/ -v
   ```

4. **Build the docs** to verify your documentation changes:
   ```bash
   uv run mkdocs build --strict
   ```

5. **Commit** with a clear message and **open a Pull Request**.

---

## Code Style

- Follow [PEP 8](https://peps.python.org/pep-0008/) conventions.
- Use type hints where practical.
- Keep Spark sessions scoped — prefer `local[*]` master for examples.

---

## Adding a New Example

1. Create a new `.py` file in the appropriate `src/xpath/` subdirectory.
2. Add a corresponding Markdown page under `docs/examples/`.
3. Register the page in `mkdocs.yml` under the `nav:` section.
4. Add a test in `tests/xml/` to validate the example.

---

## Reporting Issues

Open a GitHub Issue with:
- A clear title and description
- Steps to reproduce (if applicable)
- Expected vs actual behavior
