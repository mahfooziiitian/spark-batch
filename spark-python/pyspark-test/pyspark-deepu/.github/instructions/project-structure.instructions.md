---
applyTo: "{**/setup.py,**/requirements.txt}"
---

# Project Structure — pyspark-deepu

## Dependencies

Use **pip** for dependency management:

```bash
pip install -r requirements.txt       # install runtime + test deps
pip install -e ".[tests]"             # editable install with test extras
```

## Source & Test Layout

```
src/                          ← package root (configured in setup.py)
  constraints/
    suggestions/              ← ConstraintSuggestionRunner scripts
    verifications/            ← VerificationSuite scripts
  mertics/
    computations/
      analyzers/              ← AnalysisRunner scripts
      profiles/               ← ColumnProfilerRunner scripts
    repository/               ← FileSystemMetricsRepository scripts

tests/                        ← pytest test root
  mertics/computations/
    test_analyzers.py
```

**Rules:**
- Test directory mirrors source directory.
- One test file per source module.
- `__init__.py` files in all source packages.
- Shared fixtures in `tests/conftest.py`.

## Adding a New Module

1. Create source file: `src/<domain>/<module>.py`
2. Ensure `__init__.py` exists in all parent directories.
3. Create test file: `tests/<domain>/test_<module>.py`
4. Run `pytest tests/` to validate.

## setup.py

The `setup.py` uses `find_packages(where="src")` and `package_dir={'': 'src'}`.
Add test dependencies under `extras_require`:

```python
extras_require=dict(tests=['pytest'])
```
