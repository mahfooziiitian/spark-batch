# Project Structure

## Layout

```
pyspark-chispa/
├── src/
│   └── data_frame/
│       ├── columns/              # Column-level transformations
│       │   └── column_equality.py
│       ├── equality/             # DataFrame comparison utilities
│       │   └── df_equality.py
│       ├── functions/            # Arithmetic column functions
│       │   └── functions.py
│       ├── helper/               # Pure Python helpers (no Spark)
│       │   └── string_helper.py
│       ├── schema/               # Schema utilities
│       │   └── schema_utils.py
│       └── transformation/       # DataFrame transformations
│           └── df_transformations.py
├── tests/
│   ├── conftest.py               # Shared SparkSession fixture
│   ├── columns/
│   │   └── test_column_equality.py
│   ├── equality/
│   │   └── test_df_equality.py
│   ├── functions/
│   │   └── test_functions.py
│   ├── schema/
│   │   └── test_schema.py
│   └── transformation/
│       └── test_df_transformation.py
├── docs/                         # MkDocs documentation
├── mkdocs.yml
└── pyproject.toml                # All configuration
```

## Design Principles

### Source mirrors tests

Every source module has a corresponding test file in the same relative path:

```
src/data_frame/columns/column_equality.py
    → tests/columns/test_column_equality.py
```

### Separation of concerns

| Package | Spark dependency | Description |
| --- | --- | --- |
| `columns/` | Yes | Column → Column transformations |
| `functions/` | Yes | Arithmetic Column functions |
| `equality/` | Yes | DataFrame comparison utilities |
| `transformation/` | Yes | DataFrame → DataFrame transforms |
| `schema/` | Yes | Schema inspection and conversion |
| `helper/` | **No** | Pure Python string utilities |

!!! note
    Functions in `helper/` have no PySpark dependency and can be tested
    without a SparkSession, making them fast and portable.

### Single conftest.py

All test files share a single `SparkSession` fixture defined in `tests/conftest.py`:

```python title="tests/conftest.py"
--8 < --"tests/conftest.py"
```

1. `local[2]` — two threads, deterministic and fast.
2. `shuffle.partitions=2` — default 200 is wasteful for test data.
3. `ui.enabled=false` — skip Spark Web UI to speed up tests.
4. `setLogLevel("ERROR")` — suppress everything except errors.

!!! warning "Never create SparkSession in test files"
    Always inject via the `spark` parameter. Duplicating the fixture
    causes JVM conflicts and race conditions.

### Test class organisation

Tests are grouped into classes by function or concept:

```python
class TestRemoveNonWordCharacters:  # one class per function
    def test_removes_special_characters(self, spark): ...
    def test_preserves_digits(self, spark): ...
    def test_handles_nulls(self, spark): ...
```
