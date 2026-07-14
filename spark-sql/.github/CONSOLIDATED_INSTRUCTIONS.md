# GitHub Copilot Instructions — spark-sql (Consolidated)

A **Spark SQL learning resource** — tutorials, demos, architecture, and optimization
for **Apache Spark 3.5** and **Databricks Runtime**.

> Items marked **[Databricks]** use Databricks-only features and may not work on open-source Spark.

| Directory | Content |
|-----------|---------|
| `src/` | Spark SQL (`.sql`) and PySpark (`.py`) examples by topic |
| `tests/` | pytest + chispa test suite |
| `docs/` | MkDocs Material documentation site |
| `pyproject.toml` | Single config for all tools and tasks |

---

## 1. Quality & Tooling

All tasks: **`uv run task <name>`**. Config lives exclusively in `pyproject.toml` — never
create `.flake8`, `setup.cfg`, `.mypy.ini`, `.bandit`, `.isort.cfg`, or `ruff.toml`.

| Command | Purpose |
|---------|---------|
| `uv run task quality` | Full pipeline: import → format → lint → type_check → sql |
| `uv run task test` | pytest -vv tests/ |
| `uv run task docs_build` | MkDocs strict build (zero warnings) |
| `uv run task secure` | bandit + safety |
| `uv run task sql` | SQLFluff fix + lint |

**Pre-commit gate:**

```bash
uv run task quality && uv run task docs_build && uv run task test
```

| Setting | Value |
|---------|-------|
| Max line length | 128 |
| Python target | 3.11 |
| SQL dialect | Databricks |
| Coverage minimum | 60% |

**Dependencies:** `uv add <pkg>` (runtime), `uv add --group dev <pkg>` (dev). Always commit `uv.lock`.

**Security:** Bandit scans `src/` only. Never suppress without `# nosec: <justification>`. Never commit secrets.

---

## 2. Python & PySpark (`src/**/*.py`, `tests/**/*.py`)

PySpark executes and validates Spark SQL — most logic lives in `.sql` files.

### SparkSession

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("descriptive-job-name")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")   # use "ERROR" in tests
```

- Never create SparkSession inside library functions — pass as parameter.
- Always `spark.stop()` at end of standalone scripts.
- Use `if __name__ == "__main__":` guard.

### Imports

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F        # always alias as F — never star-import
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType
```

### Code Rules

- Type hints on **all** function signatures.
- No `print()` — use `logging.getLogger(__name__)`.
- No bare `except` — catch specific exceptions.
- No code at module level (no side effects on import).
- No `inferSchema=True` — define schemas explicitly.
- Prefer `F.col("name")` over `df["name"]`.
- Chain transformations — don't reassign variables.
- Don't mix SQL and DataFrame styles in the same function.

### Environment Variables

```python
INPUT_PATH   = os.environ.get("INPUT_PATH")           # None → use in-memory sample
OUTPUT_PATH  = os.environ.get("OUTPUT_PATH", "/tmp/spark_output")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
```

### Output

- Prefer Parquet. Partition large outputs: `.partitionBy("year_month")`.
- CSV only for non-technical audiences.

---

## 3. Spark SQL (`src/**/*.sql`)

### Formatting

- Keywords: `UPPER`. Identifiers: `lower`.
- One clause per line. Columns indented under `SELECT`.
- CTE names: `snake_case`. Prefer CTEs over nested subqueries.

### File Header

```sql
-- ============================================================
-- Topic: <topic summary>
-- Dialect: Databricks / Spark SQL 3.5
-- Description: <what this file demonstrates>
-- ============================================================
```

### SCD Conventions

- Row hash: `md5(concat_ws('||', col1, col2, ...))`.
- Null-safe comparison: `<=>` operator.
- **SCD Type 2/6 require two-step MERGE** — one MERGE cannot expire and insert for the same key.

### Performance

- Push `WHERE` filters before joins/aggregations.
- Filter on partition columns when available.
- Use `/*+ BROADCAST(dim) */` for small dimensions (< 10 MB).
- Avoid UDFs in `WHERE` — they disable predicate pushdown.
- Prefer window functions over self-joins.

### Complex Types

- `TRANSFORM` / `FILTER` / `AGGREGATE` for array/map HOFs.
- `array_contains` over `explode` for membership checks.
- `LATERAL VIEW explode(...)` when one row per element is needed.

### NULL Handling

- Always `IS NULL` / `IS NOT NULL` — never `= NULL`.
- Use `COALESCE(col, default)` over `IFNULL`.
- Use `<=>` in join conditions that may contain NULLs.

---

## 4. Databricks-Specific (`src/**/*.sql`, `docs/**/*.md`) [Databricks]

Label with `[Databricks]` in docs, `-- [Databricks]` in SQL, `# [Databricks]` in Python.

### Delta Lake DML

- `UPDATE`, `DELETE`, `MERGE INTO` require Delta tables.
- **Deduplicate source before MERGE** — Delta errors on multiple matches.

```sql
-- [Databricks] Delta MERGE
MERGE INTO target AS t
USING (SELECT * FROM source WHERE rn = 1) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Table Optimization

```sql
-- [Databricks] Compact + co-locate
OPTIMIZE table_name ZORDER BY (join_key);
VACUUM table_name RETAIN 168 HOURS;
```

### Unity Catalog

- Three-part names: `catalog.schema.table`.
- `GRANT` / `REVOKE` for access control.

### Databricks-Only Functions

`ai_generate_text()`, `read_files()`, `cloud_files()`, `h3_*()`.

### Labeling Convention

```markdown
!!! note "[Databricks] Delta Lake Required"
    This pattern requires Delta tables.
```

---

## 5. Testing (`**/test_*.py`)

### SparkSession Fixture

Session-scoped in `tests/conftest.py`:

```python
@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-session")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

### Assertions

```python
from chispa.dataframe_comparer import assert_df_equality
assert_df_equality(actual, expected, ignore_row_order=True)
```

### SCD Test Cases (mandatory)

1. New record inserted
2. Changed record versioned/updated
3. Unchanged record skipped
4. Idempotent on rerun
5. No duplicate active rows

### Rules

- Files: `test_*.py`. Functions: `test_*`. Group in classes.
- No `df.show()` or `df.printSchema()` in tests.
- No disk writes in unit tests — assert in memory. Use `tmp_path` for I/O tests.
- Markers: `unit`, `integration`, `slow` (declared in `pyproject.toml`).

---

## 6. Documentation (`docs/**/*.md`, `mkdocs.yml`)

### Build

```bash
uv run task docs_build   # strict — zero warnings
uv run task docs_serve   # localhost:8080
```

### Navigation

- Every `docs/` directory must have a `.pages` file with `title:` and `nav:`.
- **Never** add `nav:` to `mkdocs.yml` — awesome-pages plugin owns navigation.

### Page Template

```markdown
# :material-xxx: Title

One-sentence description.

---

## :material-code-tags: Syntax

## :material-information-outline: Behavior

## :material-flask-outline: Practical Examples

## :material-lightbulb-outline: When to Use
```

### Rules

1. Only `:material-xxx:` icons — **no Unicode emoji**.
2. SQL fences: ` ```sql ` (lowercase).
3. Links: relative paths to `.md` files — never directories, never absolute URLs.
4. Admonitions: `!!! tip`, `!!! note`, `!!! warning`, `!!! success`, `!!! failure`.
5. Tables: GFM pipe syntax. Sections separated by `---`.

### Interactive Visualizations (D3.js)

- `<div id="viz-<name>" class="ts-viz"></div>` in `## :material-animation-play: Interactive Demo`.
- Use `document$.subscribe(init)` for instant-navigation compatibility.

### Common Build Fixes

| Warning | Fix |
|---------|-----|
| Unrecognized relative link | Count `../` depth carefully |
| Directory-style link | Append `/index.md` |
| Omitted file | Add to directory's `.pages` |
