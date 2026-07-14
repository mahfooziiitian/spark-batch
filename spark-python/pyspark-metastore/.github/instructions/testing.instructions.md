---
applyTo: "{tests/**/*.py,conftest.py}"
---

# Testing — PySpark Metastore

## SparkSession Fixture

Use a session-scoped fixture with Hive support enabled to test catalog operations:

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    session = (SparkSession.builder
               .appName("metastore-tests")
               .master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.warehouse.dir", warehouse)
               .enableHiveSupport()
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
```

Key points:
- `tmp_path_factory` ensures each test run gets a clean warehouse directory.
- `enableHiveSupport()` activates the Hive catalog with embedded Derby.
- `local[2]` — two threads for deterministic parallelism.
- `spark.ui.enabled=false` — skip the Web UI for speed.

## Test Organisation

Group tests by metastore capability:

```python
class TestCatalogMetadata:    ...  # SHOW CATALOGS, currentCatalog, listCatalogs
class TestDatabaseOps:        ...  # CREATE/DROP DATABASE, SHOW DATABASES
class TestTableOps:           ...  # CREATE/DROP TABLE, SHOW TABLES, saveAsTable
class TestNamespaceResolution:...  # Three-level namespace, USE CATALOG, USE DATABASE
class TestWarehouse:          ...  # warehouse.dir, managed vs external tables
class TestHiveMetastore:      ...  # Hive-specific features (partitions, bucketing)
```

## Catalog Assertions

### Verify catalog existence

```python
def test_default_catalog_exists(self, spark):
    catalogs = [c.name for c in spark.catalog.listCatalogs()]
    assert "spark_catalog" in catalogs
```

### Verify database creation

```python
def test_create_database(self, spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    databases = [db.name for db in spark.catalog.listDatabases()]
    assert "test_db" in databases
    spark.sql("DROP DATABASE IF EXISTS test_db CASCADE")
```

### Verify table lifecycle

```python
def test_create_and_drop_table(self, spark):
    spark.sql("CREATE TABLE IF NOT EXISTS default.test_tbl (id INT, name STRING)")
    tables = [t.name for t in spark.catalog.listTables("default")]
    assert "test_tbl" in tables

    spark.sql("DROP TABLE IF EXISTS default.test_tbl")
    tables = [t.name for t in spark.catalog.listTables("default")]
    assert "test_tbl" not in tables
```

### Verify table data with saveAsTable

```python
def test_save_as_table(self, spark):
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, ["id", "name"])
    df.write.mode("overwrite").saveAsTable("default.people")

    result = spark.sql("SELECT * FROM default.people")
    assert result.count() == 2
    assert set(result.columns) == {"id", "name"}

    spark.sql("DROP TABLE IF EXISTS default.people")
```

### Verify catalog metadata helper

```python
def test_catalog_metadata(self, spark):
    from metastore.catalog_metadata import print_catalog_metadata

    meta = print_catalog_metadata(spark)
    assert "defaultCatalog" in meta
    assert "currentCatalog" in meta
    assert meta["currentCatalog"] == "spark_catalog"
```

## DataFrame Comparisons with chispa

The project includes `chispa` for DataFrame equality assertions:

```python
from chispa.dataframe_comparer import assert_df_equality

def test_transform(self, spark):
    input_df = spark.createDataFrame([(1, "a")], ["id", "val"])
    expected = spark.createDataFrame([(1, "a")], ["id", "val"])
    assert_df_equality(input_df, expected)
```

## Cleanup Pattern

Always drop tables and databases created during tests:

```python
@pytest.fixture
def sample_table(spark):
    spark.sql("CREATE TABLE IF NOT EXISTS default.sample (id INT)")
    spark.sql("INSERT INTO default.sample VALUES (1), (2), (3)")
    yield "default.sample"
    spark.sql("DROP TABLE IF EXISTS default.sample")
```

## Markers

Use the markers defined in `pyproject.toml`:

```python
@pytest.mark.unit
def test_catalog_list(spark): ...

@pytest.mark.integration
def test_remote_hive_metastore(spark): ...
```

## Running Tests

```bash
uv run task test                           # all tests
uv run pytest tests/ -v -k "catalog"       # filter by keyword
uv run pytest tests/ -m "unit"             # unit tests only
uv run pytest tests/ -m "integration"      # integration tests only
```

## Entry Point

Always include a direct-run entry point:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```

## CI Environment Variables

```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
```
