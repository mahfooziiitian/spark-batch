# SQL Interface

Use Spark SQL to create tables backed by XML files.

---

## CREATE TABLE USING xml

```python
spark.sql(f"""
    CREATE TABLE movies USING xml
    OPTIONS (
        path 'file:///{xml_file}',
        rootTag 'collection',
        rowTag 'movie'
    )
""")

spark.sql("SELECT * FROM movies").show(truncate=False)
```

> **Source:** `src/spark_xml/sql/spark-databrick-xml-sql.py`

!!! info "Prerequisites"
    - The `spark.jars.packages` config must include the spark-xml JAR
    - Set `spark.sql.warehouse.dir` for table metadata storage

---

## Setup

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
    .config("spark.sql.warehouse.dir", os.environ["SPARK_WAREHOUSE"])
    .appName("spark-xml-sql")
    .getOrCreate()
)
```

---

## SQL with XSD Validation

```python
spark.sql(f"""
    CREATE TABLE orders USING xml
    OPTIONS (
        path 'file:///{xml_file}',
        rowTag 'Root',
        rowValidationXSDPath 'orders.xsd',
        inferSchema 'false'
    )
""")

spark.sql("SELECT * FROM orders").show()
```

---

## SQL Queries

Once registered, use standard SQL:

```sql
-- Filter
SELECT title, year FROM movies WHERE year > 2000

-- Aggregate
SELECT genre, COUNT(*) as cnt FROM movies GROUP BY genre

-- Join with other tables
SELECT m.title, a.name
FROM movies m JOIN actors a ON m.actor_id = a.id
```

---

## DataFrame API vs SQL

=== "DataFrame API"

    ```python
    df = (
        spark.read.format("xml")
        .option("rowTag", "movie")
        .load(xml_file)
    )
    df.filter(df.year > 2000).show()
    ```

=== "Spark SQL"

    ```sql
    CREATE TABLE movies USING xml
    OPTIONS (path '...', rowTag 'movie');

    SELECT * FROM movies WHERE year > 2000;
    ```

Both approaches produce identical results. Use SQL for ad-hoc queries, DataFrame API for programmatic pipelines.
