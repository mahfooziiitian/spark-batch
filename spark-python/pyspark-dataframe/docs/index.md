# PySpark DataFrame API

A **complete reference for the PySpark 3.5 DataFrame API**. Every example is
self-contained, runnable locally with `local[*]`, and follows production-safe
patterns — explicit schemas, env-var paths, and no hardcoded credentials.

## What You Will Learn

```mermaid
graph LR
    A[Create] --> B[Transform]
    B --> C[Join]
    C --> D[Aggregate]
    D --> E[Analyse]
    E --> F[Optimise]
    F --> G[Write]

    style A fill:#e65100,color:#fff
    style B fill:#f57c00,color:#fff
    style C fill:#fb8c00,color:#fff
    style D fill:#ffa726,color:#fff
    style E fill:#ffb74d,color:#000
    style F fill:#ffd54f,color:#000
    style G fill:#ffe082,color:#000
```

| Topic | What It Covers |
|-------|---------------|
| **Creation** | From tuples, dicts, JSON, explicit `StructType`, `toDF()` |
| **Columns** | `withColumn`, `withColumns`, `select`, `when/otherwise`, `toDF` |
| **Joins** | inner, outer (left/right/full), cross, broadcast, self, natural, semi, anti |
| **Window Functions** | Ranking, analytical (lead/lag/first/last), aggregate, frame specification |
| **Pivoting** | `pivot`, dynamic pivot, `unpivot` / `stack` |
| **Aggregations** | `groupBy`, `agg`, `rollup`, `cube`, `countDistinct` |
| **Transformations** | filter, sort, dedup, union, sampling, repartition |
| **Null Handling** | `dropna`, `fillna`, `coalesce`, `isNull`, `eqNullSafe` |
| **Date & Time** | `to_date`, `to_timestamp`, `date_add`, `datediff`, `date_trunc` |
| **String Functions** | `upper/lower`, `concat_ws`, `regexp_extract/replace`, `split` |
| **Optimization** | Caching / persistence, AQE, skew-join handling, salting |
| **Schema** | `StructType`, JSON schema, introspection, `printSchema` |
| **ETL Pipeline** | Extract → Transform → Load pattern with typed functions |

## Quick Start

=== "pip"
    ```bash
    pip install pyspark==3.5.1
    ```

=== "conda"
    ```bash
    conda install -c conda-forge pyspark=3.5.1
    ```

=== "uv"
    ```bash
    uv add pyspark
    ```

```bash
cd spark-python/pyspark-dataframe
python src/data_frame/creation/tuples/dataframe_from_list_of_tuples.py
```

!!! tip "No cluster needed"
    Every script runs on your laptop with `SPARK_MASTER=local[*]` — the default
    when no environment variable is set.

!!! warning "Java required"
    Java 11 must be on your `PATH`. Check with `java -version`.

## Project Layout

```text
spark-python/pyspark-dataframe/
├── src/data_frame/
│   ├── analytical/        # Window functions and pivoting
│   ├── columns/           # Column operations
│   ├── creation/          # DataFrame creation patterns
│   ├── etl/               # End-to-end ETL examples
│   ├── joins/             # All join variants
│   ├── optimization/      # Caching and skew data
│   ├── schema/            # StructType definitions
│   └── transformation/    # filter, sort, dedup, union…
├── tests/                 # pytest test suites
├── docs/                  # This documentation
└── mkdocs.yml
```

## Running Tests

```bash
pytest tests/ -v
```
