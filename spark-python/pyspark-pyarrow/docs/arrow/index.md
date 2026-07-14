# Arrow Conversions

Apache Arrow enables zero-copy columnar data transfers between the JVM and Python,
making Pandas ↔ Spark conversions significantly faster.

```mermaid
graph LR
    A[Pandas DataFrame] -->|createDataFrame| B[Spark DataFrame]
    B -->|toPandas| A
    B -->|mapInPandas| C[Batch-level Transform]
    B -->|applyInPandas| D[Group-level Transform]
    style A fill:#f9f,stroke:#333
    style B fill:#bbf,stroke:#333
```

## Enable Arrow

```python
spark = (SparkSession.builder
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")       # (1)!
         .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")  # (2)!
         .getOrCreate())
```

1. Enables Arrow for `createDataFrame` and `toPandas`.
2. Falls back to non-Arrow path if Arrow conversion fails.

## Configuration Reference

| Config | Default | Description |
|--------|---------|-------------|
| `spark.sql.execution.arrow.pyspark.enabled` | `false` | Enable Arrow optimization |
| `spark.sql.execution.arrow.pyspark.fallback.enabled` | `true` | Fall back on Arrow failure |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `10000` | Rows per Arrow batch |

## Topics

| Page | What it covers |
|------|---------------|
| [Pandas ↔ Spark](pandas-spark.md) | `createDataFrame` and `toPandas` with Arrow |
| [mapInPandas](map-in-pandas.md) | Row-wise batch transformations |
| [applyInPandas](apply-in-pandas.md) | Grouped map operations |

## Run

```bash
python src/psa/pyspark_pyarrow.py
```
