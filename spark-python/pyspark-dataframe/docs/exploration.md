# Data Exploration

Explore, profile, and validate DataFrames before building transformation pipelines.
Data exploration (EDA) answers the questions you need before writing any business logic:
_What does the schema look like? How many nulls? Any duplicates? What is the value distribution?_

## Exploration Workflow

```mermaid
graph LR
    A[Raw Data] --> B[Schema Inspection]
    B --> C[Descriptive Statistics]
    C --> D[Null Analysis]
    D --> E[Distribution & Frequency]
    E --> F[Correlation Analysis]
    F --> G[Duplicate Detection]
    G --> H[Data Profiling Report]

    style A fill:#4a9eff,color:#fff
    style H fill:#2ecc71,color:#fff
```

Each step builds on the previous one — start with the schema to understand column types,
then progressively drill into statistics, quality issues, and relationships.

## API Quick Reference

| Task | Key API / Pattern | Source file |
|------|-------------------|-------------|
| Print schema tree | `df.printSchema()` | `df_inspect.py` |
| Column names and types | `df.dtypes`, `df.columns` | `df_inspect.py` |
| Schema field metadata | `df.schema.fields`, `df.schema[col].nullable` | `schema_analyzer.py` |
| Count / mean / stddev / min / max | `df.describe()`, `df.summary()` | `statistics.py` |
| Custom per-column aggregations | `df.agg(F.min(), F.max(), F.avg(), F.stddev())` | `statistics.py` |
| Null counts per column | `F.count(F.when(F.col(c).isNull(), c))` | `null_analysis.py` |
| Rows with any null | `F.greatest(isNull casts…) > 0` | `null_analysis.py` |
| Approximate quantiles | `df.stat.approxQuantile(col, probs, error)` | `distribution.py` |
| Histogram buckets | `rdd.histogram(n)` | `distribution.py` |
| Box-plot summary per group | `percentile_approx` inside `groupBy.agg` | `distribution.py` |
| Distinct counts / cardinality | `df.select(col).distinct().count()` | `value_frequency.py` |
| Top-N frequent values | `groupBy(col).count().orderBy(desc)` | `value_frequency.py` |
| Pearson correlation matrix | `df.stat.corr(col1, col2)` | `correlation.py` |
| Fully duplicate rows | `df.count() - df.distinct().count()` | `duplicate_detection.py` |
| Key-level duplicates | `groupBy(key).count().filter(count > 1)` | `duplicate_detection.py` |
| Near-duplicates on subset | `groupBy(subset).count().filter(count > 1)` | `duplicate_detection.py` |
| Full column profile report | `profile(df)` | `data_profile.py` |
| Date-range overlap detection | Cross-join with overlap condition | `overlap_data_df.py` |

---

## Schema & Structure Inspection

The starting point for every exploration session — understand the shape, columns, and
types before touching any data.

```python
from pyspark.sql import functions as F

df.printSchema()                                   # (1)!

print(f"Shape: {df.count()} rows × {len(df.columns)} columns")

for name, dtype in df.dtypes:                      # (2)!
    print(f"  {name:20s} {dtype}")

print(f"Total rows:    {df.count()}")
print(f"Distinct rows: {df.distinct().count()}")   # (3)!
```
1. Tree-formatted schema showing column names, types, and nullability.
2. `df.dtypes` returns a list of `(column_name, type_string)` tuples.
3. Quick check — if distinct < total, duplicates exist.

For programmatic schema analysis, access field-level metadata:

```python
for field in df.schema.fields:
    print(f"{field.name}: {field.dataType}  nullable={field.nullable}")

assert df.schema["revenue"].nullable is True
```

### Run

```bash
cd spark-python/pyspark-dataframe
python src/data_frame/exploration/df_inspect.py
```

---

## Descriptive Statistics

Spark provides two built-in methods — `describe()` for the basics and `summary()` for
percentiles — plus you can build custom per-column aggregations.

=== "describe()"

    ```python
    df.describe().show(truncate=False)             # (1)!
    ```
    1. Returns count, mean, stddev, min, and max for every numeric (and string) column.

=== "summary()"

    ```python
    df.summary().show(truncate=False)              # (1)!
    ```
    1. Extends `describe()` with 25th, 50th (median), and 75th percentile rows.

=== "Custom aggregations"

    ```python
    from pyspark.sql import functions as F

    num_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, (IntegerType, LongType, DoubleType))
    ]

    aggs = []
    for col in num_cols:
        aggs += [
            F.min(col).alias(f"{col}_min"),
            F.max(col).alias(f"{col}_max"),
            F.round(F.avg(col), 2).alias(f"{col}_mean"),       # (1)!
            F.round(F.stddev(col), 2).alias(f"{col}_stddev"),
        ]
    df.agg(*aggs).show(truncate=False)
    ```
    1. `F.round(..., 2)` avoids floating-point noise in the output.

### Run

```bash
python src/data_frame/exploration/statistics.py
```

---

## Null Analysis

Essential before deciding on an imputation or filtering strategy — know exactly where
the gaps are and how severe they are.

```python
from pyspark.sql import functions as F

total = df.count()

aggs = [                                                            # (1)!
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
]
null_row = df.agg(*aggs).collect()[0]

for col in df.columns:
    n = null_row[col]
    pct = round(n / total * 100, 1)
    bar = "█" * int(pct / 5)
    print(f"  {col:20s} {n:3d} nulls  ({pct:5.1f}%)  {bar}")     # (2)!
```
1. A single aggregation pass counts nulls across all columns simultaneously.
2. ASCII bar chart gives instant visual feedback on null density.

### Rows with at least one null

```python
any_null_condition = (                                              # (1)!
    F.greatest(*[F.col(c).isNull().cast("int") for c in df.columns]) > 0
)
any_null = df.filter(any_null_condition)
print(f"{any_null.count()} rows contain at least one null")
any_null.show(truncate=False)
```
1. `F.greatest()` returns the max across columns — if any `isNull` cast equals 1, the row
   has at least one null.

### Run

```bash
python src/data_frame/exploration/null_analysis.py
```

---

## Distribution & Frequency

### Approximate quantiles

```python
QUANTILES = [0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 1.0]
REL_ERROR = 0.01                                                    # (1)!

quantile_vals = df.stat.approxQuantile("revenue", QUANTILES, REL_ERROR)
for label, val in zip(["min","p10","p25","p50","p75","p90","max"], quantile_vals):
    print(f"  {label:5s}: {val:.2f}")
```
1. `REL_ERROR` controls approximation accuracy — lower values are more accurate but
   require more memory. `0.01` is a good default.

### Histogram

```python
BUCKETS = 5

buckets, counts = (                                                 # (1)!
    df.select("revenue").rdd.flatMap(lambda row: [row[0]]).histogram(BUCKETS)
)
max_count = max(counts)
for i, count in enumerate(counts):
    lo, hi = f"{buckets[i]:.0f}", f"{buckets[i + 1]:.0f}"
    bar = "█" * int(count / max_count * 30)
    print(f"  [{lo:>6s} – {hi:>6s}]  {count:3d}  {bar}")
```
1. `rdd.histogram(n)` returns bucket boundaries and counts — a quick way to see the
   shape of a distribution.

### Box-plot summary per category

```python
from pyspark.sql import functions as F

(df
 .groupBy("category")
 .agg(
     F.min("revenue").alias("min"),
     F.expr("percentile_approx(revenue, 0.25)").alias("q1"),       # (1)!
     F.expr("percentile_approx(revenue, 0.50)").alias("median"),
     F.expr("percentile_approx(revenue, 0.75)").alias("q3"),
     F.max("revenue").alias("max"),
     F.round(F.avg("revenue"), 2).alias("mean"),
 )
 .orderBy("category")
 .show(truncate=False))
```
1. `percentile_approx` computes approximate percentiles within each group — use it inside
   `groupBy.agg` for per-category summaries.

### Value frequency and cardinality

```python
total = df.count()

for col in df.columns:
    n_distinct = df.select(col).distinct().count()
    cardinality = "high" if n_distinct > total * 0.5 else "low"    # (1)!
    print(f"  {col:20s} {n_distinct:4d} distinct  [{cardinality}]")
```
1. High cardinality (> 50 % of total rows) suggests a unique identifier or near-unique
   column. Low cardinality suggests a categorical column.

### Top-N frequent values

```python
TOP_N = 5

cat_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]

for col in cat_cols:
    (df
     .groupBy(col)
     .agg(
         F.count("*").alias("count"),
         F.round(F.count("*") / total * 100, 1).alias("pct"),     # (1)!
     )
     .orderBy(F.desc("count"))
     .limit(TOP_N)
     .show(truncate=False))
```
1. Percentage column makes it easy to spot dominant values or skew at a glance.

### Run

```bash
python src/data_frame/exploration/distribution.py
python src/data_frame/exploration/value_frequency.py
```

---

## Duplicate Detection

Identify fully duplicate rows, key-level duplicates, and near-duplicates based on a
subset of columns.

```python
from pyspark.sql import functions as F

total = df.count()

# --- Fully duplicate rows ---
fully_duped = total - df.distinct().count()                         # (1)!
print(f"Fully duplicate rows: {fully_duped}")
df.groupBy(df.columns).count().filter(F.col("count") > 1).show()

# --- Key-level duplicates ---
key_col = "order_id"
key_dupes = df.groupBy(key_col).count().filter(F.col("count") > 1) # (2)!
print(f"Duplicate {key_col}s: {key_dupes.count()}")

# --- Near-duplicates on a subset of columns ---
subset = ["customer_id", "product", "amount"]
near_dupes = (df
    .groupBy(subset)                                                # (3)!
    .count()
    .filter(F.col("count") > 1))
print(f"Near-duplicate groups on {subset}: {near_dupes.count()}")

# --- Deduplicated result ---
deduped = df.dropDuplicates()
print(f"After dedup: {deduped.count()} rows")
```
1. Subtracting distinct count from total count gives the number of exact-duplicate rows.
2. Key-level duplicates — same primary key appears more than once (possible data ingestion issue).
3. Near-duplicates share the same values on a logical subset but differ on other columns
   (e.g. different `order_id` but same customer, product, and amount).

### Run

```bash
python src/data_frame/exploration/duplicate_detection.py
```

---

## Correlation Analysis

Compute a Pearson correlation matrix for all numeric column pairs to identify redundant
features or potential data leakage.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

num_cols = [
    f.name for f in df.schema.fields
    if isinstance(f.dataType, (IntegerType, LongType, DoubleType))
]

# --- Full correlation matrix ---
for c1 in num_cols:                                                  # (1)!
    row_str = f"{c1:20s}"
    for c2 in num_cols:
        corr = df.stat.corr(c1, c2)
        row_str += f"{corr:12.4f}"
    print(row_str)

# --- Flag highly correlated pairs ---
for i, c1 in enumerate(num_cols):
    for c2 in num_cols[i + 1:]:                                      # (2)!
        corr = df.stat.corr(c1, c2)
        if abs(corr) > 0.5:
            print(f"  {c1} ↔ {c2}: r = {corr:.4f}")
```
1. `df.stat.corr(col1, col2)` computes the Pearson correlation coefficient — each call
   triggers a Spark job, so the cost is O(n²) in columns.
2. Only check the upper triangle to avoid computing each pair twice.

!!! warning "Performance on wide DataFrames"
    The correlation matrix computes `n × (n-1) / 2` Spark jobs for `n` numeric columns.
    For wide DataFrames (50+ columns), consider sampling first with
    `df.sample(fraction=0.1)` or computing correlations on a subset of columns.

### Run

```bash
python src/data_frame/exploration/correlation.py
```

---

## Data Profiling

Combine schema, nulls, distinct counts, and basic statistics into a single profile
report — a one-stop first look at any new dataset.

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

_NUMERIC = (IntegerType, LongType, DoubleType)


def profile(df: DataFrame, sample_size: int = 5) -> None:         # (1)!
    total = df.count()
    print(f"Rows: {total}  |  Columns: {len(df.columns)}")

    for field in df.schema.fields:
        col = field.name
        dtype = field.dataType

        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = round(null_count / total * 100, 1) if total else 0
        n_distinct = df.select(col).distinct().count()

        if isinstance(dtype, tuple(_NUMERIC)):                     # (2)!
            row = df.agg(F.min(col), F.max(col)).first()
            col_min, col_max = str(row[0]), str(row[1])
        elif isinstance(dtype, StringType):                        # (3)!
            top = df.groupBy(col).count().orderBy(F.desc("count")).first()
            col_min = top[col] if top else "—"
            col_max = "—"
        else:
            col_min = col_max = "—"

        print(f"  {col:20s} {null_count:7d} nulls ({null_pct:.1f}%)  "
              f"{n_distinct:9d} distinct  [{col_min} .. {col_max}]")

    df.show(sample_size, truncate=True)
```
1. Call `profile(df)` on any DataFrame for a quick quality assessment.
2. For numeric columns, the report shows min and max values.
3. For string columns, the report shows the most frequent (top) value.

The `DataFrameProfiler` class provides the same capabilities with a reusable API:

```python
from data_frame.exploration.data_profile import DataFrameProfiler

stats = DataFrameProfiler.basic_stats(df)       # summary() for numeric cols
stats.show()

profile = DataFrameProfiler.column_profiling(df) # per-column detail dict
for col, info in profile.items():
    print(f"{col}: {info}")

corr = DataFrameProfiler.correlation_analysis(df, "revenue", "quantity")
print(f"Pearson r = {corr:.4f}")
```

### Run

```bash
python src/data_frame/exploration/data_profile.py
```

---

## Dataset Overlap / Comparison

Detect overlapping date ranges between rows using a cross-join with a boundary condition —
useful for scheduling conflicts, insurance policy overlaps, or SLA coverage analysis.

```python
from pyspark.sql import functions as F

data = [
    ("1-Jan-23", "10-Jan-23"),
    ("13-Jan-23", "18-Jan-23"),
    ("8-Jan-23", "11-Jan-23"),
]
df = spark.createDataFrame(data, ["Start_Date", "End_Date"])

df = (df
    .withColumn("Start_Date", F.col("Start_Date").cast("timestamp"))
    .withColumn("End_Date", F.col("End_Date").cast("timestamp")))

df2 = (df                                                          # (1)!
    .withColumnRenamed("Start_Date", "Start_Date_2")
    .withColumnRenamed("End_Date", "End_Date_2"))

cross_df = df.crossJoin(df2)                                       # (2)!

condition = (
    (F.col("Start_Date") <= F.col("End_Date_2"))
    & (F.col("End_Date") >= F.col("Start_Date_2"))                 # (3)!
    & (F.col("End_Date") != F.col("End_Date_2"))
)

result_df = cross_df.withColumn("overlap_ind", condition).select(
    "Start_Date", "End_Date", "Start_Date_2", "End_Date_2", "overlap_ind"
)
result_df.show(truncate=False)
```
1. Rename columns on the second copy to avoid ambiguous references after the cross-join.
2. Cross-join produces the Cartesian product — every row paired with every other row.
3. Two intervals `[A, B]` and `[C, D]` overlap when `A ≤ D` and `B ≥ C`. The third
   condition excludes self-comparisons.

!!! warning "Cross-join cost"
    Cross-joins produce `N²` rows. For large DataFrames, filter or sample before
    cross-joining, or use a windowed self-join approach instead.

### Run

```bash
python src/data_frame/exploration/overlap_data/overlap_data_df.py
```

---

## Best Practices

!!! tip "EDA checklist for new datasets"
    1. **Schema** — run `printSchema()` and check types make sense
    2. **Shape** — `df.count()` and `len(df.columns)` for row × column dimensions
    3. **Nulls** — count nulls per column before any transformation
    4. **Duplicates** — check both exact and key-level duplicates
    5. **Statistics** — `describe()` or `summary()` to spot outliers
    6. **Distributions** — quantiles and frequency tables for skew detection
    7. **Correlations** — flag highly correlated pairs (|r| > 0.8)

!!! warning "Performance on large datasets"
    - `df.distinct().count()` and `df.select(col).distinct().count()` trigger shuffles —
      use `approx_count_distinct()` for quick estimates on billion-row tables.
    - `df.stat.corr()` launches one Spark job per column pair — sample first for wide
      DataFrames.
    - `rdd.histogram()` forces a full RDD evaluation — avoid on very large datasets.

!!! note "Spark-specific behaviour"
    - `describe()` and `summary()` return string columns for all statistics — cast the
      result columns if you need numeric comparisons.
    - `approxQuantile` uses the Greenwald-Khanna algorithm — the `relativeError` parameter
      controls the trade-off between accuracy and memory usage.
    - Null values are excluded from `count(col)` but included in `count("*")`.

---

## Full Source

### df_inspect.py

```python title="src/data_frame/exploration/df_inspect.py"
--8<-- "data_frame/exploration/df_inspect.py"
```

### statistics.py

```python title="src/data_frame/exploration/statistics.py"
--8<-- "data_frame/exploration/statistics.py"
```

### null_analysis.py

```python title="src/data_frame/exploration/null_analysis.py"
--8<-- "data_frame/exploration/null_analysis.py"
```

### distribution.py

```python title="src/data_frame/exploration/distribution.py"
--8<-- "data_frame/exploration/distribution.py"
```

### value_frequency.py

```python title="src/data_frame/exploration/value_frequency.py"
--8<-- "data_frame/exploration/value_frequency.py"
```

### duplicate_detection.py

```python title="src/data_frame/exploration/duplicate_detection.py"
--8<-- "data_frame/exploration/duplicate_detection.py"
```

### correlation.py

```python title="src/data_frame/exploration/correlation.py"
--8<-- "data_frame/exploration/correlation.py"
```

### data_profile.py

```python title="src/data_frame/exploration/data_profile.py"
--8<-- "data_frame/exploration/data_profile.py"
```

### schema_analyzer.py

```python title="src/data_frame/exploration/schema_analyzer.py"
--8<-- "data_frame/exploration/schema_analyzer.py"
```

### overlap_data_df.py

```python title="src/data_frame/exploration/overlap_data/overlap_data_df.py"
--8<-- "data_frame/exploration/overlap_data/overlap_data_df.py"
```
