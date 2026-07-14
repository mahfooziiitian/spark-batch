# :material-chart-bar: Statistical Aggregations

Spark SQL provides a suite of statistical aggregate functions for measuring dispersion, correlation, and distribution of numeric data.

---

## :material-pin: Functions

| Function | Syntax | Description |
|----------|--------|-------------|
| `STDDEV` | `STDDEV(expr)` | Sample standard deviation (alias: `STDDEV_SAMP`) |
| `STDDEV_POP` | `STDDEV_POP(expr)` | Population standard deviation |
| `VARIANCE` | `VARIANCE(expr)` | Sample variance (alias: `VAR_SAMP`) |
| `VAR_POP` | `VAR_POP(expr)` | Population variance |
| `COVAR_SAMP` | `COVAR_SAMP(y, x)` | Sample covariance of two columns |
| `COVAR_POP` | `COVAR_POP(y, x)` | Population covariance of two columns |
| `CORR` | `CORR(y, x)` | Pearson correlation coefficient (−1 to 1) |
| `PERCENTILE_APPROX` | `PERCENTILE_APPROX(expr, p [, accuracy])` | Approximate percentile via HyperLogLog; `p` can be a scalar or array |
| `MEDIAN` | `MEDIAN(expr)` | Exact median (50th percentile); alias for `PERCENTILE(expr, 0.5)` |
| `KURTOSIS` | `KURTOSIS(expr)` | Excess kurtosis (tailedness of the distribution) |
| `SKEWNESS` | `SKEWNESS(expr)` | Skewness (asymmetry of the distribution) |

---

## :material-magnify: Behavior

1. **Sample vs population** — `STDDEV` / `VARIANCE` divide by `n − 1` (Bessel's correction for a sample); `STDDEV_POP` / `VAR_POP` divide by `n`. Use sample variants when working with a subset drawn from a larger population.
2. **NULL handling** — all statistical functions ignore `NULL` values; if every input is `NULL` the result is `NULL`.
3. **FILTER support** — all aggregate functions accept `FILTER (WHERE condition)` to scope the computation to a subset of rows without a separate subquery.
4. **`PERCENTILE_APPROX` accuracy** — the optional third argument controls the accuracy trade-off (default `10000`); higher values are more accurate but use more memory. Pass an array such as `ARRAY(0.25, 0.5, 0.75)` to compute multiple percentiles in one pass.
5. **`CORR` range** — returns values in `[−1, 1]`; returns `NULL` when the input has fewer than 2 non-NULL pairs or when either column has zero variance.
6. **Minimum group sizes** — `KURTOSIS` requires at least 4 non-NULL rows; `SKEWNESS` requires at least 3; results are `NULL` below these thresholds.

---

## :material-flask-outline: Practical Examples

### Setup

```sql
CREATE TABLE measurements (
    sensor_id   STRING,
    region      STRING,
    reading     DOUBLE,
    baseline    DOUBLE,
    measured_at DATE
);

INSERT INTO measurements VALUES
    ('S1', 'East',  23.5, 20.0, DATE '2024-01-01'),
    ('S2', 'East',  25.1, 20.0, DATE '2024-01-01'),
    ('S3', 'East',  22.8, 20.0, DATE '2024-01-02'),
    ('S4', 'West',  31.4, 28.0, DATE '2024-01-01'),
    ('S5', 'West',  29.7, 28.0, DATE '2024-01-02'),
    ('S6', 'West',  33.2, 28.0, DATE '2024-01-03'),
    ('S7', 'North', 18.0, 18.0, DATE '2024-01-01'),
    ('S8', 'North', 19.5, 18.0, DATE '2024-01-02'),
    ('S9', 'North', 17.2, 18.0, DATE '2024-01-03');
```

### 1 — Standard deviation and variance per group

```sql
SELECT
    region,
    ROUND(AVG(reading), 2)        AS avg_reading,
    ROUND(STDDEV(reading), 4)     AS stddev_sample,
    ROUND(STDDEV_POP(reading), 4) AS stddev_pop,
    ROUND(VARIANCE(reading), 4)   AS variance_sample,
    ROUND(VAR_POP(reading), 4)    AS variance_pop
FROM measurements
GROUP BY region
ORDER BY region;
-- Result:
-- region | avg_reading | stddev_sample | stddev_pop | variance_sample | variance_pop
-- --------|-------------|---------------|------------|-----------------|-------------
-- East    | 23.8        | 1.1790        | 0.9626     | 1.39            | 0.9267
-- North   | 18.23       | 1.1504        | 0.9398     | 1.3234          | 0.8832
-- West    | 31.43       | 1.7502        | 1.4289     | 3.0633          | 2.0422
```

### 2 — Correlation and covariance analysis

```sql
SELECT
    region,
    ROUND(CORR(reading, baseline), 4)       AS pearson_corr,
    ROUND(COVAR_SAMP(reading, baseline), 4) AS covar_sample,
    ROUND(COVAR_POP(reading, baseline), 4)  AS covar_pop
FROM measurements
GROUP BY region
ORDER BY region;
-- CORR close to 1.0  → reading and baseline move together strongly.
-- CORR close to 0.0  → no linear relationship between the two columns.
```

### 3 — Approximate percentiles (scalar and array)

```sql
SELECT
    region,
    PERCENTILE_APPROX(reading, 0.5)               AS median_reading,
    PERCENTILE_APPROX(reading, ARRAY(0.25, 0.75)) AS iqr_bounds,
    PERCENTILE_APPROX(reading, ARRAY(0.05, 0.95), 50000) AS p5_p95_high_accuracy
FROM measurements
GROUP BY region;
-- Result:
-- region | median_reading | iqr_bounds         | p5_p95_high_accuracy
-- --------|----------------|--------------------|---------------------
-- East    | 23.5           | [22.8, 25.1]       | [22.8, 25.1]
-- West    | 31.4           | [29.7, 33.2]       | [29.7, 33.2]
-- North   | 18.0           | [17.2, 19.5]       | [17.2, 19.5]
```

### 4 — FILTER on statistical functions

```sql
SELECT
    ROUND(STDDEV(reading), 4)                                 AS overall_stddev,
    ROUND(STDDEV(reading) FILTER (WHERE region = 'East'), 4)  AS east_stddev,
    ROUND(STDDEV(reading) FILTER (WHERE region != 'East'), 4) AS non_east_stddev
FROM measurements;
-- Compute statistics for subsets of rows without a separate GROUP BY or subquery.
```

### 5 — Combined distribution summary

```sql
SELECT
    region,
    COUNT(*)                         AS n,
    ROUND(AVG(reading), 2)           AS mean,
    ROUND(MEDIAN(reading), 2)        AS median,
    ROUND(STDDEV(reading), 4)        AS stddev,
    ROUND(SKEWNESS(reading), 4)      AS skewness,
    ROUND(KURTOSIS(reading), 4)      AS kurtosis,
    PERCENTILE_APPROX(reading, 0.25) AS q1,
    PERCENTILE_APPROX(reading, 0.75) AS q3
FROM measurements
GROUP BY region
ORDER BY region;
```

---

## :material-brain: When to Use

| Scenario | Recommended Function |
|----------|---------------------|
| Measure data spread or volatility | `STDDEV`, `VARIANCE` |
| Full-population statistics (census, complete data) | `STDDEV_POP`, `VAR_POP` |
| Detect linear relationship between two metrics | `CORR` |
| Measure joint variation of two metrics | `COVAR_SAMP`, `COVAR_POP` |
| Fast percentile on large datasets | `PERCENTILE_APPROX` |
| Exact median on small or medium datasets | `MEDIAN` |
| Detect distribution tail behaviour | `KURTOSIS` |
| Detect distribution asymmetry | `SKEWNESS` |
| Scoped statistics for a subset of rows | `agg() FILTER (WHERE ...)` |
