# :material-sigma: Statistical Aggregate Functions

Statistical functions compute **descriptive statistics** across grouped or ungrouped rows —
averages, medians, modes, percentiles, and extremes with associated values.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[Statistical Function]
    C --> D[One Row per Group]
```

## :material-pin: Available Functions

| Function | Description | NULL Handling |
|----------|-------------|--------------|
| `AVG(expr)` | Arithmetic mean | Skips NULLs |
| `MEAN(expr)` | Alias for `AVG` | Skips NULLs |
| `MEDIAN(expr)` | Middle value (50th percentile) | Skips NULLs |
| `MODE(expr)` | Most frequent value | Skips NULLs |
| `PERCENTILE(expr, p)` | Value at the p-th percentile | Skips NULLs |
| `MIN(expr)` | Minimum value | Skips NULLs |
| `MAX(expr)` | Maximum value | Skips NULLs |
| `MIN_BY(expr, ordering)` | Value of `expr` at row where `ordering` is minimum | Skips NULLs |
| `MAX_BY(expr, ordering)` | Value of `expr` at row where `ordering` is maximum | Skips NULLs |
| `STDDEV(expr)` | Sample standard deviation | Skips NULLs |
| `STDDEV_POP(expr)` | Population standard deviation | Skips NULLs |
| `VARIANCE(expr)` | Sample variance | Skips NULLs |
| `VAR_POP(expr)` | Population variance | Skips NULLs |
| `SKEWNESS(expr)` | Skewness of the distribution | Skips NULLs |
| `KURTOSIS(expr)` | Kurtosis of the distribution | Skips NULLs |
| `CORR(expr1, expr2)` | Pearson correlation coefficient | Skips NULLs |
| `COVAR_SAMP(expr1, expr2)` | Sample covariance | Skips NULLs |
| `COVAR_POP(expr1, expr2)` | Population covariance | Skips NULLs |

## :material-magnify: Behavior

1. All statistical functions **skip NULLs** — NULLs are excluded from computation.
2. If all values are NULL, the result is `NULL`.
3. `AVG` and `MEAN` are interchangeable — both compute the arithmetic mean.
4. `MEDIAN` is equivalent to `PERCENTILE(expr, 0.5)`.
5. `MODE` returns the most frequent value; ties are broken arbitrarily.
6. `PERCENTILE` uses linear interpolation for fractional percentiles.
7. `MIN_BY` / `MAX_BY` return the value of the first expression at the row where the second expression is minimized / maximized.

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. AVG / MEAN — Arithmetic Mean

```sql
SELECT AVG(col) FROM VALUES (1), (2), (3) AS tab(col);
-- Result: 2.0

-- NULLs are skipped (not counted as zero)
SELECT AVG(col) FROM VALUES (1), (2), (NULL) AS tab(col);
-- Result: 1.5  (sum=3, count=2)
```

### :material-toy-brick: 2. MEDIAN — Middle Value

```sql
SELECT MEDIAN(col) FROM VALUES (0), (10) AS tab(col);
-- Result: 5.0

SELECT MEDIAN(col) FROM VALUES (1), (3), (5), (7) AS tab(col);
-- Result: 4.0  (average of 3 and 5)

-- Works with intervals
SELECT MEDIAN(col) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS tab(col);
-- Result: INTERVAL '5' MONTH
```

### :material-toy-brick: 3. MODE — Most Frequent Value

```sql
SELECT MODE(col) FROM VALUES (0), (10), (10) AS tab(col);
-- Result: 10

-- NULLs are skipped
SELECT MODE(col) FROM VALUES (0), (10), (10), (NULL), (NULL), (NULL) AS tab(col);
-- Result: 10  (NULLs excluded, 10 has highest frequency)

-- Works with intervals
SELECT MODE(col)
FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH), (INTERVAL '10' MONTH) AS tab(col);
-- Result: INTERVAL '10' MONTH
```

### :material-toy-brick: 4. PERCENTILE — Value at Percentile

```sql
-- Single percentile
SELECT PERCENTILE(col, 0.3) FROM VALUES (0), (10) AS tab(col);
-- Result: 3.0

-- Multiple percentiles at once
SELECT PERCENTILE(col, ARRAY(0.25, 0.5, 0.75)) FROM VALUES (0), (10) AS tab(col);
-- Result: [2.5, 5.0, 7.5]

-- Works with intervals
SELECT PERCENTILE(col, 0.5)
FROM VALUES (INTERVAL '0' SECOND), (INTERVAL '10' SECOND) AS tab(col);
-- Result: INTERVAL '5' SECOND
```

### :material-toy-brick: 5. MIN / MAX — Extremes

```sql
SELECT MIN(col), MAX(col) FROM VALUES (10), (-1), (20) AS tab(col);
-- Result: -1, 20
```

### :material-toy-brick: 6. MIN_BY / MAX_BY — Value at Extreme

```sql
-- Which name has the lowest score?
SELECT MIN_BY(name, score)
FROM VALUES ('Alice', 90), ('Bob', 50), ('Charlie', 80) AS tab(name, score);
-- Result: Bob

-- Which name has the highest score?
SELECT MAX_BY(name, score)
FROM VALUES ('Alice', 90), ('Bob', 50), ('Charlie', 80) AS tab(name, score);
-- Result: Alice
```

### :material-toy-brick: 7. Standard Deviation & Variance

```sql
SELECT
  ROUND(STDDEV(col), 2)     AS sample_stddev,
  ROUND(STDDEV_POP(col), 2) AS pop_stddev,
  ROUND(VARIANCE(col), 2)   AS sample_var,
  ROUND(VAR_POP(col), 2)    AS pop_var
FROM VALUES (2), (4), (4), (4), (5), (5), (7), (9) AS tab(col);
-- sample_stddev=2.14, pop_stddev=2.0, sample_var=4.57, pop_var=4.0
```

### :material-toy-brick: 8. Correlation & Covariance

```sql
SELECT
  ROUND(CORR(x, y), 4)       AS correlation,
  ROUND(COVAR_SAMP(x, y), 2) AS sample_cov,
  ROUND(COVAR_POP(x, y), 2)  AS pop_cov
FROM VALUES (1, 10), (2, 20), (3, 30), (4, 40) AS tab(x, y);
-- correlation=1.0, sample_cov=16.67, pop_cov=12.5
```

### :material-toy-brick: 9. Skewness & Kurtosis

```sql
SELECT
  ROUND(SKEWNESS(col), 4) AS skew,
  ROUND(KURTOSIS(col), 4) AS kurt
FROM VALUES (1), (2), (2), (3), (3), (3), (4) AS tab(col);
-- skew and kurtosis of the distribution
```

### :material-toy-brick: 10. Grouped Statistics

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('East', 100), ('East', 200), ('East', 150),
  ('West', 300), ('West', 250), ('West', 400)
AS sales(region, revenue);

SELECT region,
       ROUND(AVG(revenue), 0)    AS avg_rev,
       MEDIAN(revenue)           AS median_rev,
       MIN(revenue)              AS min_rev,
       MAX(revenue)              AS max_rev,
       ROUND(STDDEV(revenue), 1) AS stddev_rev
FROM sales
GROUP BY region;
-- East: avg=150, median=150, min=100, max=200, stddev=50.0
-- West: avg=317, median=300, min=250, max=400, stddev=76.4
```

## :material-brain: When to Use

| Scenario | Function(s) |
|----------|------------|
| Central tendency | `AVG`, `MEDIAN`, `MODE` |
| Spread / dispersion | `STDDEV`, `VARIANCE`, `PERCENTILE` |
| Extremes | `MIN`, `MAX` |
| Value at extreme of another column | `MIN_BY`, `MAX_BY` |
| Distribution shape | `SKEWNESS`, `KURTOSIS` |
| Relationship between two columns | `CORR`, `COVAR_SAMP`, `COVAR_POP` |
| Multiple percentiles at once | `PERCENTILE(col, ARRAY(…))` |

> **Tip:** Use `PERCENTILE(col, ARRAY(0.25, 0.5, 0.75))` to compute the full IQR
> (interquartile range) in a single pass — more efficient than three separate calls.
