# :material-numeric-9-circle: Median and Approximate Percentiles

Compute median and arbitrary percentiles within each partition.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', 100),
  ('North', 'Alice', 200),
  ('North', 'Bob',   150),
  ('North', 'Bob',   300),
  ('North', 'Alice', 250),
  ('South', 'Carol', 400),
  ('South', 'Carol', 500),
  ('South', 'Dave',  450),
  ('South', 'Dave',  600),
  ('South', 'Carol', 550)
AS sales(region, rep, amount);
```

### Approximate Percentiles (Recommended for Large Datasets)

`PERCENTILE_APPROX` uses a t-digest algorithm — fast and memory-efficient
with configurable accuracy.

```sql
SELECT
    region,
    PERCENTILE_APPROX(amount, 0.5)  AS median_amount,
    PERCENTILE_APPROX(amount, 0.25) AS p25,
    PERCENTILE_APPROX(amount, 0.75) AS p75,
    PERCENTILE_APPROX(amount, array(0.05, 0.95)) AS p5_p95
FROM sales
GROUP BY region
ORDER BY region;
```

??? success "Expected Output"

    | region | median_amount | p25 | p75 | p5_p95      |
    |--------|:-------------:|----:|----:|-------------|
    | North  |           200 | 150 | 250 | [100, 300]  |
    | South  |           500 | 450 | 550 | [400, 600]  |

    **How to read:**

    - `median_amount` (p50) — the middle value when amounts are sorted.
    - `p25` — 25% of values fall at or below this point.
    - `p75` — 75% of values fall at or below this point.
    - `p5_p95` — array containing the 5th and 95th percentile boundaries.

!!! note "Accuracy parameter"
    `PERCENTILE_APPROX(col, pct, accuracy)` accepts an optional third argument
    (default 10000). Higher values = more precise but more memory. For most
    use cases the default is sufficient.

### Exact Percentile (Small Datasets Only)

`PERCENTILE` performs a full sort — exact but expensive on large partitions:

```sql
SELECT
    region,
    PERCENTILE(amount, 0.5)  AS exact_median,
    PERCENTILE(amount, 0.25) AS exact_p25,
    PERCENTILE(amount, 0.75) AS exact_p75
FROM sales
GROUP BY region
ORDER BY region;
```

??? success "Expected Output"

    | region | exact_median | exact_p25 | exact_p75 |
    |--------|-------------:|----------:|----------:|
    | North  |        200.0 |     150.0 |     250.0 |
    | South  |        500.0 |     450.0 |     550.0 |

    With 5 sorted values in North (100, 150, 200, 250, 300):

    - p50 = middle value = **200**
    - p25 = value at 25% position = **150** (interpolated)
    - p75 = value at 75% position = **250** (interpolated)

!!! warning "Performance"
    `PERCENTILE` triggers a full sort per group. Use `PERCENTILE_APPROX` for tables
    with more than a few thousand rows per partition.

---

## :material-information-outline: Window vs GROUP BY

Percentile functions in Spark SQL are **aggregate** functions, not window functions.
To attach percentile values to every row without collapsing, use one of these patterns:

### Pattern 1 — JOIN Back to Original Rows

```sql
WITH stats AS (
    SELECT
        region,
        PERCENTILE_APPROX(amount, 0.5)  AS median_amount,
        PERCENTILE_APPROX(amount, 0.25) AS p25,
        PERCENTILE_APPROX(amount, 0.75) AS p75
    FROM sales
    GROUP BY region
)
SELECT
    s.region,
    s.rep,
    s.amount,
    st.median_amount,
    st.p25,
    st.p75,
    CASE
        WHEN s.amount < st.p25 THEN 'Below Q1'
        WHEN s.amount > st.p75 THEN 'Above Q3'
        ELSE 'Interquartile'
    END AS iqr_position
FROM sales s
JOIN stats st ON s.region = st.region
ORDER BY s.region, s.amount;
```

??? success "Expected Output"

    | region | rep   | amount | median_amount | p25 | p75 | iqr_position  |
    |--------|-------|-------:|--------------:|----:|----:|---------------|
    | North  | Alice |    100 |           200 | 150 | 250 | Below Q1      |
    | North  | Bob   |    150 |           200 | 150 | 250 | Interquartile |
    | North  | Alice |    200 |           200 | 150 | 250 | Interquartile |
    | North  | Alice |    250 |           200 | 150 | 250 | Interquartile |
    | North  | Bob   |    300 |           200 | 150 | 250 | Above Q3      |
    | South  | Carol |    400 |           500 | 450 | 550 | Below Q1      |
    | South  | Dave  |    450 |           500 | 450 | 550 | Interquartile |
    | South  | Carol |    500 |           500 | 450 | 550 | Interquartile |
    | South  | Carol |    550 |           500 | 450 | 550 | Interquartile |
    | South  | Dave  |    600 |           500 | 450 | 550 | Above Q3      |

### Pattern 2 — Row-Level Ranking with PERCENT_RANK

For per-row percentile scores (not group-level statistics), use window functions:

```sql
SELECT
    region,
    rep,
    amount,
    ROUND(PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount), 2) AS pct_rank
FROM sales
ORDER BY region, amount;
```

??? success "Expected Output"

    | region | rep   | amount | pct_rank |
    |--------|-------|-------:|---------:|
    | North  | Alice |    100 |     0.00 |
    | North  | Bob   |    150 |     0.25 |
    | North  | Alice |    200 |     0.50 |
    | North  | Alice |    250 |     0.75 |
    | North  | Bob   |    300 |     1.00 |
    | South  | Carol |    400 |     0.00 |
    | South  | Dave  |    450 |     0.25 |
    | South  | Carol |    500 |     0.50 |
    | South  | Carol |    550 |     0.75 |
    | South  | Dave  |    600 |     1.00 |

!!! tip "Choosing the right tool"
    | Need | Function | Type |
    |------|----------|------|
    | Group-level median/percentile | `PERCENTILE_APPROX` | Aggregate (`GROUP BY`) |
    | Row-level percentile score | `PERCENT_RANK()` | Window function |
    | Row-level bucket assignment | `NTILE(n)` | Window function |

---

## :material-compare: PERCENTILE vs PERCENTILE_APPROX

| Aspect | `PERCENTILE` | `PERCENTILE_APPROX` |
|--------|:------------:|:-------------------:|
| Algorithm | Full sort | t-digest approximation |
| Accuracy | Exact | Configurable (default ~0.01% error) |
| Performance | O(N log N) per group | O(N) streaming |
| Memory | Holds all values | Fixed-size sketch |
| Column types | Numeric only | Numeric only |
| Best for | < 10K rows per group | Any size |

---

## :material-lightbulb-outline: When to Use

- Summary statistics dashboards — median salary, revenue, latency per group.
- Outlier detection — flag values outside the p5–p95 range.
- Data profiling — understand distribution shape across partitions.

---

## :material-arrow-right: Related

- [Percentile Scoring](percentile.md) — row-level `PERCENT_RANK` and `NTILE`
- [Window Types — Aggregate](../functions/aggregate.md) — `SUM`, `AVG`, `COUNT` as windows
