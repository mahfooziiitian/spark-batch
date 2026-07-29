# :material-numeric-6-circle: Percentile Scoring

Assign each rep a percentile rank and quartile bucket across their region
using `PERCENT_RANK` and `NTILE(4)`.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW sales AS
SELECT * FROM VALUES
  ('North', 'Alice', 100),
  ('North', 'Bob',   150),
  ('North', 'Alice', 200),
  ('North', 'Alice', 300),
  ('North', 'Bob',   300),
  ('South', 'Carol', 400),
  ('South', 'Dave',  450),
  ('South', 'Carol', 500),
  ('South', 'Dave',  600)
AS sales(region, rep, amount);

SELECT
    region,
    rep,
    amount,
    ROUND(PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount), 2) AS pct_rank,
    NTILE(4)             OVER (PARTITION BY region ORDER BY amount)     AS quartile
FROM sales
ORDER BY region, amount;
```

??? success "Expected Output"

    | region | rep   | amount | pct_rank | quartile |
    |--------|-------|-------:|---------:|---------:|
    | North  | Alice |    100 |     0.00 |        1 |
    | North  | Bob   |    150 |     0.25 |        1 |
    | North  | Alice |    200 |     0.50 |        2 |
    | North  | Alice |    300 |     0.75 |        3 |
    | North  | Bob   |    300 |     1.00 |        4 |
    | South  | Carol |    400 |     0.00 |        1 |
    | South  | Dave  |    450 |     0.33 |        2 |
    | South  | Carol |    500 |     0.67 |        3 |
    | South  | Dave  |    600 |     1.00 |        4 |

---

## :material-information-outline: How It Works

### PERCENT_RANK

Computes the **relative standing** of each row within its partition:

```
PERCENT_RANK = (rank - 1) / (partition_size - 1)
```

| Step | Description |
|------|-------------|
| 1 | Assign a `RANK()` to each row based on `ORDER BY amount` |
| 2 | Count total rows in the partition (`N`) |
| 3 | Calculate `(rank - 1) / (N - 1)` |

For the **North** partition (5 rows):

| rep   | amount | rank | (rank - 1) / (5 - 1) | pct_rank |
|-------|-------:|-----:|----------------------:|---------:|
| Alice |    100 |    1 |              0 / 4    |     0.00 |
| Bob   |    150 |    2 |              1 / 4    |     0.25 |
| Alice |    200 |    3 |              2 / 4    |     0.50 |
| Alice |    300 |    4 |              3 / 4    |     0.75 |
| Bob   |    300 |    4 |              3 / 4    |     0.75 |

!!! note "Ties share the same PERCENT_RANK"
    Rows with amount = 300 both get rank 4 → `pct_rank = 0.75`.
    The next rank (5) is skipped, same as `RANK()` behaviour.

### NTILE(n)

Divides the partition into **n roughly equal buckets** and assigns a bucket number:

```
bucket_size = partition_size / n  (remainder rows go to earlier buckets)
```

For the **North** partition (5 rows, 4 buckets):

| Bucket | Rows assigned | Why |
|-------:|:-------------:|-----|
| 1 | 2 rows | 5 / 4 = 1 remainder 1 → first bucket gets an extra row |
| 2 | 1 row | |
| 3 | 1 row | |
| 4 | 1 row | |

For the **South** partition (4 rows, 4 buckets): each bucket gets exactly 1 row.

---

## :material-compare: PERCENT_RANK vs CUME_DIST

```sql
SELECT
    region,
    rep,
    amount,
    ROUND(PERCENT_RANK() OVER w, 2) AS pct_rank,
    ROUND(CUME_DIST()    OVER w, 2) AS cume_dist
FROM sales
WINDOW w AS (PARTITION BY region ORDER BY amount)
ORDER BY region, amount;
```

??? success "Expected Output"

    | region | rep   | amount | pct_rank | cume_dist |
    |--------|-------|-------:|---------:|----------:|
    | North  | Alice |    100 |     0.00 |      0.20 |
    | North  | Bob   |    150 |     0.25 |      0.40 |
    | North  | Alice |    200 |     0.50 |      0.60 |
    | North  | Alice |    300 |     0.75 |      1.00 |
    | North  | Bob   |    300 |     0.75 |      1.00 |
    | South  | Carol |    400 |     0.00 |      0.25 |
    | South  | Dave  |    450 |     0.33 |      0.50 |
    | South  | Carol |    500 |     0.67 |      0.75 |
    | South  | Dave  |    600 |     1.00 |      1.00 |

| Function | Formula | Range | Characteristic |
|----------|---------|-------|----------------|
| `PERCENT_RANK` | (rank - 1) / (N - 1) | 0 → 1 | First row is always 0 |
| `CUME_DIST` | rank / N | > 0 → 1 | Last row is always 1 |

!!! tip "Which to choose?"
    - Use `PERCENT_RANK` when you need a 0-based percentile score (e.g., "top 10%"
      filtering: `PERCENT_RANK() >= 0.9`).
    - Use `CUME_DIST` when you want the proportion of values less than or equal to
      the current value (empirical CDF).

---

## :material-lightbulb-outline: When to Use

- Assign customers to tier buckets (quartiles, deciles) for segmentation.
- Benchmark individual performance against the group distribution.
- Identify outliers — rows in the top or bottom percentile.

---

## :material-arrow-right: Related

- [Median and Percentiles](median.md) — `PERCENTILE_APPROX` for group-level stats
- [Window Types — Ranking](../functions/ranking.md) — `PERCENT_RANK`, `NTILE`, `CUME_DIST`
