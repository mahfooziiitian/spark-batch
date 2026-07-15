# :material-table-pivot: UNPIVOT

`UNPIVOT` transforms columns into rows — the inverse of `PIVOT`. It is used to normalise wide (pivoted) data into a long format suitable for aggregation, filtering, and analytics.

---

## :material-sitemap: Overview

```mermaid
graph LR
    A["Wide table<br/>year | q1 | q2 | q3 | q4"] --> B["UNPIVOT on q1..q4"]
    B --> C["Long table<br/>year | quarter | revenue"]
```

---

## :material-pin: Syntax

```sql
SELECT unpivot_col, value_col [, ...]
FROM source_table
UNPIVOT [INCLUDE NULLS] (
    value_col
    FOR unpivot_col IN (col1 [AS alias1], col2 [AS alias2], ...)
);
```

| Element | Description |
|---------|-------------|
| `value_col` | Output column that receives the cell values from the listed input columns |
| `FOR unpivot_col IN (...)` | Name of the output label column; `IN` lists which input columns to unpivot and optional aliases for their label values |
| `INCLUDE NULLS` | By default rows where the value is `NULL` are excluded; add this to keep them |

---

## :material-magnify: Behavior

1. **Row expansion** — each row in the source produces one output row per column listed in `IN (...)`; a source with 3 rows and 4 unpivoted columns produces up to 12 rows.
2. **NULL filtering** — `UNPIVOT` silently drops output rows where the value column is `NULL`; use `UNPIVOT INCLUDE NULLS` to retain them.
3. **Type alignment** — all columns listed in `IN (...)` must have compatible types; Spark casts to the widest compatible type automatically.
4. **Multi-value unpivot** — you can unpivot two parallel columns simultaneously: `(qty, rev) FOR quarter IN ((q1_qty, q1_rev) AS 'Q1', ...)`.
5. **Equivalent `STACK` approach** — `LATERAL VIEW STACK(n, 'label1', col1, 'label2', col2, ...) AS label, value` produces the same output but is harder to read; prefer `UNPIVOT`.

---

## :material-flask-outline: Practical Examples

### Setup — quarterly revenue pivot table

```sql
CREATE TABLE quarterly_revenue (
    region  STRING,
    yr      INT,
    q1      DOUBLE,
    q2      DOUBLE,
    q3      DOUBLE,
    q4      DOUBLE
);

INSERT INTO quarterly_revenue VALUES
    ('East',  2024, 100.0, 200.0, 150.0, 300.0),
    ('West',  2024, 250.0, 180.0, 320.0, 270.0),
    ('North', 2024,  90.0,  NULL, 110.0, 140.0);  -- q2 is NULL for North
```

### 1 — Basic UNPIVOT (NULL rows excluded by default)

```sql
SELECT region, yr, quarter, revenue
FROM quarterly_revenue
UNPIVOT (
    revenue
    FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2', q3 AS 'Q3', q4 AS 'Q4')
)
ORDER BY region, yr, quarter;
-- Result (North Q2 row is dropped because revenue = NULL):
-- region | yr   | quarter | revenue
-- --------|------|---------|--------
-- East    | 2024 | Q1      | 100.0
-- East    | 2024 | Q2      | 200.0
-- East    | 2024 | Q3      | 150.0
-- East    | 2024 | Q4      | 300.0
-- North   | 2024 | Q1      |  90.0
-- North   | 2024 | Q3      | 110.0
-- North   | 2024 | Q4      | 140.0
-- West    | 2024 | Q1      | 250.0
-- West    | 2024 | Q2      | 180.0
-- West    | 2024 | Q3      | 320.0
-- West    | 2024 | Q4      | 270.0
```

### 2 — INCLUDE NULLS to retain missing values

```sql
SELECT region, yr, quarter, revenue
FROM quarterly_revenue
UNPIVOT INCLUDE NULLS (
    revenue
    FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2', q3 AS 'Q3', q4 AS 'Q4')
)
ORDER BY region, yr, quarter;
-- Result (North Q2 row is now present with revenue = NULL):
-- region | yr   | quarter | revenue
-- --------|------|---------|--------
-- ...
-- North   | 2024 | Q1      |  90.0
-- North   | 2024 | Q2      |  NULL   ← included
-- North   | 2024 | Q3      | 110.0
-- North   | 2024 | Q4      | 140.0
-- ...
```

### 3 — Aggregate after UNPIVOT

```sql
SELECT quarter, ROUND(AVG(revenue), 2) AS avg_revenue
FROM quarterly_revenue
UNPIVOT (
    revenue
    FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2', q3 AS 'Q3', q4 AS 'Q4')
)
GROUP BY quarter
ORDER BY quarter;
-- Result:
-- quarter | avg_revenue
-- --------|------------
-- Q1      | 146.67
-- Q2      | 190.00   -- North Q2 excluded (NULL)
-- Q3      | 193.33
-- Q4      | 236.67
```

### 4 — UNPIVOT then re-PIVOT to verify round-trip

```sql
WITH long_form AS (
    SELECT region, yr, quarter, revenue
    FROM quarterly_revenue
    UNPIVOT (
        revenue FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2', q3 AS 'Q3', q4 AS 'Q4')
    )
)
SELECT *
FROM long_form
PIVOT (
    SUM(revenue) AS revenue
    FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
)
ORDER BY region;
-- Reconstructs the original wide table (NULLs where data was missing).
```

### 5 — Sensor data: multiple measurement columns → long format

```sql
CREATE OR REPLACE TEMP VIEW sensor_readings AS
SELECT * FROM VALUES
    ('S1', '2024-01-01', 23.5, 55.0,  1013.0),
    ('S2', '2024-01-01', 25.1, 60.2,  1010.5),
    ('S3', '2024-01-02', 22.8, 58.0,  1011.0)
AS t(sensor_id, reading_date, temperature, humidity, pressure);

SELECT sensor_id, reading_date, metric, value
FROM sensor_readings
UNPIVOT (
    value
    FOR metric IN (
        temperature AS 'temperature',
        humidity    AS 'humidity',
        pressure    AS 'pressure'
    )
)
ORDER BY sensor_id, metric;
-- Result:
-- sensor_id | reading_date | metric      | value
-- ----------|--------------|-------------|-------
-- S1        | 2024-01-01   | humidity    | 55.0
-- S1        | 2024-01-01   | pressure    | 1013.0
-- S1        | 2024-01-01   | temperature | 23.5
-- S2        | 2024-01-01   | humidity    | 60.2
-- ...
```

---

## :material-brain: When to Use

| Scenario | Recommended Pattern |
|----------|---------------------|
| Normalise wide pivot output into long format | `UNPIVOT` |
| Multiple metrics stored as columns → single column | `UNPIVOT` |
| Aggregate across formerly-separate metric columns | `UNPIVOT` then `GROUP BY` |
| Retain rows where a metric is NULL | `UNPIVOT INCLUDE NULLS` |
| Wide-format check — verify count per metric | `UNPIVOT` then `COUNT(*) GROUP BY metric` |
