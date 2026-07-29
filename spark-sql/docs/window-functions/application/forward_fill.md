# :material-numeric-7-circle: Forward-Fill (Last Observation Carried Forward)

Fill `NULL` readings with the most recent non-null value in each sensor's timeline.

---

## :material-flask-outline: Practical Examples

```sql
CREATE OR REPLACE TEMP VIEW sensor_readings AS
SELECT * FROM VALUES
  ('S1', '2024-01-01', 10.0),
  ('S1', '2024-01-02', NULL),
  ('S1', '2024-01-03', NULL),
  ('S1', '2024-01-04', 12.5),
  ('S1', '2024-01-05', NULL),
  ('S2', '2024-01-01', NULL),   -- no prior reading exists
  ('S2', '2024-01-02', 5.0),
  ('S2', '2024-01-03', NULL)
AS sensor_readings(sensor_id, reading_date, value);
```

### Spark 3.3+ — LAST_VALUE with IGNORE NULLS

```sql
SELECT
    sensor_id,
    reading_date,
    value,
    LAST_VALUE(value) IGNORE NULLS OVER (
        PARTITION BY sensor_id
        ORDER BY reading_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS value_ffill
FROM sensor_readings
ORDER BY sensor_id, reading_date;
```

??? success "Expected Output"

    | sensor_id | reading_date | value | value_ffill |
    |-----------|--------------|------:|------------:|
    | S1        | 2024-01-01   |  10.0 |        10.0 |
    | S1        | 2024-01-02   |  NULL |        10.0 |
    | S1        | 2024-01-03   |  NULL |        10.0 |
    | S1        | 2024-01-04   |  12.5 |        12.5 |
    | S1        | 2024-01-05   |  NULL |        12.5 |
    | S2        | 2024-01-01   |  NULL |        NULL |
    | S2        | 2024-01-02   |   5.0 |         5.0 |
    | S2        | 2024-01-03   |  NULL |         5.0 |

    - `LAST_VALUE ... IGNORE NULLS` scans from the start of the partition up to the
      current row and returns the most recent non-null value.
    - S2's first row stays NULL because no prior non-null value exists.

---

## :material-arrow-down-bold: Pre-Spark 3.3 Alternatives

### MAX with a Conditional Window (Value-Group Technique)

This technique works on **any Spark version**. The idea: assign each row a
group ID that increments whenever a non-null value appears, then take the
`MAX` of the original value within each group.

```sql
WITH groups AS (
    SELECT
        sensor_id,
        reading_date,
        value,
        -- Count non-null values from start to current row → group ID
        COUNT(value) OVER (
            PARTITION BY sensor_id
            ORDER BY reading_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS value_group
    FROM sensor_readings
)
SELECT
    sensor_id,
    reading_date,
    value,
    -- MAX within each group picks the single non-null value
    MAX(value) OVER (
        PARTITION BY sensor_id, value_group
    ) AS value_ffill
FROM groups
ORDER BY sensor_id, reading_date;
```

??? success "Expected Output"

    | sensor_id | reading_date | value | value_ffill |
    |-----------|--------------|------:|------------:|
    | S1        | 2024-01-01   |  10.0 |        10.0 |
    | S1        | 2024-01-02   |  NULL |        10.0 |
    | S1        | 2024-01-03   |  NULL |        10.0 |
    | S1        | 2024-01-04   |  12.5 |        12.5 |
    | S1        | 2024-01-05   |  NULL |        12.5 |
    | S2        | 2024-01-01   |  NULL |        NULL |
    | S2        | 2024-01-02   |   5.0 |         5.0 |
    | S2        | 2024-01-03   |  NULL |         5.0 |

**Step-by-step breakdown** for S1:

| reading_date | value | COUNT(value) = value_group | MAX(value) in group |
|--------------|------:|---------------------------:|--------------------:|
| 2024-01-01   |  10.0 |                          1 |                10.0 |
| 2024-01-02   |  NULL |                          1 |                10.0 |
| 2024-01-03   |  NULL |                          1 |                10.0 |
| 2024-01-04   |  12.5 |                          2 |                12.5 |
| 2024-01-05   |  NULL |                          2 |                12.5 |

!!! tip "Why `COUNT(value)` works"
    `COUNT(value)` skips NULLs, so it only increments when a real value appears.
    All NULLs following a non-null row share the same group ID as that row.
    `MAX` within the group then returns the single non-null value.

### Self-Join Approach

Join each row to the most recent non-null row that precedes it:

```sql
WITH indexed AS (
    SELECT
        sensor_id,
        reading_date,
        value,
        ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY reading_date) AS rn
    FROM sensor_readings
)
SELECT
    a.sensor_id,
    a.reading_date,
    a.value,
    b.value AS value_ffill
FROM indexed a
LEFT JOIN indexed b
  ON  a.sensor_id = b.sensor_id
  AND b.value IS NOT NULL
  AND b.rn <= a.rn
  AND b.rn = (
      SELECT MAX(c.rn)
      FROM indexed c
      WHERE c.sensor_id = a.sensor_id
        AND c.value IS NOT NULL
        AND c.rn <= a.rn
  )
ORDER BY a.sensor_id, a.reading_date;
```

??? success "Expected Output"

    | sensor_id | reading_date | value | value_ffill |
    |-----------|--------------|------:|------------:|
    | S1        | 2024-01-01   |  10.0 |        10.0 |
    | S1        | 2024-01-02   |  NULL |        10.0 |
    | S1        | 2024-01-03   |  NULL |        10.0 |
    | S1        | 2024-01-04   |  12.5 |        12.5 |
    | S1        | 2024-01-05   |  NULL |        12.5 |
    | S2        | 2024-01-01   |  NULL |        NULL |
    | S2        | 2024-01-02   |   5.0 |         5.0 |
    | S2        | 2024-01-03   |  NULL |         5.0 |

!!! warning "Self-join performance"
    The self-join with a correlated subquery is expensive on large datasets — it
    scales as O(N²) per partition. **Prefer the `MAX` with value-group technique**
    when `IGNORE NULLS` is not available.

---

## :material-compare: Approach Comparison

| Approach | Spark Version | Performance | Complexity |
|----------|:-------------:|:-----------:|:----------:|
| `LAST_VALUE ... IGNORE NULLS` | 3.3+ | :material-check: Fast | Simple |
| `MAX` + value-group CTE | Any | :material-check: Fast | Moderate |
| Self-join | Any | :material-close: Slow | Complex |

---

## :material-lightbulb-outline: When to Use

- IoT / sensor data — fill gaps in intermittent readings.
- Financial data — carry forward the last known price when markets are closed.
- Survey / form data — propagate a header value across detail rows.

---

## :material-arrow-right: Related

- [NULL Handling in Windows](../nulls/null_options_wf.md) — `IGNORE NULLS`, `RESPECT NULLS`
- [Gap Detection](gap_detection.md) — detect missing data instead of filling it
