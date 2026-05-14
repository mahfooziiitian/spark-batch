# :material-information: Introduction to SCD

**Slowly Changing Dimensions** (SCD) is a foundational data warehousing pattern for managing changes
in dimension tables over time. Depending on business requirements, you can discard history (Type 1),
preserve every version (Type 2), or store limited history in extra columns (Type 3).

---

## :material-compare: SCD Types at a Glance

| Type | Strategy | History | Use when… |
|------|----------|---------|-----------|
| **Type 1** | Overwrite in place | None | Only current state matters |
| **Type 2** | Insert new row per change | Full | Point-in-time queries required |
| **Type 3** | Add `previous_` columns | One level | One prior value is enough |
| **Type 4** | Separate history table | Full | Lean current dim + history for analysts |
| **Type 5** | Type 4 + embedded current | Full | BI tools need current value without join |
| **Type 6** | Type 1+2+3 combined | Full | Current value on every history row |

---

## :material-toy-brick: Source Table

```sql
CREATE TABLE IF NOT EXISTS source_data (
    id      INT,
    name    STRING,
    address STRING,
    load_date DATE
)
USING DELTA;
```

---

## :material-database: Dimension Table (Type 2 template)

```sql
CREATE TABLE IF NOT EXISTS dimension_table (
    id                   INT,
    name                 STRING,
    address              STRING,
    effective_start_date DATE,
    effective_end_date   DATE,    -- NULL = currently active; or use DATE '9999-12-31'
    is_current           BOOLEAN
)
USING DELTA;
```

---

## :material-database-import: Seed Initial Data

```sql
-- Populate source
INSERT INTO source_data VALUES
    (1, 'Alice', '123 Main St', DATE '2023-01-01'),
    (2, 'Bob',   '456 Elm St',  DATE '2023-01-01');

-- Initial dimension load
INSERT INTO dimension_table VALUES
    (1, 'Alice', '123 Main St', DATE '2023-01-01', DATE '9999-12-31', TRUE),
    (2, 'Bob',   '456 Elm St',  DATE '2023-01-01', DATE '9999-12-31', TRUE);
```

---

## :material-tray-arrow-down: Incoming Change Batch

```sql
-- Simulate a new load: Alice moved, Charlie is new
INSERT INTO source_data VALUES
    (1, 'Alice',   '789 Oak St',   DATE '2023-02-01'),  -- address changed
    (3, 'Charlie', '111 Pine St',  DATE '2023-02-01');  -- new record
```

---

## :material-repeat: MERGE Foundation (Type 2 pattern)

!!! warning "Two-step MERGE required"
    A single MERGE cannot expire an existing row **and** insert a new version for the same key
    in one pass. Always use two separate statements.

**Step 1 — Expire changed rows:**

```sql
MERGE INTO dimension_table AS d
USING (
    SELECT id, name, address, load_date FROM source_data
) AS s
ON d.id = s.id AND d.is_current = TRUE
WHEN MATCHED AND (d.name <> s.name OR d.address <> s.address) THEN
    UPDATE SET
        d.is_current          = FALSE,
        d.effective_end_date  = s.load_date;
```

**Step 2 — Insert new / changed rows:**

```sql
MERGE INTO dimension_table AS d
USING (
    SELECT s.id, s.name, s.address, s.load_date
    FROM source_data AS s
    LEFT JOIN dimension_table AS d
        ON d.id = s.id AND d.is_current = TRUE
    WHERE d.id IS NULL
       OR d.name    <> s.name
       OR d.address <> s.address
) AS src
ON d.id = src.id AND d.is_current = TRUE AND d.name = src.name AND d.address = src.address
WHEN NOT MATCHED THEN
    INSERT (id, name, address, effective_start_date, effective_end_date, is_current)
    VALUES (src.id, src.name, src.address, src.load_date, DATE '9999-12-31', TRUE);
```

---

## :material-check-circle-outline: Expected Result

After both steps:

| id | name    | address       | effective_start | effective_end | is_current |
|----|---------|---------------|-----------------|---------------|------------|
| 1  | Alice   | 123 Main St   | 2023-01-01      | 2023-02-01    | false      |
| 1  | Alice   | 789 Oak St    | 2023-02-01      | 9999-12-31    | true       |
| 2  | Bob     | 456 Elm St    | 2023-01-01      | 9999-12-31    | true       |
| 3  | Charlie | 111 Pine St   | 2023-02-01      | 9999-12-31    | true       |

!!! tip "Row hash pattern"
    For production use, add `md5(concat_ws('||', name, address))` as a `row_hash` column.
    This reduces the MERGE condition to a single string comparison instead of one `!=` per tracked column.

---

## :material-arrow-right: Next Steps

Explore each type in detail — each section covers table design, seed data, MERGE patterns,
analytical queries, and common pitfalls.

