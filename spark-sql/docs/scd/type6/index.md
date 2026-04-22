# :material-layers: SCD Type 6

SCD **Type 6** is the most comprehensive hybrid pattern, combining behaviours from all three
foundational types in a **single table**:

| Behaviour | Inherited from | How it is stored |
|-----------|---------------|-----------------|
| Overwrite current values | Type 1 | `name`, `email`, `city` are always current on every row |
| Full version history | Type 2 | One row per change — `start_date`, `end_date`, `is_current` |
| Previous-value column | Type 3 | `prev_city` carries the value from the immediately preceding row |

The superposition of all three gives analysts both **point-in-time accuracy** and
**fast access to current state** without joins to a separate history table.

---

## :material-layers-triple: How Type 6 Differs from Related Types

| Feature | Type 2 | Type 3 | Type 5 | **Type 6** |
|---------|--------|--------|--------|-----------|
| Full history rows | :material-check: | :material-close: | Separate table | :material-check: Same table |
| Current-value columns on all rows | :material-close: | :material-check: | :material-check: | :material-check: |
| Previous-value column | :material-close: | :material-check: One column | :material-close: | :material-check: One column |
| Extra table needed | :material-close: | :material-close: | :material-check: | :material-close: |
| Query complexity (current) | Medium | Low | Low | Low |
| Query complexity (history) | Low | High | Medium | Low |

---

## :material-toy-brick: Table Design

A single `dim_customer` table carries the full Type 1 / 2 / 3 payload.

```sql
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk  BIGINT GENERATED ALWAYS AS IDENTITY,  -- surrogate key
    customer_id  STRING      NOT NULL,                  -- natural / business key
    name         STRING,
    email        STRING,
    city         STRING,                                -- Type 1: always current value
    prev_city    STRING,                                -- Type 3: value before the last change
    row_hash     STRING,                                -- change detection fingerprint
    start_date   TIMESTAMP,                             -- Type 2: row effective from
    end_date     TIMESTAMP,                             -- Type 2: row expired at (NULL = active)
    is_current   BOOLEAN                                -- Type 2: convenience flag
)
USING DELTA;
```

!!! note "Why keep `city` current on all rows?"
    In a pure Type 2 table, historical rows store the *old* city value — you need a filter
    (`WHERE is_current = TRUE`) just to get today's state.  Type 6 **back-fills** the current
    `city` value onto every historical row so that `GROUP BY city` always reflects the customer's
    current location, regardless of which version row is selected.

---

## :material-database-import: Seed Data

```sql
INSERT INTO dim_customer
    (customer_id, name, email, city, prev_city, row_hash, start_date, end_date, is_current)
VALUES
    ('cust1', 'Alice', 'alice@example.com', 'NY', NULL,
        md5('Alice||alice@example.com||NY'), '2024-01-01', NULL, TRUE),
    ('cust2', 'Bob',   'bob@example.com',   'CA', NULL,
        md5('Bob||bob@example.com||CA'),   '2024-01-01', NULL, TRUE);
```

---

## :material-tray-arrow-down: Incoming Staging Data

```sql
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT *
FROM VALUES
    ('cust1', 'Alice',   'alice@example.com',  'NY'),   -- no change
    ('cust2', 'Bobby',   'bob@newdomain.com',  'TX'),   -- name + email + city changed
    ('cust3', 'Charlie', 'charlie@example.com','WA')    -- new customer
AS t(customer_id, name, email, city);
```

---

## :material-repeat: Step-by-Step SCD Type 6 Logic

### Step 1 — Compute row hash for incoming records

```sql
CREATE OR REPLACE TEMP VIEW staged_hashed AS
SELECT
    customer_id,
    name,
    email,
    city,
    md5(concat_ws('||', name, email, city)) AS row_hash
FROM staging_customer;
```

### Step 2 — Identify changed and new customers

```sql
CREATE OR REPLACE TEMP VIEW changes AS
SELECT
    s.customer_id,
    s.name,
    s.email,
    s.city,
    s.row_hash,
    d.city       AS old_city,      -- for prev_city logic
    d.prev_city  AS old_prev_city,
    d.customer_sk IS NOT NULL AS is_existing
FROM staged_hashed AS s
LEFT JOIN dim_customer AS d
    ON  d.customer_id = s.customer_id
    AND d.is_current  = TRUE
WHERE d.customer_id IS NULL              -- new customer
   OR d.row_hash   <> s.row_hash;        -- changed customer
```

### Step 3 — Expire active rows for changed customers (Type 2)

Use `MERGE` instead of `UPDATE` so the operation is atomic and idempotent.

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT customer_id
    FROM changes
    WHERE is_existing = TRUE
) AS src
ON  tgt.customer_id = src.customer_id
AND tgt.is_current  = TRUE

WHEN MATCHED THEN
    UPDATE SET
        end_date   = current_timestamp(),
        is_current = FALSE;
```

### Step 4 — Insert new version rows (Type 1 + 2 + 3 combined)

```sql
INSERT INTO dim_customer
    (customer_id, name, email, city, prev_city, row_hash, start_date, end_date, is_current)
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.city,
    -- Type 3: carry forward old_city only when city actually changed
    CASE
        WHEN c.old_city IS NOT NULL AND c.old_city <> c.city THEN c.old_city
        WHEN c.old_city IS NOT NULL                          THEN c.old_prev_city
        ELSE NULL
    END                     AS prev_city,
    c.row_hash,
    current_timestamp()     AS start_date,
    NULL                    AS end_date,
    TRUE                    AS is_current
FROM changes AS c;
```

### Step 5 — Back-fill current `city` on all historical rows (Type 1 overlay)

This is the hallmark of Type 6: every version row reflects the customer's **current** city,
even though the row was created when the customer lived elsewhere.

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT
        c.customer_id,
        c.city AS current_city
    FROM dim_customer AS c
    WHERE c.is_current = TRUE
) AS cur
ON  tgt.customer_id = cur.customer_id
AND tgt.is_current  = FALSE           -- historical rows only
AND tgt.city       <> cur.current_city

WHEN MATCHED THEN
    UPDATE SET city = cur.current_city;
```

!!! warning "Step 5 trade-off"
    Back-filling `city` on historical rows overwrites the *original* city value on those rows.
    The original city is preserved in `prev_city` (on the row that introduced the change) and
    in the `start_date` / `end_date` timestamps.  If you need full original-value fidelity on
    every row, omit Step 5 and use pure Type 2 instead.

---

## :material-check-circle-outline: Final Output

After processing the staging batch:

**`dim_customer`**

| customer_sk | customer_id | name    | email                  | city | prev_city | is_current | start_date          | end_date            |
|-------------|-------------|---------|------------------------|------|-----------|------------|---------------------|---------------------|
| 1           | cust1       | Alice   | alice@example.com      | NY   | NULL      | true       | 2024-01-01 00:00:00 | NULL                |
| 2           | cust2       | Bobby   | bob@example.com        | TX   | NULL      | false      | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| 3           | cust2       | Bobby   | bob@newdomain.com      | TX   | CA        | true       | 2024-06-15 09:00:00 | NULL                |
| 4           | cust3       | Charlie | charlie@example.com    | WA   | NULL      | true       | 2024-06-15 09:00:00 | NULL                |

Notice:

- Row 2 (`cust2`, expired) now shows `city = TX` (current) — the Type 1 back-fill from Step 5.
- Row 3 (`cust2`, active) shows `prev_city = CA` — the Type 3 memory of the previous location.

---

## :material-flask-outline: Analytical Queries

### Current state of all customers

```sql
SELECT
    customer_id,
    name,
    email,
    city,
    prev_city,
    start_date
FROM dim_customer
WHERE is_current = TRUE
ORDER BY customer_id;
```

### Full version history for a customer

```sql
SELECT
    customer_sk,
    name,
    email,
    city,
    prev_city,
    start_date,
    COALESCE(end_date, current_timestamp()) AS end_date,
    is_current,
    DATEDIFF(
        COALESCE(end_date, current_timestamp()),
        start_date
    ) AS days_active
FROM dim_customer
WHERE customer_id = 'cust2'
ORDER BY start_date;
```

### Point-in-time snapshot — what did `cust2` look like on a specific date?

```sql
SELECT *
FROM dim_customer
WHERE customer_id = 'cust2'
  AND start_date  <= TIMESTAMP '2024-03-01 00:00:00'
  AND (end_date    > TIMESTAMP '2024-03-01 00:00:00' OR end_date IS NULL);
```

### Customers who have ever changed city

```sql
SELECT DISTINCT customer_id, prev_city AS previous_city, city AS current_city
FROM dim_customer
WHERE prev_city IS NOT NULL
  AND prev_city <> city
ORDER BY customer_id;
```

### Version count and tenure per customer

```sql
SELECT
    customer_id,
    COUNT(*)                                    AS total_versions,
    MIN(start_date)                             AS first_seen,
    SUM(DATEDIFF(
        COALESCE(end_date, current_timestamp()),
        start_date
    ))                                          AS total_days
FROM dim_customer
GROUP BY customer_id
ORDER BY total_versions DESC;
```

### Fact table join — enrich orders with **current** customer city

```sql
SELECT
    o.order_id,
    o.order_date,
    o.amount,
    c.name,
    c.city          AS current_city,   -- always current due to Type 1 back-fill
    c.prev_city
FROM orders         AS o
JOIN dim_customer   AS c
    ON  c.customer_id = o.customer_id
    AND c.start_date <= o.order_date
    AND (c.end_date   > o.order_date OR c.end_date IS NULL);
```

---

## :material-wrench-outline: Operational Enhancements

### Add audit columns

Extend the table with columns that answer *who*, *why*, and *from where*:

```sql
ALTER TABLE dim_customer ADD COLUMNS (
    change_reason  STRING,    -- 'email_update', 'address_change', etc.
    changed_by     STRING,    -- ETL pipeline / user ID
    source_system  STRING     -- 'crm', 'salesforce', etc.
);
```

### Optimize with Delta Z-ordering

Co-locate data for the most common access pattern (lookup by `customer_id`):

```sql
OPTIMIZE dim_customer ZORDER BY (customer_id);
```

### Enforce data quality with constraints

```sql
ALTER TABLE dim_customer
ADD CONSTRAINT valid_dates
    CHECK (end_date IS NULL OR end_date > start_date);
```

### Track active-row count as a metric

```sql
SELECT
    COUNT(*)                               AS total_rows,
    SUM(CAST(is_current AS INT))           AS active_rows,
    COUNT(*) - SUM(CAST(is_current AS INT)) AS historical_rows
FROM dim_customer;
```

---

## :material-shield-outline: Common Pitfalls

| Pitfall | Consequence | Solution |
|---------|-------------|---------|
| Running Step 4 before Step 3 | New row inserted with no matching expired predecessor — `prev_city` logic breaks | Always expire (Step 3) before inserting (Step 4) |
| Back-fill (Step 5) omitted | Historical rows keep their original `city` — current-city GROUP BY is inaccurate | Include Step 5 every batch, or accept pure Type 2 behaviour |
| Step 5 on immutable storage | Delta MERGE on historical rows fails or violates audit policy | Omit Step 5; add a `current_city` join column instead |
| No `row_hash` guard on Step 3 | Unchanged rows expire and re-insert each run | Add `AND tgt.row_hash <> src.row_hash` to Step 3 MERGE |
| Duplicate keys in staging | Double expiry + double insert — phantom versions | Deduplicate with `ROW_NUMBER()` before Step 1 |
| `prev_city` set when only email changed | City shift falsely recorded | `CASE WHEN old_city <> new_city THEN old_city` guards the assignment |
| Querying without `is_current` | Multiple rows returned per customer — unexpected duplicates | Create a `dim_customer_current` view or always filter `WHERE is_current = TRUE` |

---

## :material-lightbulb-outline: When to Use SCD Type 6

!!! success "Good fit"
    - You need **point-in-time** fact-table joins (exact state at order/event time)
    - AND reports must reflect **current location / segment** without extra joins
    - AND stakeholders want to see **what changed** (prev_city, prev_tier)
    - Data warehouse where a **single dimension table** is simpler to govern than two tables (Type 5)

!!! failure "Not a good fit"
    - Very high change frequency — the back-fill in Step 5 touches many rows and strains Delta MERGE
    - You only need current state — use Type 1 (no history overhead)
    - You need unlimited previous-value columns — Type 3 does not scale beyond 1–2 tracked attributes
    - Strict immutability is required — Step 5 mutates historical rows

---

## :material-swap-horizontal: SCD Type Comparison

| Scenario | Recommended Type |
|----------|-----------------|
| Only current state, no history needed | Type 1 |
| Full history, point-in-time accuracy | Type 2 |
| Current + one previous value per attribute | Type 3 |
| Current + history in separate table, no FK | Type 4 |
| Current + history in separate table, FK link | Type 5 |
| Current + history + previous value, single table | **Type 6** |
