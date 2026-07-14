# :material-layers: SCD Type 5

SCD **Type 5** is a hybrid pattern that combines:

- **Type 1** — overwrites current dimension values in-place (fast, no history in the main table)
- **Type 4** — keeps a separate *mini-dimension* table that stores full attribute history

The result is a two-table design where BI dashboards always query a clean, current-values table
while auditors and analysts can still trace every change through the linked history table.

---

## :material-layers-triple: How Type 5 Differs from Its Parents

| Feature | Type 1 | Type 4 | **Type 5** |
|---------|--------|--------|-----------|
| Current values | Overwritten | Overwritten | Overwritten |
| Full history | :material-close: No | :material-check: Separate table | :material-check: Separate table |
| Foreign key to history | :material-close: No | :material-close: No | :material-check: `hist_key` in main dim |
| Query complexity | Low | Medium | Low (current) / Medium (history) |
| Storage overhead | Minimal | Moderate | Moderate |

The key differentiator over pure Type 4 is the `hist_key` column in the main dimension.
It acts as a foreign key pointing to the *latest* row in the history table, making joins trivial.

---

## :material-toy-brick: Table Design

### Main Dimension — `dim_customer`

Holds current attribute values plus a reference to the latest history entry.

```sql
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id  STRING    NOT NULL,
    name         STRING,
    email        STRING,
    city         STRING,
    hist_key     BIGINT,       -- FK → dim_customer_history.hist_key (latest row)
    updated_at   TIMESTAMP
)
USING DELTA;
```

### Mini-Dimension (History) — `dim_customer_history`

Stores one row per attribute change, forming a full audit trail.

```sql
CREATE TABLE IF NOT EXISTS dim_customer_history (
    hist_key     BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id  STRING,
    name         STRING,
    email        STRING,
    city         STRING,
    row_hash     STRING,
    valid_from   TIMESTAMP,
    valid_to     TIMESTAMP     -- NULL means currently active
)
USING DELTA;
```

!!! note "Identity column support"
    `GENERATED ALWAYS AS IDENTITY` is available in Databricks Runtime 10.4 LTS and later.
    On open-source Spark, generate `hist_key` using `MONOTONICALLY_INCREASING_ID()` or a UUID.

---

## :material-tray-arrow-down: Seed Data

Populate both tables with an initial customer snapshot.

```sql
-- Seed the history table first so hist_key values exist
INSERT INTO dim_customer_history
    (customer_id, name, email, city, row_hash, valid_from, valid_to)
VALUES
    ('cust1', 'Alice', 'alice@example.com', 'NY',
        md5('Alice||alice@example.com||NY'), '2024-01-01', NULL),
    ('cust2', 'Bob',   'bob@example.com',   'CA',
        md5('Bob||bob@example.com||CA'),   '2024-01-01', NULL);

-- Seed the main dimension, referencing the history keys just created
INSERT INTO dim_customer (customer_id, name, email, city, hist_key, updated_at)
SELECT
    h.customer_id,
    h.name,
    h.email,
    h.city,
    h.hist_key,
    h.valid_from
FROM dim_customer_history h
WHERE h.valid_to IS NULL;
```

---

## :material-database-import: Incoming Staging Data

```sql
CREATE OR REPLACE TEMP VIEW staging_customer AS
SELECT *
FROM VALUES
    ('cust1', 'Alice',   'alice@example.com',    'NY'),  -- no change
    ('cust2', 'Bobby',   'bob@newdomain.com',     'TX'),  -- email + city changed
    ('cust3', 'Charlie', 'charlie@example.com',   'WA')   -- new customer
AS t(customer_id, name, email, city);
```

---

## :material-repeat: Step-by-Step SCD Type 5 Logic

### Step 1 — Compute row hash for incoming records

Hash the slowly-changing attributes so a single string comparison detects any change.

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

### Step 2 — Close the previous history row when attributes change

Set `valid_to` on the currently-active history row for any customer whose hash differs.

```sql
MERGE INTO dim_customer_history AS hist
USING (
    SELECT
        s.customer_id,
        s.row_hash AS new_hash
    FROM staged_hashed s
    JOIN dim_customer c
        ON c.customer_id = s.customer_id
    WHERE s.row_hash <> md5(concat_ws('||', c.name, c.email, c.city))
) AS changed
ON  hist.customer_id = changed.customer_id
AND hist.valid_to    IS NULL

WHEN MATCHED THEN
    UPDATE SET valid_to = current_timestamp();
```

### Step 3 — Insert a new history row for changed or new customers

```sql
INSERT INTO dim_customer_history
    (customer_id, name, email, city, row_hash, valid_from, valid_to)
SELECT
    s.customer_id,
    s.name,
    s.email,
    s.city,
    s.row_hash,
    current_timestamp(),
    NULL
FROM staged_hashed AS s
LEFT JOIN dim_customer AS c
    ON c.customer_id = s.customer_id
WHERE
    c.customer_id IS NULL                          -- new customer
    OR s.row_hash <> md5(concat_ws('||', c.name, c.email, c.city));  -- changed
```

### Step 4 — Upsert the main dimension with current values and updated `hist_key`

```sql
MERGE INTO dim_customer AS tgt
USING (
    SELECT
        h.customer_id,
        h.name,
        h.email,
        h.city,
        h.hist_key,
        current_timestamp() AS updated_at
    FROM dim_customer_history AS h
    JOIN staged_hashed        AS s
        ON  s.customer_id = h.customer_id
        AND s.row_hash    = h.row_hash
    WHERE h.valid_to IS NULL    -- pick only the freshly-inserted (active) row
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN
    UPDATE SET
        name       = src.name,
        email      = src.email,
        city       = src.city,
        hist_key   = src.hist_key,
        updated_at = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, hist_key, updated_at)
    VALUES (src.customer_id, src.name, src.email, src.city, src.hist_key, src.updated_at);
```

!!! tip "Run order matters"
    Steps 2 → 3 → 4 must execute in sequence.
    Step 2 closes old rows before Step 3 opens new ones; Step 4 reads the freshly-opened rows
    to get the correct `hist_key`.

---

## :material-check-circle-outline: Final Output

After processing the staging batch:

**`dim_customer`** (current values, always 1 row per customer)

| customer_id | name    | email                  | city | hist_key | updated_at          |
|-------------|---------|------------------------|------|----------|---------------------|
| cust1       | Alice   | alice@example.com      | NY   | 1        | 2024-01-01 00:00:00 |
| cust2       | Bobby   | bob@newdomain.com      | TX   | 3        | 2024-06-15 09:00:00 |
| cust3       | Charlie | charlie@example.com    | WA   | 4        | 2024-06-15 09:00:00 |

**`dim_customer_history`** (full audit trail)

| hist_key | customer_id | name    | email                  | city | valid_from          | valid_to            |
|----------|-------------|---------|------------------------|------|---------------------|---------------------|
| 1        | cust1       | Alice   | alice@example.com      | NY   | 2024-01-01 00:00:00 | NULL                |
| 2        | cust2       | Bob     | bob@example.com        | CA   | 2024-01-01 00:00:00 | 2024-06-15 09:00:00 |
| 3        | cust2       | Bobby   | bob@newdomain.com      | TX   | 2024-06-15 09:00:00 | NULL                |
| 4        | cust3       | Charlie | charlie@example.com    | WA   | 2024-06-15 09:00:00 | NULL                |

Seed generated `hist_key` 1 and 2. The batch generated 3 (new `cust2` version) and 4 (new `cust3` row).

---

## :material-flask-outline: Analytical Queries

### All current customers

```sql
SELECT *
FROM dim_customer
ORDER BY customer_id;
```

### Full change history for a specific customer

```sql
SELECT
    hist_key,
    name,
    email,
    city,
    valid_from,
    COALESCE(valid_to, current_timestamp()) AS valid_to,
    DATEDIFF(
        COALESCE(valid_to, current_timestamp()),
        valid_from
    ) AS days_active
FROM dim_customer_history
WHERE customer_id = 'cust2'
ORDER BY valid_from;
```

### Join current dimension to its latest history row (denormalised view)

```sql
SELECT
    c.customer_id,
    c.name          AS current_name,
    c.city          AS current_city,
    h.valid_from    AS last_changed_at,
    h.hist_key
FROM dim_customer          AS c
JOIN dim_customer_history  AS h
    ON h.hist_key = c.hist_key;
```

### Customers who changed city in the last 90 days

```sql
SELECT DISTINCT h.customer_id
FROM dim_customer_history AS h
WHERE h.valid_from >= current_timestamp() - INTERVAL 90 DAYS
  AND h.valid_to   IS NOT NULL    -- rows that were subsequently closed = changed
  AND EXISTS (
      SELECT 1
      FROM dim_customer_history AS prev
      WHERE prev.customer_id = h.customer_id
        AND prev.hist_key    < h.hist_key
        AND prev.city       <> h.city
  );
```

### Count attribute versions per customer

```sql
SELECT
    customer_id,
    COUNT(*)                            AS total_versions,
    MIN(valid_from)                     AS first_seen,
    MAX(COALESCE(valid_to, current_timestamp())) AS last_seen
FROM dim_customer_history
GROUP BY customer_id
ORDER BY total_versions DESC;
```

### Fact-table join — enrich orders with current city

```sql
SELECT
    o.order_id,
    o.order_date,
    o.amount,
    c.name,
    c.city          AS current_city,
    h.valid_from    AS city_since
FROM orders                  AS o
JOIN dim_customer            AS c USING (customer_id)
JOIN dim_customer_history    AS h ON h.hist_key = c.hist_key;
```

---

## :material-shield-outline: Common Pitfalls

| Pitfall | Consequence | Solution |
|---------|-------------|---------|
| Running Step 4 before Step 3 | `hist_key` join finds no active row — inserts fail | Always run Steps 2 → 3 → 4 in sequence |
| Step 2 misses a changed row | Stale `hist_key` in main dimension | Ensure hash comparison covers all tracked columns |
| `hist_key` not updated in Step 4 | Main dim still points to the old (now-closed) history row | Confirm Step 4 MERGE sources `WHERE valid_to IS NULL` |
| Duplicate keys in staging | Double-insert into history + double-update of main dim | Deduplicate staging with `ROW_NUMBER()` before Step 1 |
| Identity column unavailable | `hist_key` generation fails | Use `MONOTONICALLY_INCREASING_ID()` or `UUID()` as fallback |
| History table not partitioned | Full scans on per-customer history queries | Add `PARTITIONED BY (customer_id)` to the history table |

---

## :material-lightbulb-outline: When to Use SCD Type 5

!!! success "Good fit"
    - BI dashboards need clean, fast access to **current values** without `is_current` filters
    - Analytics also requires **full change history** for audits, compliance, or ML feature stores
    - You want to **avoid the complexity** of Type 2's effective-date range queries in day-to-day reporting
    - Dimension tables are large and you need **partition pruning** on the main dim for performance

!!! failure "Not a good fit"
    - Change history is never queried — use Type 1 instead (simpler, less storage)
    - You need point-in-time snapshots of the main fact table — use Type 2 instead
    - The mini-dimension grows extremely fast (high-cardinality, frequent changes) — history table becomes a bottleneck
    - Your platform does not support identity columns or efficient MERGE — operational complexity increases

---

## :material-swap-horizontal: Type Comparison Summary

| Scenario | Recommended Type |
|----------|-----------------|
| Only current state matters | Type 1 |
| Full history with effective dates in main table | Type 2 |
| Mix: current + partial history as extra columns | Type 3 |
| Separate current and history tables, no FK link | Type 4 |
| Separate tables **with** `hist_key` foreign key | **Type 5** |
| Type 2 history + Type 1 overwrite hybrid | Type 6 |
