# :material-key-variant: Primary Keys

A primary key uniquely identifies each row in a table. In Spark SQL / Delta Lake,
primary key constraints are **informational** — declared for documentation and optimizer
hints, but not enforced. The pipeline must ensure uniqueness.

---

## :material-code-tags: Syntax

```sql
-- Informational primary key (Delta / Unity Catalog)
CREATE TABLE customers (
    customer_id BIGINT   NOT NULL,
    name        STRING   NOT NULL,
    email       STRING,
    created_at  TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
CONSTRAINT customers_pk PRIMARY KEY (customer_id);

-- Add primary key to an existing table
ALTER TABLE customers
ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);

-- Drop a primary key constraint
ALTER TABLE customers
DROP CONSTRAINT customers_pk;

-- Check existing constraints
SHOW CONSTRAINTS ON customers;
DESCRIBE TABLE EXTENDED customers;
```

---

## :material-information-outline: Behavior

1. `PRIMARY KEY` implies `NOT NULL` — the column must be declared `NOT NULL` separately in Spark SQL (the constraint itself does not enforce nullability in older runtimes).
2. Primary key declarations are stored in the metastore and used by BI tools (Tableau, Power BI) for data model inference.
3. The Catalyst optimizer can use `PRIMARY KEY` declarations to eliminate redundant joins and improve cardinality estimation.
4. Duplicate primary key values do **not** raise a write error — use `MERGE` or pre-insert deduplication to enforce uniqueness.
5. For Delta tables, pair the primary key column with a `NOT NULL` constraint (enforced) to guarantee non-null values.

---

## :material-flask-outline: Practical Examples

### Declare a primary key at table creation

```sql
CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGINT    NOT NULL,
    name        STRING    NOT NULL,
    email       STRING    NOT NULL,
    region      STRING,
    created_at  TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Customer dimension — customer_id is the primary key'
CONSTRAINT customers_pk PRIMARY KEY (customer_id);
```

### Enforce NOT NULL (the only Delta-enforced part)

```sql
-- NOT NULL is enforced at write time; duplicate values are not
ALTER TABLE customers ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE customers ALTER COLUMN name        SET NOT NULL;
```

### Upsert pattern — maintain PK uniqueness via MERGE

```sql
-- Dedup source first, then MERGE to maintain uniqueness
WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
        FROM staging_customers
    )
    WHERE rn = 1
)
MERGE INTO customers AS t
USING deduped AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET name = s.name, email = s.email
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, region, created_at)
    VALUES (s.customer_id, s.name, s.email, s.region, current_timestamp());
```

### Detect PK violations before loading

```sql
-- Check for duplicates in staging before INSERT
SELECT customer_id, COUNT(*) AS cnt
FROM staging_customers
GROUP BY customer_id
HAVING cnt > 1;
-- If this returns rows, dedup before loading
```

### Validate PK uniqueness in the target table

```sql
-- Should always return 0 rows in a clean table
SELECT customer_id, COUNT(*) AS cnt
FROM customers
GROUP BY customer_id
HAVING cnt > 1;
```

### Find PK gaps (integer sequence)

```sql
WITH RECURSIVE seq AS (
    SELECT MIN(customer_id) AS id FROM customers
    UNION ALL
    SELECT id + 1 FROM seq WHERE id < (SELECT MAX(customer_id) FROM customers)
)
SELECT seq.id AS missing_customer_id
FROM seq
LEFT JOIN customers c ON seq.id = c.customer_id
WHERE c.customer_id IS NULL;
```

---

## :material-play-circle-outline: Worked Example

Self-contained — paste and run. A staging batch arrives with a duplicate
`customer_id`; the audit query surfaces the offending key before you load.

```sql
CREATE OR REPLACE TEMP VIEW staging_customers AS
SELECT * FROM VALUES
    (1, 'Alice', 'alice@acme.io'),
    (2, 'Bob',   'bob@acme.io'),
    (2, 'Bob R', 'bob.r@acme.io'),   -- duplicate customer_id
    (3, 'Carol', 'carol@acme.io')
AS t (customer_id, name, email);

-- Primary-key audit: which values appear more than once?
SELECT customer_id, COUNT(*) AS cnt
FROM staging_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY customer_id;
-- customer_id | cnt
-- 2           | 2      ← must be resolved before INSERT
```

!!! success "Interpretation"
    A clean primary key returns **zero rows** here. Because Spark does not enforce
    uniqueness, this check is your guardrail — run it in staging, then dedup or `MERGE`.

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommendation |
|----------|---------------|
| Declare key for BI tool data model | `CONSTRAINT ... PRIMARY KEY` |
| Enforce non-null on PK column | `ALTER COLUMN ... SET NOT NULL` (Delta-enforced) |
| Upsert without duplicates | `MERGE` with deduped source |
| Audit PK uniqueness | `GROUP BY pk HAVING COUNT(*) > 1` |
| Integer PK gaps analysis | Recursive CTE spine + `LEFT JOIN` |

!!! warning "No write-time enforcement"
    Spark SQL does not reject writes that violate primary key uniqueness.
    Always validate uniqueness in the staging layer before loading into the target table.
