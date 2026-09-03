# :material-shield-check-outline: Key Constraints in Delta

Delta Lake supports four types of table constraints. Only `NOT NULL` and `CHECK` are
**enforced** at write time. `PRIMARY KEY`, `FOREIGN KEY`, and `UNIQUE` are
**informational** — stored in the metastore but not checked during inserts or updates.

---

## :material-code-tags: Syntax

```sql
-- NOT NULL (enforced)
ALTER TABLE customers ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE customers ALTER COLUMN name        SET NOT NULL;

-- CHECK constraint (enforced)
ALTER TABLE orders ADD CONSTRAINT valid_amount    CHECK (amount > 0);
ALTER TABLE orders ADD CONSTRAINT valid_status    CHECK (status IN ('PENDING','PROCESSING','SHIPPED','CANCELLED'));
ALTER TABLE orders ADD CONSTRAINT valid_dates     CHECK (ship_date IS NULL OR ship_date >= order_date);

-- Primary key (informational)
ALTER TABLE customers ADD CONSTRAINT customers_pk PRIMARY KEY (customer_id);

-- Foreign key (informational)
ALTER TABLE orders ADD CONSTRAINT orders_customer_fk
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id);

-- Unique constraint (informational)
ALTER TABLE customers ADD CONSTRAINT customers_email_uq UNIQUE (email);

-- Drop a constraint
ALTER TABLE orders DROP CONSTRAINT valid_amount;

-- List all constraints
SHOW CONSTRAINTS ON orders;
DESCRIBE TABLE EXTENDED orders;    -- constraints appear in table properties
```

---

## :material-information-outline: Behavior

| Constraint | Enforced at write? | Raises error on violation? |
|-----------|-------------------|--------------------------|
| `NOT NULL` | Yes | Yes — `AnalysisException` |
| `CHECK` | Yes | Yes — `DeltaInvariantViolationException` |
| `PRIMARY KEY` | No | No |
| `FOREIGN KEY` | No | No |
| `UNIQUE` | No | No |

1. `NOT NULL` is enforced on every `INSERT`, `UPDATE`, and `MERGE` — a null value for the column raises an error and aborts the write.
2. `CHECK` expressions can reference any column in the table and support any SQL predicate expression.
3. Both `NOT NULL` and `CHECK` scan the existing table data when added to a non-empty table — the `ALTER TABLE ADD CONSTRAINT` command fails if existing rows already violate the constraint.
4. Informational constraints (`PRIMARY KEY`, `FOREIGN KEY`, `UNIQUE`) are used by the Catalyst optimizer for join elimination and by tools like Unity Catalog, Tableau, and Power BI for data model inference.
5. Adding a constraint does not lock the table from reads; writes are blocked only during the constraint-check scan.

---

## :material-flask-outline: Practical Examples

### Create a table with enforced constraints

```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id    BIGINT         NOT NULL,
    customer_id BIGINT         NOT NULL,
    order_date  DATE           NOT NULL,
    amount      DECIMAL(18, 2) NOT NULL,
    status      STRING         NOT NULL,
    ship_date   DATE
)
USING DELTA
CONSTRAINT orders_pk          PRIMARY KEY  (order_id),
CONSTRAINT orders_amount_chk  CHECK        (amount > 0),
CONSTRAINT orders_status_chk  CHECK        (status IN ('PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED')),
CONSTRAINT orders_dates_chk   CHECK        (ship_date IS NULL OR ship_date >= order_date);
```

### Add NOT NULL to an existing column

```sql
-- Fails if any existing row has a NULL in that column
ALTER TABLE products ALTER COLUMN sku  SET NOT NULL;
ALTER TABLE products ALTER COLUMN name SET NOT NULL;
```

### Add a CHECK constraint to an existing table

```sql
-- The command scans all existing rows — fails if any row violates the check
ALTER TABLE inventory ADD CONSTRAINT positive_qty CHECK (quantity_on_hand >= 0);
```

### Test that constraints are enforced

```sql
-- This INSERT should fail: amount is negative
INSERT INTO orders (order_id, customer_id, order_date, amount, status)
VALUES (999, 1, '2024-06-01', -50.00, 'PENDING');
-- DeltaInvariantViolationException: CHECK constraint valid_amount (amount > 0) violated

-- This INSERT should fail: status is not in the allowed list
INSERT INTO orders (order_id, customer_id, order_date, amount, status)
VALUES (1000, 1, '2024-06-01', 100.00, 'UNKNOWN');
-- DeltaInvariantViolationException: CHECK constraint orders_status_chk violated
```

### Remove a constraint that is no longer valid

```sql
-- Remove old status constraint before adding a new one with updated allowed values
ALTER TABLE orders DROP CONSTRAINT orders_status_chk;

ALTER TABLE orders ADD CONSTRAINT orders_status_chk
    CHECK (status IN ('PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED','REFUNDED'));
```

### List all constraints on a table

```sql
SHOW CONSTRAINTS ON orders;
-- | name                | constraint_type | constraint_definition                        |
-- |---------------------|-----------------|----------------------------------------------|
-- | orders_pk           | PRIMARY KEY     | PRIMARY KEY (order_id)                       |
-- | orders_amount_chk   | CHECK           | amount > 0                                   |
-- | orders_status_chk   | CHECK           | status IN ('PENDING',...)                    |
-- | orders_customer_fk  | FOREIGN KEY     | FOREIGN KEY (customer_id) → customers        |
```

### CHECK constraint for cross-column validation

```sql
-- Discount must be less than unit price
ALTER TABLE order_lines
ADD CONSTRAINT valid_discount CHECK (discount IS NULL OR discount < unit_price);

-- end_date must be after start_date, or NULL (open-ended)
ALTER TABLE subscriptions
ADD CONSTRAINT valid_date_range CHECK (end_date IS NULL OR end_date > start_date);
```

### Pre-validate data before adding a constraint

```sql
-- Check for constraint violations in existing data BEFORE running ALTER TABLE
-- (avoids a long-running ALTER that ultimately fails)

-- Check for negative amounts
SELECT COUNT(*) AS violations FROM orders WHERE amount <= 0;

-- Check for invalid statuses
SELECT status, COUNT(*) AS cnt
FROM orders
WHERE status NOT IN ('PENDING','PROCESSING','SHIPPED','DELIVERED','CANCELLED')
GROUP BY status;

-- Only add constraint if violations = 0
ALTER TABLE orders ADD CONSTRAINT orders_amount_chk CHECK (amount > 0);
```

---

## :material-play-circle-outline: Worked Example

Self-contained — before writing to a Delta table guarded by `NOT NULL` and a
`CHECK (amount >= 0)` constraint, pre-validate the batch so the write can't fail midway.

```sql
CREATE OR REPLACE TEMP VIEW staging_orders AS
SELECT * FROM VALUES
    (1, CAST(120.00 AS DECIMAL(10, 2))),
    (2, CAST(-5.00  AS DECIMAL(10, 2))),   -- violates CHECK (amount >= 0)
    (3, CAST(NULL   AS DECIMAL(10, 2)))    -- violates NOT NULL
AS t (order_id, amount);

-- Pre-validation: flag rows that WOULD be rejected by the Delta constraints
SELECT
    order_id,
    amount,
    (amount IS NULL)                       AS fails_not_null,
    (amount IS NOT NULL AND amount < 0)    AS fails_check
FROM staging_orders
WHERE amount IS NULL OR amount < 0
ORDER BY order_id;
-- order_id | amount | fails_not_null | fails_check
-- 2        | -5.00  | false          | true
-- 3        | NULL   | true           | false
```

!!! success "Fail fast, in staging"
    Delta enforces `NOT NULL` and `CHECK` **at write time** — one bad row aborts the whole
    write. Running this pre-check lets you quarantine rows 2 and 3 first, so the load into
    the constrained table always succeeds.

---

## :material-lightbulb-outline: When to Use Each Constraint

| Constraint | Use for |
|-----------|---------|
| `NOT NULL` | Any column that must always have a value (PKs, FKs, business-critical fields) |
| `CHECK` | Value range validation, allowed-value lists, cross-column business rules |
| `PRIMARY KEY` | Document the unique row identifier; enable optimizer and BI tool hints |
| `FOREIGN KEY` | Document table relationships; enable Unity Catalog lineage |
| `UNIQUE` | Document uniqueness of a non-PK column (e.g., email, SKU) |

!!! tip "Pre-validate before adding constraints to existing tables"
    `ALTER TABLE ADD CONSTRAINT` on a non-empty Delta table scans **all existing rows**.
    Always run the validation query first — if violations exist, the `ALTER TABLE`
    command will fail and roll back, wasting time on a full table scan.
