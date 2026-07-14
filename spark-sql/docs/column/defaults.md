# :material-table-settings: Column Defaults & Generated Columns

Column defaults and generated columns let the database automatically supply or
compute a column value when a row is inserted. Delta Lake supports both `DEFAULT`
expressions and `GENERATED ALWAYS AS` computed columns.

---

## :material-code-tags: Syntax

```sql
-- DEFAULT value (scalar expression)
CREATE TABLE events (
    event_id    BIGINT        NOT NULL,
    event_type  STRING        NOT NULL,
    created_at  TIMESTAMP     NOT NULL DEFAULT current_timestamp(),
    is_active   BOOLEAN       NOT NULL DEFAULT TRUE,
    status      STRING        NOT NULL DEFAULT 'PENDING'
)
USING DELTA;

-- GENERATED ALWAYS AS (computed from other columns — cannot be inserted manually)
CREATE TABLE orders (
    order_id    BIGINT         NOT NULL,
    order_date  DATE           NOT NULL,
    amount      DECIMAL(18, 2) NOT NULL,
    tax_rate    DECIMAL(5, 4)  NOT NULL DEFAULT 0.2,
    tax_amount  DECIMAL(18, 2) GENERATED ALWAYS AS (ROUND(amount * tax_rate, 2)),
    total       DECIMAL(18, 2) GENERATED ALWAYS AS (ROUND(amount + amount * tax_rate, 2)),
    order_year  INT            GENERATED ALWAYS AS (YEAR(order_date))
)
USING DELTA;

-- GENERATED ALWAYS AS IDENTITY (auto-increment surrogate key)
CREATE TABLE dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    customer_id BIGINT NOT NULL
)
USING DELTA;

-- Add a default to an existing column
ALTER TABLE events ALTER COLUMN status SET DEFAULT 'PENDING';

-- Drop a default
ALTER TABLE events ALTER COLUMN status DROP DEFAULT;
```

---

## :material-information-outline: Behavior

1. `DEFAULT` values are used when an `INSERT` omits the column or explicitly uses `DEFAULT` in the `VALUES` list.
2. `GENERATED ALWAYS AS` columns are **read-only** — you cannot insert or update them directly; Delta computes the value from the expression on every write.
3. `GENERATED ALWAYS AS IDENTITY` auto-increments but is **not contiguous** — gaps can appear due to failed transactions or concurrent inserts.
4. Generated columns on partitioned Delta tables can be used as **partition columns** — e.g., partition by `order_year` which is derived from `order_date`.
5. `DEFAULT current_timestamp()` captures the wall-clock time of the write, not query parsing time.
6. All these features require **Delta Lake** (`USING DELTA`) — they are not available for Parquet/ORC tables.

---

## :material-flask-outline: Practical Examples

### INSERT using DEFAULT

```sql
-- Omit created_at and status — Delta fills in the DEFAULT values
INSERT INTO events (event_id, event_type)
VALUES (1, 'page_view'),
       (2, 'click'),
       (3, 'purchase');
-- created_at = current_timestamp(), is_active = TRUE, status = 'PENDING'
```

### Explicit DEFAULT keyword in VALUES

```sql
INSERT INTO events (event_id, event_type, created_at, is_active, status)
VALUES
    (4, 'login',  DEFAULT, DEFAULT, 'ACTIVE'),  -- created_at and is_active use DEFAULT
    (5, 'logout', DEFAULT, FALSE,   DEFAULT);   -- is_active overridden; status uses DEFAULT
```

### Generated computed columns

```sql
CREATE TABLE invoices (
    invoice_id  BIGINT         NOT NULL,
    subtotal    DECIMAL(18, 2) NOT NULL,
    tax_rate    DECIMAL(5, 4)  NOT NULL DEFAULT 0.20,
    tax_amount  DECIMAL(18, 2) GENERATED ALWAYS AS (ROUND(subtotal * tax_rate, 2)),
    total       DECIMAL(18, 2) GENERATED ALWAYS AS (subtotal + ROUND(subtotal * tax_rate, 2))
)
USING DELTA;

-- Insert — only supply base columns; tax_amount and total are auto-computed
INSERT INTO invoices (invoice_id, subtotal)
VALUES (1001, 500.00);
-- tax_amount = 100.00, total = 600.00
```

### Partition by a generated column

```sql
CREATE TABLE events (
    event_id    BIGINT    NOT NULL,
    event_ts    TIMESTAMP NOT NULL,
    event_type  STRING,
    payload     STRING,
    event_date  DATE GENERATED ALWAYS AS (CAST(event_ts AS DATE))
)
USING DELTA
PARTITIONED BY (event_date);

-- Insert only event_ts — event_date is generated and used for partitioning
INSERT INTO events (event_id, event_ts, event_type)
VALUES (1, '2024-06-01 14:30:00', 'click');
-- Automatically partitioned into event_date=2024-06-01/
```

### Query generated columns (they behave like regular columns)

```sql
SELECT
    event_id,
    event_ts,
    event_date,                 -- generated column is readable
    event_type
FROM events
WHERE event_date = '2024-06-01';   -- partition pruning works on generated column
```

### Identity column — surrogate key

```sql
CREATE TABLE dim_product (
    product_sk   BIGINT GENERATED ALWAYS AS IDENTITY,
    product_id   STRING NOT NULL,
    name         STRING NOT NULL,
    category     STRING
)
USING DELTA
CONSTRAINT dim_product_pk PRIMARY KEY (product_sk);

-- Insert without specifying product_sk
INSERT INTO dim_product (product_id, name, category)
VALUES ('SKU-001', 'Widget Pro', 'Electronics');
-- product_sk is auto-assigned: 1, 2, 3, ...
```

### Add a DEFAULT to an existing table

```sql
ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'PENDING';

-- Verify
DESCRIBE TABLE EXTENDED orders;
-- status column now shows: DEFAULT 'PENDING'
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Pattern |
|----------|---------|
| Auto-populate audit timestamp | `created_at TIMESTAMP DEFAULT current_timestamp()` |
| Auto-populate a status flag | `status STRING DEFAULT 'PENDING'` |
| Auto-compute a derived metric | `GENERATED ALWAYS AS (expression)` |
| Partition by a derived date field | `GENERATED` date column + `PARTITIONED BY` |
| Surrogate key for dimension table | `GENERATED ALWAYS AS IDENTITY` |

!!! note "Delta only"
    `DEFAULT`, `GENERATED ALWAYS AS`, and `GENERATED ALWAYS AS IDENTITY` are Delta Lake
    features. They are not available for Parquet, ORC, or Hive tables in Spark SQL.
