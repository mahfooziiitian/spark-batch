# :material-identifier: Surrogate Keys

A surrogate key is a system-generated, meaningless unique identifier assigned to each
row. It decouples physical row identity from business meaning, making tables resilient
to natural key changes and enabling SCD Type 2 versioning.

---

## :material-code-tags: Syntax

```sql
-- Auto-increment identity column (Delta / Databricks)
CREATE TABLE dim_customer (
    customer_sk  BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    customer_id  BIGINT    NOT NULL,   -- natural / business key
    name         STRING    NOT NULL,
    email        STRING,
    is_current   BOOLEAN   NOT NULL DEFAULT TRUE
)
USING DELTA;

-- UUID surrogate key (any Spark version)
CREATE TABLE dim_product (
    product_sk  STRING    NOT NULL,    -- UUID
    product_id  STRING    NOT NULL,    -- natural key
    name        STRING,
    category    STRING
)
USING DELTA;

-- Hash surrogate key (deterministic, idempotent)
CREATE TABLE dim_location (
    location_sk STRING NOT NULL,       -- md5 hash of natural key
    city        STRING,
    state       STRING,
    country     STRING
)
USING DELTA;
```

---

## :material-information-outline: Behavior

1. `GENERATED ALWAYS AS IDENTITY` assigns a unique, auto-incrementing integer — **Databricks / Delta only**. Standard Spark does not support identity columns.
2. Identity columns are **not guaranteed to be contiguous** — gaps can appear due to failed transactions or concurrent inserts.
3. `UUID()` generates a universally unique random identifier per row — not deterministic across runs.
4. `md5(concat_ws('||', ...))` generates a deterministic surrogate from the natural key — the same input always produces the same hash, making it idempotent for upserts.
5. Surrogate keys are used as the join key in star schemas; the natural/business key is stored separately for lookups and deduplication.

---

## :material-flask-outline: Practical Examples

### Identity column (Databricks Delta)

```sql
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk  BIGINT  GENERATED ALWAYS AS IDENTITY,
    customer_id  BIGINT  NOT NULL,
    name         STRING  NOT NULL,
    email        STRING,
    region       STRING,
    valid_from   TIMESTAMP NOT NULL,
    valid_to     TIMESTAMP,
    is_current   BOOLEAN   NOT NULL
)
USING DELTA
CONSTRAINT dim_customer_pk PRIMARY KEY (customer_sk);
```

Insert — the identity value is assigned automatically:

```sql
INSERT INTO dim_customer (customer_id, name, email, region, valid_from, valid_to, is_current)
SELECT customer_id, name, email, region, current_timestamp(), NULL, TRUE
FROM staging_customers;
```

### UUID surrogate key

```sql
INSERT INTO dim_product (product_sk, product_id, name, category)
SELECT
    UUID()       AS product_sk,
    product_id,
    name,
    category
FROM staging_products
WHERE product_id NOT IN (SELECT product_id FROM dim_product);
```

!!! warning "UUID is not idempotent"
    `UUID()` generates a different value every time the query runs.
    Do **not** use it in upsert pipelines — re-running the insert will assign a different
    surrogate key to the same natural key. Use `md5` hash for idempotent pipelines.

### Hash surrogate key (idempotent)

```sql
INSERT INTO dim_location (location_sk, city, state, country)
SELECT
    md5(concat_ws('||', city, state, country)) AS location_sk,
    city,
    state,
    country
FROM staging_locations
WHERE md5(concat_ws('||', city, state, country))
      NOT IN (SELECT location_sk FROM dim_location);
```

### Surrogate key lookup for fact table loading

```sql
-- Resolve natural keys to surrogate keys before inserting into the fact table
INSERT INTO fact_orders
SELECT
    o.order_id,
    dc.customer_sk,
    dp.product_sk,
    o.order_date,
    o.quantity,
    o.amount
FROM staging_orders AS o
JOIN dim_customer AS dc
    ON dc.customer_id = o.customer_id AND dc.is_current = TRUE
JOIN dim_product AS dp
    ON dp.product_id  = o.product_id;
```

### SCD Type 2 — new surrogate key per version

```sql
-- Each version of a customer row gets its own surrogate key (identity column)
INSERT INTO dim_customer (customer_id, name, email, region, valid_from, valid_to, is_current)
SELECT
    s.customer_id,
    s.name,
    s.email,
    s.region,
    current_timestamp() AS valid_from,
    NULL                AS valid_to,
    TRUE                AS is_current
FROM staging_customers AS s
LEFT JOIN dim_customer AS d
    ON  d.customer_id = s.customer_id
    AND d.is_current  = TRUE
WHERE d.customer_id IS NULL
   OR md5(concat_ws('||', s.name, s.email, s.region))
   <> md5(concat_ws('||', d.name, d.email, d.region));
-- The identity column (customer_sk) is auto-assigned — no manual key generation needed
```

### Generate a sequential surrogate key without IDENTITY

```sql
-- Use ROW_NUMBER when IDENTITY is not available
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) + (
        SELECT COALESCE(MAX(customer_sk), 0) FROM dim_customer
    ) AS customer_sk,
    customer_id,
    name,
    email
FROM staging_customers;
```

### Audit: natural keys mapped to multiple surrogate keys

```sql
-- Useful for SCD Type 2: confirm each customer_id has at most one is_current = TRUE row
SELECT customer_id, COUNT(*) AS current_count
FROM dim_customer
WHERE is_current = TRUE
GROUP BY customer_id
HAVING current_count > 1;
```

---

## :material-swap-horizontal: Surrogate Key Strategies

| Strategy | Uniqueness | Idempotent | Use case |
|----------|-----------|-----------|---------|
| `IDENTITY` column | Guaranteed | No | Databricks Delta; simple dimensions |
| `UUID()` | Probabilistically unique | No | When natural key is unavailable and idempotency is not required |
| `md5(concat_ws(...))` | Hash collision possible (rare) | Yes | Idempotent pipelines; SCD hash keys |
| `ROW_NUMBER() + offset` | Unique per run | No | Non-Delta environments without IDENTITY |

---

## :material-lightbulb-outline: When to Use Surrogate Keys

| Scenario | Recommendation |
|----------|---------------|
| Dimension table in a star schema | Always use a surrogate key as the join key |
| SCD Type 2 versioned dimension | Identity column — one new SK per version |
| Idempotent pipeline (safe to re-run) | `md5` hash surrogate |
| No natural unique key exists | `UUID()` or `IDENTITY` |
| Non-Databricks Spark (no IDENTITY) | `ROW_NUMBER() + MAX(existing_sk)` |
