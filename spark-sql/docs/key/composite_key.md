# :material-table-multiple: Composite Keys

A composite key uses two or more columns together to uniquely identify a row. It is the
natural key pattern for junction tables, order lines, time-series data, and any entity
whose identity depends on a combination of attributes.

---

## :material-code-tags: Syntax

```sql
-- Composite primary key at CREATE TABLE
CREATE TABLE order_lines (
    order_id    BIGINT  NOT NULL,
    line_id     INT     NOT NULL,
    product_id  BIGINT  NOT NULL,
    quantity    INT,
    unit_price  DECIMAL(18, 2)
)
USING DELTA
CONSTRAINT order_lines_pk PRIMARY KEY (order_id, line_id);

-- Composite unique constraint
ALTER TABLE order_lines
ADD CONSTRAINT order_lines_uq UNIQUE (order_id, product_id);

-- Multi-column foreign key
ALTER TABLE order_lines
ADD CONSTRAINT order_lines_fk
    FOREIGN KEY (order_id) REFERENCES orders (order_id);
```

---

## :material-information-outline: Behavior

1. Each column in a composite key should be declared `NOT NULL` individually — Spark SQL does not automatically enforce this from the constraint declaration.
2. A composite primary key is informational in Spark/Delta — uniqueness across the column combination must be maintained by the pipeline.
3. Composite keys require all columns to be included in `JOIN ON` conditions; omitting one column produces a cross-product fan-out.
4. For upserts, the `MERGE ON` clause must include **all columns** of the composite key.
5. Partition strategies for tables with composite keys should include the most selective key column in `PARTITIONED BY` to enable partition pruning.

---

## :material-flask-outline: Practical Examples

### Junction table (many-to-many)

```sql
CREATE TABLE IF NOT EXISTS product_categories (
    product_id  BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    assigned_at TIMESTAMP
)
USING DELTA
CONSTRAINT product_categories_pk PRIMARY KEY (product_id, category_id);
```

### Order lines with composite PK

```sql
CREATE TABLE IF NOT EXISTS order_lines (
    order_id    BIGINT         NOT NULL,
    line_id     INT            NOT NULL,
    product_id  BIGINT         NOT NULL,
    quantity    INT            NOT NULL,
    unit_price  DECIMAL(18, 2) NOT NULL,
    discount    DECIMAL(5, 2)  DEFAULT 0.0
)
USING DELTA
CONSTRAINT order_lines_pk PRIMARY KEY (order_id, line_id);
```

### Time-series with composite key (entity + timestamp)

```sql
CREATE TABLE IF NOT EXISTS device_readings (
    device_id    STRING    NOT NULL,
    reading_time TIMESTAMP NOT NULL,
    temperature  DOUBLE,
    humidity     DOUBLE,
    battery_pct  INT
)
USING DELTA
PARTITIONED BY (DATE(reading_time))
CONSTRAINT device_readings_pk PRIMARY KEY (device_id, reading_time);
```

### MERGE on composite key

```sql
MERGE INTO order_lines AS t
USING staging_order_lines AS s
    ON  t.order_id = s.order_id      -- all PK columns in ON clause
    AND t.line_id  = s.line_id
WHEN MATCHED THEN
    UPDATE SET quantity = s.quantity, unit_price = s.unit_price
WHEN NOT MATCHED THEN
    INSERT (order_id, line_id, product_id, quantity, unit_price)
    VALUES (s.order_id, s.line_id, s.product_id, s.quantity, s.unit_price);
```

### Detect composite key violations

```sql
-- Find duplicate (order_id, line_id) combinations
SELECT order_id, line_id, COUNT(*) AS cnt
FROM order_lines
GROUP BY order_id, line_id
HAVING cnt > 1;
```

### Dedup before loading — keep latest per composite key

```sql
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id, line_id
               ORDER BY updated_at DESC
           ) AS rn
    FROM staging_order_lines
)
INSERT INTO order_lines
SELECT order_id, line_id, product_id, quantity, unit_price
FROM deduped
WHERE rn = 1;
```

### Join on composite key — must include all columns

```sql
-- ✅ Correct: both PK columns in ON clause
SELECT
    ol.order_id,
    ol.line_id,
    ol.quantity,
    ol.unit_price,
    p.name AS product_name
FROM order_lines AS ol
JOIN products    AS p  ON ol.product_id = p.product_id
JOIN orders      AS o  ON ol.order_id   = o.order_id
WHERE o.order_date >= '2024-01-01';

-- ❌ Incorrect: joining only on order_id creates a fan-out
-- (every line for an order multiplied by every order row)
SELECT ol.*, o.*
FROM order_lines AS ol
JOIN orders AS o ON ol.order_id = o.order_id;   -- This is actually correct — orders.order_id is the PK
                                                  -- Fan-out occurs when joining fact to fact on partial key
```

### Composite key in partitioned table — partition pruning

```sql
-- Partition on the date component of the composite key for efficient queries
CREATE TABLE IF NOT EXISTS sensor_events (
    sensor_id    STRING    NOT NULL,
    event_time   TIMESTAMP NOT NULL,
    event_type   STRING,
    payload      STRING,
    event_date   DATE      NOT NULL    -- partition column derived from event_time
)
USING DELTA
PARTITIONED BY (event_date)
CONSTRAINT sensor_events_pk PRIMARY KEY (sensor_id, event_time);

-- Query with partition pruning + composite key filter
SELECT sensor_id, event_type, payload
FROM sensor_events
WHERE event_date  = '2024-06-01'    -- partition pruning
  AND sensor_id   = 'SENSOR-042';   -- further filter on composite key
```

---

## :material-swap-horizontal: Single Key vs Composite Key

| Aspect | Single PK | Composite PK |
|--------|-----------|-------------|
| Join simplicity | Simple `ON t.id = s.id` | All columns required in `ON` |
| MERGE clause | One `ON` condition | One condition per key column |
| Partition strategy | Partition by PK range | Partition by most selective key column |
| Surrogate key option | Add a surrogate SK instead | Add a surrogate SK to simplify joins |
| Natural fit | Entities with a single identity | Junction tables, time-series, order lines |

---

## :material-lightbulb-outline: When to Use Composite Keys

| Scenario | Recommendation |
|----------|---------------|
| Junction / bridge table (many-to-many) | Composite PK of both FK columns |
| Order lines, invoice items | `(order_id, line_id)` composite PK |
| Time-series readings per entity | `(entity_id, timestamp)` composite PK |
| MERGE on multi-column natural key | Include all key columns in `ON` clause |
| Large composite key table | Consider adding a surrogate key to simplify downstream joins |

!!! tip "Consider a surrogate key for complex joins"
    If a table with a composite key is referenced by many fact tables, adding a single
    surrogate key (e.g., `IDENTITY` or `md5` hash of the composite) simplifies all
    downstream `JOIN ON` clauses to a single column condition.
