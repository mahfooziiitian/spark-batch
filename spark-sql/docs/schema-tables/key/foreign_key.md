# :material-key-link: Foreign Keys

A foreign key establishes a logical relationship between a column in one table and the
primary key of another. In Spark SQL / Delta Lake, foreign key constraints are
**informational** — useful for BI tools and data catalogs but not enforced at write time.

---

## :material-code-tags: Syntax

```sql
-- Declare FK at CREATE TABLE
CREATE TABLE orders (
    order_id    BIGINT    NOT NULL,
    customer_id BIGINT    NOT NULL,
    product_id  BIGINT    NOT NULL,
    order_date  DATE      NOT NULL,
    amount      DECIMAL(18, 2)
)
USING DELTA
CONSTRAINT orders_pk          PRIMARY KEY (order_id),
CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES customers (customer_id),
CONSTRAINT orders_product_fk  FOREIGN KEY (product_id)  REFERENCES products  (product_id);

-- Add FK to an existing table
ALTER TABLE orders
ADD CONSTRAINT orders_customer_fk
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id);

-- Drop a FK constraint
ALTER TABLE orders
DROP CONSTRAINT orders_customer_fk;
```

---

## :material-information-outline: Behavior

1. Foreign key declarations are stored in the metastore and surfaced by Unity Catalog, Databricks data lineage, and BI tools for join suggestion and ERD generation.
2. **No referential integrity is enforced** — orphaned rows (FK values with no matching PK) are allowed by Spark.
3. The optimizer may use FK declarations to eliminate unnecessary joins (join elimination) when the FK column is provably non-null and the join result is not used.
4. Orphaned rows must be detected and rejected by the pipeline (typically a `LEFT JOIN + IS NULL` check before load).
5. FK columns should be declared `NOT NULL` when a null FK value is semantically invalid.

---

## :material-flask-outline: Practical Examples

### Star-schema FK declarations

```sql
CREATE TABLE fact_sales (
    sale_id      BIGINT         NOT NULL,
    customer_id  BIGINT         NOT NULL,
    product_id   BIGINT         NOT NULL,
    store_id     BIGINT         NOT NULL,
    sale_date    DATE           NOT NULL,
    quantity     INT,
    revenue      DECIMAL(18, 2)
)
USING DELTA
CONSTRAINT fact_sales_pk          PRIMARY KEY (sale_id),
CONSTRAINT fact_sales_customer_fk FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
CONSTRAINT fact_sales_product_fk  FOREIGN KEY (product_id)  REFERENCES dim_product  (product_id),
CONSTRAINT fact_sales_store_fk    FOREIGN KEY (store_id)    REFERENCES dim_store    (store_id);
```

### Detect orphaned FK rows before loading

```sql
-- Orders referencing a customer_id that does not exist in customers
SELECT s.order_id, s.customer_id
FROM staging_orders AS s
LEFT JOIN customers AS c ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
-- Any rows here are orphans — reject or fix before loading
```

### Detect orphaned rows in an existing fact table

```sql
SELECT f.sale_id, f.customer_id
FROM fact_sales AS f
LEFT JOIN dim_customer AS c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

### Load only FK-valid rows (filter during insert)

```sql
INSERT INTO fact_sales
SELECT s.*
FROM staging_sales AS s
WHERE EXISTS (SELECT 1 FROM dim_customer c WHERE c.customer_id = s.customer_id)
  AND EXISTS (SELECT 1 FROM dim_product  p WHERE p.product_id  = s.product_id)
  AND EXISTS (SELECT 1 FROM dim_store    st WHERE st.store_id  = s.store_id);
```

### FK join — standard dimension lookup

```sql
SELECT
    f.sale_id,
    f.sale_date,
    f.revenue,
    c.name      AS customer_name,
    c.segment,
    p.name      AS product_name,
    p.category,
    st.region
FROM fact_sales    AS f
JOIN dim_customer  AS c  ON f.customer_id = c.customer_id
JOIN dim_product   AS p  ON f.product_id  = p.product_id
JOIN dim_store     AS st ON f.store_id    = st.store_id
WHERE f.sale_date >= '2024-01-01';
```

### Referential integrity report

```sql
WITH fk_check AS (
    SELECT
        'customer_id' AS fk_column,
        COUNT(*)      AS orphan_count
    FROM fact_sales AS f
    WHERE NOT EXISTS (SELECT 1 FROM dim_customer c WHERE c.customer_id = f.customer_id)

    UNION ALL

    SELECT
        'product_id',
        COUNT(*)
    FROM fact_sales AS f
    WHERE NOT EXISTS (SELECT 1 FROM dim_product p WHERE p.product_id = f.product_id)

    UNION ALL

    SELECT
        'store_id',
        COUNT(*)
    FROM fact_sales AS f
    WHERE NOT EXISTS (SELECT 1 FROM dim_store s WHERE s.store_id = f.store_id)
)
SELECT fk_column, orphan_count
FROM fk_check
WHERE orphan_count > 0;
```

---

## :material-lightbulb-outline: When to Use

| Scenario | Recommendation |
|----------|---------------|
| Document table relationships for BI tools | Declare `FOREIGN KEY` constraints |
| Validate FK integrity before INSERT | `LEFT JOIN ... IS NULL` or `NOT EXISTS` check |
| Filter out orphaned rows during load | `EXISTS` subquery in `INSERT ... SELECT` |
| Full integrity audit across a star schema | Union of per-FK `NOT EXISTS` checks |
| Enforce non-null FK column | `ALTER COLUMN ... SET NOT NULL` |

!!! tip "Unity Catalog lineage"
    When foreign keys are declared in Unity Catalog, the data lineage graph
    automatically shows table-to-table relationships, making ERD and impact analysis
    available in the Databricks UI without any extra configuration.
