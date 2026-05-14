# :material-database-edit-outline: CTE in DML Statements

CTEs can prefix any DML statement — `INSERT`, `INSERT OVERWRITE`, `MERGE`, `UPDATE`,
and `DELETE`. They prepare, clean, or enrich source data before it is written, keeping
the DML clause itself simple and readable.

---

## :material-code-tags: Syntax

```sql
-- CTE before INSERT
WITH cte AS (
    SELECT ...
)
INSERT INTO target SELECT * FROM cte;

-- CTE before INSERT OVERWRITE
WITH cte AS (
    SELECT ...
)
INSERT OVERWRITE target PARTITION (date = '2024-06-01')
SELECT * FROM cte;

-- CTE as the USING source in MERGE
WITH cte AS (
    SELECT ...
)
MERGE INTO target AS t
USING cte AS s ON t.id = s.id
WHEN MATCHED     THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- CTE before UPDATE (Delta)
WITH cte AS (
    SELECT id, new_value FROM staging WHERE condition
)
UPDATE target
SET    value = cte.new_value
FROM   cte
WHERE  target.id = cte.id;

-- CTE before DELETE (Delta)
WITH stale AS (
    SELECT id FROM archive WHERE last_seen < '2023-01-01'
)
DELETE FROM target
WHERE id IN (SELECT id FROM stale);
```

---

## :material-information-outline: Behavior

1. The CTE is scoped to the entire DML statement — it is visible in the `USING`, `SET`, `WHERE`, and `VALUES` clauses.
2. For `MERGE`, placing complex deduplication or transformation logic in a CTE keeps the `MERGE` body focused on match/action logic.
3. Delta `UPDATE ... FROM` and `DELETE ... WHERE id IN (SELECT ...)` both accept a leading `WITH` clause.
4. A CTE does **not** change the transactional behaviour of the DML — the atomicity guarantee comes from the storage format (Delta = atomic; Parquet/ORC = not atomic).
5. Multiple CTEs can be chained before a single DML statement.

---

## :material-flask-outline: Practical Examples

### INSERT: clean and load

```sql
WITH cleaned AS (
    SELECT
        CAST(order_id   AS BIGINT)       AS order_id,
        TRIM(customer_name)              AS customer_name,
        CAST(order_date AS DATE)         AS order_date,
        CAST(amount     AS DECIMAL(18,2)) AS amount,
        UPPER(region)                    AS region
    FROM raw_orders
    WHERE order_id   IS NOT NULL
      AND amount      > 0
      AND order_date IS NOT NULL
)
INSERT INTO fact_orders
SELECT order_id, customer_name, order_date, amount, region
FROM cleaned;
```

### INSERT OVERWRITE: idempotent daily partition reload

```sql
WITH todays_orders AS (
    SELECT
        order_id,
        customer_id,
        SUM(amount)  AS daily_total,
        COUNT(*)     AS item_count
    FROM staging_orders
    WHERE order_date = CURRENT_DATE()
    GROUP BY order_id, customer_id
)
INSERT OVERWRITE TABLE daily_summary
PARTITION (order_date = CURRENT_DATE())
SELECT order_id, customer_id, daily_total, item_count
FROM todays_orders;
```

### MERGE: upsert with deduplication

```sql
WITH deduped_source AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
        FROM staging_customers
    )
    WHERE rn = 1
),
enriched AS (
    SELECT
        s.customer_id,
        s.name,
        s.email,
        s.city,
        md5(concat_ws('||', s.name, s.email, s.city)) AS row_hash,
        s.updated_at
    FROM deduped_source AS s
)
MERGE INTO dim_customer AS t
USING enriched AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED AND t.row_hash <> s.row_hash THEN
    UPDATE SET
        name       = s.name,
        email      = s.email,
        city       = s.city,
        row_hash   = s.row_hash,
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, city, row_hash, updated_at)
    VALUES (s.customer_id, s.name, s.email, s.city, s.row_hash, s.updated_at);
```

### MERGE: SCD Type 2 expire step

```sql
WITH changed AS (
    SELECT s.customer_id
    FROM staging_customers AS s
    JOIN dim_customer AS d
        ON  s.customer_id = d.customer_id
        AND d.is_current  = TRUE
    WHERE md5(concat_ws('||', s.name, s.email, s.city))
       <> md5(concat_ws('||', d.name, d.email, d.city))
)
MERGE INTO dim_customer AS t
USING changed AS s
    ON  t.customer_id = s.customer_id
    AND t.is_current  = TRUE
WHEN MATCHED THEN
    UPDATE SET end_date = current_timestamp(), is_current = FALSE;
```

### UPDATE: apply corrections from a staging table

```sql
WITH corrections AS (
    SELECT order_id, corrected_amount AS amount
    FROM order_corrections
    WHERE correction_date = CURRENT_DATE()
)
UPDATE fact_orders AS t
SET    amount = c.amount
FROM   corrections AS c
WHERE  t.order_id = c.order_id;
```

### DELETE: purge expired records

```sql
WITH expired AS (
    SELECT user_id
    FROM user_sessions
    WHERE last_active < DATEADD(DAY, -90, CURRENT_DATE())
)
DELETE FROM user_sessions
WHERE user_id IN (SELECT user_id FROM expired);
```

### Multi-CTE MERGE: transform, validate, then upsert

```sql
WITH
raw AS (
    SELECT *
    FROM staging_products
    WHERE ingested_at = CURRENT_DATE()
),
validated AS (
    SELECT *
    FROM raw
    WHERE product_id IS NOT NULL
      AND price > 0
      AND category IS NOT NULL
),
enriched AS (
    SELECT
        v.product_id,
        v.name,
        v.category,
        v.price,
        md5(concat_ws('||', v.name, v.category, CAST(v.price AS STRING))) AS row_hash
    FROM validated AS v
)
MERGE INTO dim_product AS t
USING enriched AS s
    ON t.product_id = s.product_id
WHEN MATCHED AND t.row_hash <> s.row_hash THEN
    UPDATE SET name = s.name, category = s.category, price = s.price, row_hash = s.row_hash
WHEN NOT MATCHED THEN
    INSERT (product_id, name, category, price, row_hash)
    VALUES (s.product_id, s.name, s.category, s.price, s.row_hash);
```

---

## :material-lightbulb-outline: When to Use CTEs in DML

| Scenario | Pattern |
|----------|---------|
| Clean / cast before loading | CTE + `INSERT INTO` |
| Idempotent daily partition | CTE + `INSERT OVERWRITE PARTITION (...)` |
| Upsert with source deduplication | Dedup CTE + `MERGE` |
| SCD Type 2 expire step | Changed-rows CTE + `MERGE` |
| Apply corrections to a Delta table | Corrections CTE + `UPDATE ... FROM` |
| Purge rows matching a complex condition | Expired-rows CTE + `DELETE ... WHERE IN` |

!!! tip "Keep MERGE logic clean"
    A `MERGE` statement is easiest to audit when the `USING` clause is a simple CTE
    reference rather than an inline subquery. Put all transformations and deduplication
    in one or more CTEs above, then keep the `MERGE` body focused purely on
    match/action logic.
