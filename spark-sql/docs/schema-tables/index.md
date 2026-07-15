# :material-table-multiple: Schema & Table

The foundation of Spark SQL — defining structure, enforcing types, and organising
data into tables, columns, keys, and views.

---

## :material-compass-outline: Topics

| Topic | What You'll Find |
|-------|-------------------|
| :material-table: [Tables](table/index.md) | Managed & external tables, metadata, partitioning |
| :material-table-column: [Columns](column/index.md) | Selection, aliases, casting, derived, nested, defaults |
| :material-format-text: [Data Types](types/index.md) | Primitives, datetime, complex types, VARIANT |
| :material-key: [Keys & Constraints](key/index.md) | Primary, foreign, composite, surrogate, natural keys |
| :material-eye: [Views](view/index.md) | Temporary, global, permanent views and use cases |
| :material-table-edit: [DML](dml/index.md) | INSERT, UPDATE, DELETE, MERGE, COPY INTO |

---

## :material-pin: Quick Reference

```sql
-- Create a managed table
CREATE TABLE sales (
    id         BIGINT,
    product    STRING,
    amount     DECIMAL(10, 2),
    sold_at    TIMESTAMP,
    region     STRING
)
USING DELTA
PARTITIONED BY (region);

-- Add a column with default
ALTER TABLE sales ADD COLUMN discount DECIMAL(5, 2) DEFAULT 0.0;

-- Create a view
CREATE OR REPLACE VIEW monthly_sales AS
SELECT region, DATE_TRUNC('month', sold_at) AS month, SUM(amount) AS total
FROM sales
GROUP BY ALL;
```

---

## :material-lightbulb-outline: Best Practices

- Prefer **Delta** format for full DML support (UPDATE, DELETE, MERGE).
- Use **partitioning** on high-cardinality filter columns (date, region).
- Define **NOT NULL** constraints on critical columns for data quality.
- Use **VARIANT** type for semi-structured data instead of raw JSON strings.
- Leverage **lateral column alias** (Spark 4.0) to simplify complex expressions.
