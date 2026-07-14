# :material-code-block-braces: Compound Statements and Conditionals

Spark SQL procedural blocks use `BEGIN … END` to group statements and
`IF … END IF` / `CASE … END CASE` for statement-level branching.

---

## :material-pin: BEGIN … END

`BEGIN … END` creates a **compound statement block** — a sequence of SQL statements
executed together. Variables declared inside are scoped to that block.

```sql
BEGIN
  -- SQL statements here
  statement1;
  statement2;
END;
```

### Nested blocks

Blocks can be nested; inner variables shadow outer ones of the same name.

```sql
BEGIN
  DECLARE x INT DEFAULT 10;
  BEGIN
    DECLARE x INT DEFAULT 20;  -- shadows outer x
    SELECT x;                  -- returns 20
  END;
  SELECT x;                    -- returns 10
END;
```

---

## :material-call-split: IF … END IF

```sql
IF condition THEN
    statements;
[ELSEIF condition THEN
    statements;]
...
[ELSE
    statements;]
END IF;
```

### Single branch

```sql
BEGIN
  DECLARE row_count BIGINT;
  SET row_count = (SELECT COUNT(*) FROM staging.orders);

  IF row_count = 0 THEN
    INSERT INTO audit_log VALUES ('no_data', current_timestamp());
  END IF;
END;
```

### IF / ELSEIF / ELSE

```sql
BEGIN
  DECLARE load_mode STRING DEFAULT 'full';

  IF load_mode = 'full' THEN
    TRUNCATE TABLE dim_customer;
    INSERT INTO dim_customer SELECT * FROM staging_customer;

  ELSEIF load_mode = 'incremental' THEN
    MERGE INTO dim_customer AS tgt
    USING staging_customer AS src
    ON tgt.customer_id = src.customer_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;

  ELSE
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Unknown load_mode';
  END IF;
END;
```

### Conditional table creation

```sql
BEGIN
  DECLARE tbl_exists BOOLEAN DEFAULT FALSE;

  SET tbl_exists = (
    SELECT COUNT(*) > 0
    FROM information_schema.tables
    WHERE table_schema = 'analytics'
      AND table_name   = 'daily_summary'
  );

  IF NOT tbl_exists THEN
    CREATE TABLE analytics.daily_summary (
      summary_date DATE,
      total_revenue DECIMAL(18,2),
      order_count   BIGINT
    )
    USING DELTA
    PARTITIONED BY (summary_date);
  END IF;
END;
```

---

## :material-arrow-decision: CASE … END CASE (Statement)

The statement-level `CASE` matches a variable against values — analogous to a `switch`
in procedural languages. It is different from the expression-level `CASE WHEN` inside
`SELECT`.

### Simple CASE statement

```sql
BEGIN
  DECLARE env STRING DEFAULT 'prod';

  CASE env
    WHEN 'dev'  THEN
      SET VARIABLE max_rows = 1000;
    WHEN 'test' THEN
      SET VARIABLE max_rows = 10000;
    WHEN 'prod' THEN
      SET VARIABLE max_rows = 0;   -- no limit
    ELSE
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Unknown environment';
  END CASE;
END;
```

### Searched CASE statement

```sql
BEGIN
  DECLARE score INT DEFAULT 85;
  DECLARE grade STRING;

  CASE
    WHEN score >= 90 THEN SET grade = 'A';
    WHEN score >= 80 THEN SET grade = 'B';
    WHEN score >= 70 THEN SET grade = 'C';
    ELSE                   SET grade = 'F';
  END CASE;

  SELECT grade;
END;
```

---

## :material-compare: IF vs CASE (statement level)

| Feature | `IF … END IF` | `CASE … END CASE` |
|---------|:-------------:|:-----------------:|
| Arbitrary conditions | :material-check: | Searched form only |
| Value matching | :material-check: | :material-check: (simple form) |
| ELSE branch | `ELSE` | `ELSE` |
| ELSEIF | :material-check: | :material-close: (use searched form) |
| Readability (value switch) | Medium | High |

---

## :material-layers: Realistic ETL Pattern

```sql
BEGIN
  DECLARE batch_date DATE DEFAULT current_date();
  DECLARE rows_loaded BIGINT DEFAULT 0;
  DECLARE rows_rejected BIGINT DEFAULT 0;

  -- Step 1: validate staging
  IF (SELECT COUNT(*) FROM staging.orders WHERE order_date IS NULL) > 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'NULL order_date rows found in staging';
  END IF;

  -- Step 2: conditional load strategy
  IF (SELECT COUNT(*) FROM analytics.orders WHERE order_date = batch_date) = 0 THEN
    -- First load for this date: full insert
    INSERT INTO analytics.orders
    SELECT * FROM staging.orders WHERE order_date = batch_date;
  ELSE
    -- Re-run: overwrite partition
    DELETE FROM analytics.orders WHERE order_date = batch_date;
    INSERT INTO analytics.orders
    SELECT * FROM staging.orders WHERE order_date = batch_date;
  END IF;

  SET rows_loaded = (SELECT COUNT(*) FROM analytics.orders WHERE order_date = batch_date);

  INSERT INTO audit_log (batch_date, rows_loaded, completed_at)
  VALUES (batch_date, rows_loaded, current_timestamp());
END;
```

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| Missing `END IF` | Parse error | Every `IF` needs `END IF` |
| Missing `;` after statements | Parse error | Each statement inside a block ends with `;` |
| `CASE` without `ELSE` | `CASE_NOT_FOUND` error if no branch matches | Always add an `ELSE` or handle via `DECLARE HANDLER` |
| Using expression `IF()` as a statement | Not valid — `IF(x, a, b)` is an expression | Use `IF x THEN … END IF` for statements |
| Accessing outer variable after inner redeclaration | Gets inner-scope value | Use distinct variable names |
