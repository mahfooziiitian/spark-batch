# :material-variable: Variables

Databricks SQL procedural blocks support local variables declared with `DECLARE`
and mutated with `SET`. Variables are strongly typed and scoped to the enclosing `BEGIN … END` block.

---

## :material-pin: DECLARE

```sql
DECLARE variable_name [data_type] [DEFAULT default_value];
```

| Clause | Required | Notes |
|--------|:--------:|-------|
| `variable_name` | Yes | Must be unique within the block |
| `data_type` | No | Inferred from `DEFAULT` if omitted |
| `DEFAULT value` | No | Initial value; NULL if omitted |

### Supported data types

```sql
DECLARE i         INT        DEFAULT 0;
DECLARE total     BIGINT     DEFAULT 0;
DECLARE flag      BOOLEAN    DEFAULT FALSE;
DECLARE ratio     DOUBLE     DEFAULT 0.0;
DECLARE label     STRING     DEFAULT 'unknown';
DECLARE dt        DATE       DEFAULT current_date();
DECLARE ts        TIMESTAMP  DEFAULT current_timestamp();
DECLARE amt       DECIMAL(18,2) DEFAULT 0.00;
```

---

## :material-pencil: SET

```sql
SET variable_name = expression;
SET variable_name = (SELECT scalar_subquery);
```

!!! note
    The subquery form must return exactly **one row, one column** — otherwise a runtime error is raised.

---

## :material-flask-outline: Examples

### Basic counter

```sql
BEGIN
  DECLARE i INT DEFAULT 1;
  DECLARE total INT DEFAULT 10;

  WHILE i <= total DO
    INSERT INTO numbers VALUES (i);
    SET i = i + 1;
  END WHILE;
END;
```

### Assign from a subquery

```sql
BEGIN
  DECLARE latest_date DATE;
  DECLARE row_count   BIGINT DEFAULT 0;

  SET latest_date = (SELECT MAX(order_date) FROM orders);
  SET row_count   = (SELECT COUNT(*) FROM orders WHERE order_date = latest_date);

  INSERT INTO run_log (run_date, rows_found, logged_at)
  VALUES (latest_date, row_count, current_timestamp());
END;
```

### Boolean flag for conditional branching

```sql
BEGIN
  DECLARE data_ready BOOLEAN DEFAULT FALSE;

  SET data_ready = (
    SELECT COUNT(*) > 0
    FROM staging.events
    WHERE load_date = current_date()
  );

  IF data_ready THEN
    INSERT INTO analytics.events
    SELECT * FROM staging.events WHERE load_date = current_date();
  ELSE
    INSERT INTO audit_log VALUES ('no_data_today', current_timestamp());
  END IF;
END;
```

### Multiple variables with computed defaults

```sql
BEGIN
  DECLARE batch_start TIMESTAMP DEFAULT date_trunc('hour', current_timestamp());
  DECLARE batch_end   TIMESTAMP DEFAULT batch_start + INTERVAL 1 HOUR;
  DECLARE batch_label STRING;

  SET batch_label = CONCAT(
    DATE_FORMAT(batch_start, 'yyyy-MM-dd HH'),
    '_to_',
    DATE_FORMAT(batch_end,   'yyyy-MM-dd HH')
  );

  INSERT INTO hourly_batches (label, start_ts, end_ts)
  VALUES (batch_label, batch_start, batch_end);
END;
```

### Accumulator pattern

```sql
BEGIN
  DECLARE total_inserted BIGINT DEFAULT 0;
  DECLARE batch_inserted BIGINT DEFAULT 0;

  FOR region_row IN (SELECT region_id FROM regions WHERE is_active = TRUE)
  DO
    INSERT INTO region_summary
    SELECT region_row.region_id, COUNT(*), SUM(amount)
    FROM orders
    WHERE region_id = region_row.region_id
      AND order_date = current_date();

    SET batch_inserted = (
      SELECT COUNT(*) FROM region_summary
      WHERE region_id  = region_row.region_id
        AND summary_date = current_date()
    );

    SET total_inserted = total_inserted + batch_inserted;
  END FOR;

  INSERT INTO run_summary (total_rows, completed_at)
  VALUES (total_inserted, current_timestamp());
END;
```

---

## :material-eye-circle: Variable Scope

Variables are **block-scoped** — accessible only within the `BEGIN … END` they are declared in,
and any nested blocks, until shadowed by an inner declaration of the same name.

```sql
BEGIN
  DECLARE x STRING DEFAULT 'outer';

  BEGIN
    DECLARE x STRING DEFAULT 'inner'; -- shadows outer x
    SELECT x;   -- 'inner'
  END;

  SELECT x;     -- 'outer' (inner x is gone)
END;
```

!!! warning "No global variables"
    There are no session-level mutable variables in Databricks SQL.
    To pass values between separate SQL cells in a notebook, use a Delta table or a temp view.

---

## :material-compare: DECLARE vs Session Parameters

| | `DECLARE` variable | Spark configuration |
|-|:-----------------:|:-------------------:|
| Scope | Block only | Session-wide |
| Mutable | `SET var = …` | `SET spark.conf = …` |
| SQL-accessible | Direct | `${spark.conf}` substitution |
| Procedural use | :material-check: | :material-close: |

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| `DECLARE` outside a `BEGIN … END` block | Parse error | Wrap in `BEGIN … END` |
| Subquery returning multiple rows to `SET` | Runtime error | Add `LIMIT 1` or use `MAX()`/`MIN()` |
| Using variable before `SET` | Gets `DEFAULT` (or NULL) | Ensure `SET` runs before use |
| Type mismatch in assignment | Runtime cast error | Explicitly `CAST` the expression |
| Referencing variable in WHERE without alias | Ambiguous | Use distinct variable names from column names |
