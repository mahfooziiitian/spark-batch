# :material-alert-circle-outline: Exception Handling

Databricks SQL procedural blocks support structured exception handling via
`DECLARE … HANDLER`, allowing scripts to catch SQL errors, log them, and either
recover or propagate the failure.

---

## :material-pin: SQLSTATE Codes

Every SQL error has a 5-character **SQLSTATE** code. Handlers match on these codes
or on named condition shortcuts.

| SQLSTATE | Meaning |
|----------|---------|
| `'00000'` | Success |
| `'01000'` | General warning |
| `'02000'` | No data (e.g. cursor exhausted) |
| `'23000'` | Integrity constraint violation |
| `'40001'` | Serialization failure / deadlock |
| `'42000'` | Syntax error or access rule violation |
| `'45000'` | Unhandled user-defined exception |
| `'HY000'` | General error |

---

## :material-pin: DECLARE … HANDLER

```sql
DECLARE {CONTINUE | EXIT} HANDLER FOR
    {SQLSTATE 'code' | condition_name | SQLEXCEPTION | SQLWARNING | NOT FOUND}
    handler_statement;
```

| Keyword | Behaviour after handler runs |
|---------|------------------------------|
| `CONTINUE` | Execution resumes at the statement **after** the one that raised the error |
| `EXIT` | Execution exits the **current `BEGIN … END` block** |

---

## :material-flask-outline: Examples

### EXIT handler — log error and abort block

```sql
BEGIN
  DECLARE error_msg STRING DEFAULT '';

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET error_msg = 'ETL failed: ' || SQLERRM();
    INSERT INTO error_log (message, occurred_at)
    VALUES (error_msg, current_timestamp());
  END;

  -- If this INSERT fails, the EXIT handler fires
  INSERT INTO analytics.orders
  SELECT * FROM staging.orders;

  INSERT INTO run_log VALUES ('success', current_timestamp());
END;
```

### CONTINUE handler — skip bad rows and continue

```sql
BEGIN
  DECLARE skip_count INT DEFAULT 0;

  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    SET skip_count = skip_count + 1;

  FOR row_var IN (SELECT id, payload FROM incoming_events)
  DO
    INSERT INTO processed_events
    SELECT id, parse_json(payload) FROM VALUES (row_var.id, row_var.payload);
  END FOR;

  INSERT INTO run_summary (skipped, completed_at)
  VALUES (skip_count, current_timestamp());
END;
```

### Handle specific SQLSTATE

```sql
BEGIN
  DECLARE EXIT HANDLER FOR SQLSTATE '23000'
  BEGIN
    INSERT INTO dup_log (key_value, detected_at)
    SELECT customer_id, current_timestamp()
    FROM staging_customer;
  END;

  INSERT INTO dim_customer
  SELECT * FROM staging_customer;
END;
```

### NOT FOUND handler — cursor exhausted

```sql
BEGIN
  DECLARE done BOOLEAN DEFAULT FALSE;

  DECLARE CONTINUE HANDLER FOR NOT FOUND
    SET done = TRUE;

  FOR row_var IN (SELECT * FROM work_queue WHERE status = 'pending')
  DO
    IF done THEN LEAVE; END IF;
    -- process row_var
  END FOR;
END;
```

---

## :material-bullhorn: SIGNAL — Raise a User-Defined Exception

`SIGNAL` raises an error with a custom SQLSTATE and message.
Use SQLSTATE `'45000'` for application-level errors (reserved for user-defined use).

```sql
SIGNAL SQLSTATE 'code'
    SET MESSAGE_TEXT = 'description';
```

### Validate input before processing

```sql
BEGIN
  DECLARE batch_date DATE DEFAULT current_date();

  IF batch_date > current_date() THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'batch_date cannot be in the future';
  END IF;

  IF (SELECT COUNT(*) FROM staging WHERE load_date = batch_date) = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('No staging data for ', CAST(batch_date AS STRING));
  END IF;

  INSERT INTO analytics.orders
  SELECT * FROM staging WHERE load_date = batch_date;
END;
```

### Enforce business rules

```sql
BEGIN
  FOR row_var IN (SELECT order_id, amount FROM orders_staging)
  DO
    IF row_var.amount < 0 THEN
      SIGNAL SQLSTATE '45001'
        SET MESSAGE_TEXT = CONCAT('Negative amount for order_id: ', row_var.order_id);
    END IF;

    INSERT INTO orders VALUES (row_var.order_id, row_var.amount);
  END FOR;
END;
```

---

## :material-refresh-circle: RESIGNAL — Re-Raise from a Handler

`RESIGNAL` re-raises the current exception (optionally with a new message) from within
a handler, propagating it to the caller.

```sql
BEGIN
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    INSERT INTO error_log VALUES (SQLERRM(), current_timestamp());
    RESIGNAL;  -- propagate to outer caller
  END;

  INSERT INTO target SELECT * FROM source;
END;
```

---

## :material-layers: Full Pattern — Audit, Handle, and Recover

```sql
BEGIN
  DECLARE rows_loaded  BIGINT  DEFAULT 0;
  DECLARE error_detail STRING  DEFAULT NULL;
  DECLARE load_status  STRING  DEFAULT 'success';

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET error_detail = SQLERRM();
    SET load_status  = 'failed';
    INSERT INTO etl_audit (run_date, status, error_msg, completed_at)
    VALUES (current_date(), load_status, error_detail, current_timestamp());
  END;

  -- Step 1: validate
  IF (SELECT COUNT(*) FROM staging.orders WHERE order_date IS NULL) > 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'NULL order_date in staging';
  END IF;

  -- Step 2: load
  INSERT INTO analytics.orders
  SELECT * FROM staging.orders WHERE order_date = current_date();

  SET rows_loaded = (
    SELECT COUNT(*) FROM analytics.orders WHERE order_date = current_date()
  );

  -- Step 3: audit success
  INSERT INTO etl_audit (run_date, status, rows_loaded, completed_at)
  VALUES (current_date(), load_status, rows_loaded, current_timestamp());
END;
```

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| Using `CONTINUE` when data integrity may be broken | Silently skips failures | Use `EXIT` for critical paths; `CONTINUE` only for known-tolerable errors |
| `SIGNAL` with a non-45xxx SQLSTATE | Overrides standard SQL codes | Use `'45000'`–`'45999'` for user-defined exceptions |
| No handler and an error occurs | Script aborts with no audit trail | Always add an `EXIT HANDLER FOR SQLEXCEPTION` on production scripts |
| Calling `SQLERRM()` outside a handler | Returns NULL | Call `SQLERRM()` only inside `DECLARE … HANDLER BEGIN … END` blocks |
| Forgetting `RESIGNAL` | Error is silently swallowed | Add `RESIGNAL` if the error should propagate to the caller |
