# :material-repeat: Loops

Spark SQL provides four loop constructs for iterating within procedural blocks.
All loop bodies are compound statement sequences terminated by `;`.

---

## :material-pin: Loop Constructs at a Glance

| Construct | Checks condition | Best for |
|-----------|:----------------:|---------|
| `WHILE … DO … END WHILE` | Before each iteration | Condition-controlled iteration |
| `REPEAT … UNTIL … END REPEAT` | After each iteration (do-while) | Execute at least once |
| `LOOP … END LOOP` | Never (use `LEAVE`) | Explicit break-controlled loops |
| `FOR … DO … END FOR` | Per-row (result set cursor) | Iterating over query results |

---

## :material-refresh: WHILE … DO … END WHILE

Evaluates the condition **before** each iteration. If FALSE on entry, body never runs.

```sql
WHILE condition DO
    statements;
END WHILE;
```

### Example — retry with exponential backoff (simulation)

```sql
BEGIN
  DECLARE attempt INT DEFAULT 0;
  DECLARE max_attempts INT DEFAULT 5;
  DECLARE success BOOLEAN DEFAULT FALSE;

  WHILE attempt < max_attempts AND NOT success DO
    SET attempt = attempt + 1;

    BEGIN
      INSERT INTO target SELECT * FROM staging WHERE batch_id = current_batch();
      SET success = TRUE;
    END;
  END WHILE;

  IF NOT success THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Load failed after max retries';
  END IF;
END;
```

### Example — process batches of 1000 rows

```sql
BEGIN
  DECLARE offset_val INT DEFAULT 0;
  DECLARE batch_size INT DEFAULT 1000;
  DECLARE total     INT DEFAULT (SELECT COUNT(*) FROM staging_data);

  WHILE offset_val < total DO
    INSERT INTO target_table
    SELECT * FROM staging_data
    ORDER BY id
    LIMIT batch_size OFFSET offset_val;

    SET offset_val = offset_val + batch_size;
  END WHILE;
END;
```

---

## :material-repeat-once: REPEAT … UNTIL … END REPEAT

Evaluates the condition **after** each iteration — body always runs at least once.

```sql
REPEAT
    statements;
UNTIL condition
END REPEAT;
```

### Example — poll until data arrives

```sql
BEGIN
  DECLARE found INT DEFAULT 0;
  DECLARE tries INT DEFAULT 0;

  REPEAT
    SET tries = tries + 1;
    SET found = (SELECT COUNT(*) FROM incoming_events WHERE processed = FALSE);
  UNTIL found > 0 OR tries >= 10
  END REPEAT;

  IF found = 0 THEN
    INSERT INTO audit_log VALUES ('no_events_after_10_polls', current_timestamp());
  END IF;
END;
```

---

## :material-infinity: LOOP … END LOOP with LEAVE

An unconditional loop — runs forever until a `LEAVE` statement exits it.
Use a **loop label** with `LEAVE label_name` to break out.

```sql
label: LOOP
    statements;
    IF exit_condition THEN
        LEAVE label;
    END IF;
END LOOP label;
```

### Example — process until empty

```sql
BEGIN
  DECLARE remaining INT;

  process_loop: LOOP
    SET remaining = (SELECT COUNT(*) FROM work_queue WHERE status = 'pending');

    IF remaining = 0 THEN
      LEAVE process_loop;
    END IF;

    UPDATE work_queue
    SET status = 'processing'
    WHERE id = (SELECT id FROM work_queue WHERE status = 'pending' LIMIT 1);

    -- ... do work ...
  END LOOP process_loop;
END;
```

---

## :material-skip-next: ITERATE — Continue to Next Iteration

`ITERATE label` skips the rest of the current loop body and jumps to the next iteration
(analogous to `continue` in Python/Java). Only valid inside labelled `LOOP`, `WHILE`, or `REPEAT`.

```sql
BEGIN
  DECLARE i INT DEFAULT 0;

  count_loop: WHILE i < 10 DO
    SET i = i + 1;

    IF i % 2 = 0 THEN
      ITERATE count_loop;   -- skip even numbers
    END IF;

    INSERT INTO odd_numbers VALUES (i);
  END WHILE count_loop;
END;
```

---

## :material-table-arrow-right: FOR … DO … END FOR (Cursor Loop)

`FOR` iterates over the rows returned by a `SELECT` query.
Each row's columns are accessible as variables within the loop body.

```sql
FOR row_var IN (SELECT col1, col2 FROM table_name WHERE ...)
DO
    -- row_var.col1, row_var.col2 available here
    statements;
END FOR;
```

### Example — process each active region

```sql
BEGIN
  FOR region_row IN (SELECT region_id, region_name FROM regions WHERE is_active = TRUE)
  DO
    INSERT INTO region_summary (region_id, summary_date, order_count)
    SELECT
      region_row.region_id,
      current_date(),
      COUNT(*)
    FROM orders
    WHERE region_id = region_row.region_id
      AND order_date = current_date();
  END FOR;
END;
```

### Example — generate monthly partition tables

```sql
BEGIN
  FOR month_row IN (
    SELECT DISTINCT DATE_TRUNC('month', sale_date) AS month_start
    FROM sales_staging
    ORDER BY 1
  )
  DO
    MERGE INTO sales_partitioned AS tgt
    USING (
      SELECT * FROM sales_staging
      WHERE DATE_TRUNC('month', sale_date) = month_row.month_start
    ) AS src
    ON tgt.sale_id = src.sale_id
    WHEN NOT MATCHED THEN INSERT *;
  END FOR;
END;
```

### Example — send alerts for breached thresholds

```sql
BEGIN
  FOR alert_row IN (
    SELECT metric_name, metric_value, threshold
    FROM metric_thresholds
    WHERE metric_value > threshold
  )
  DO
    INSERT INTO alert_queue (metric, value, threshold_breached, created_at)
    VALUES (
      alert_row.metric_name,
      alert_row.metric_value,
      alert_row.threshold,
      current_timestamp()
    );
  END FOR;
END;
```

---

## :material-compare: Loop Comparison

| Feature | `WHILE` | `REPEAT` | `LOOP` | `FOR` |
|---------|:-------:|:--------:|:------:|:-----:|
| Condition before body | :material-check: | :material-close: | :material-close: | N/A |
| Always runs once | :material-close: | :material-check: | :material-close: | N/A |
| Explicit `LEAVE` to exit | Optional | Optional | Required | :material-close: |
| Iterates over query rows | :material-close: | :material-close: | :material-close: | :material-check: |
| `ITERATE` (continue) | :material-check: | :material-check: | :material-check: | :material-close: |

---

## :material-alert-circle: Common Mistakes

| Mistake | Problem | Fix |
|---------|---------|-----|
| `LOOP` without `LEAVE` | Infinite loop — times out | Add `IF … THEN LEAVE label END IF` |
| Modifying loop source inside `FOR` | Undefined behaviour | Use a staging table as the source |
| Large result set in `FOR` | Row-by-row processing is slow | Prefer set-based SQL; use `FOR` only for small config sets |
| Counter never incremented in `WHILE` | Infinite loop | Ensure loop variable changes each iteration |
| Missing loop label on `LEAVE` | Ambiguous in nested loops | Always label nested loops |

!!! tip "Performance"
    Row-by-row loops in SQL are significantly slower than set-based operations.
    Use `FOR`/`WHILE` for **control flow and config iteration** (small row counts),
    not as a substitute for `INSERT … SELECT` or `MERGE`.
