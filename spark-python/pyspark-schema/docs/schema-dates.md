# Dates & Timestamps

PySpark provides two distinct types for temporal data, plus a rich set of
functions for parsing, arithmetic, and extraction.

## Types

| Type | Spark DDL | Python equivalent | Precision |
| ---- | --------- | ----------------- | --------- |
| `DateType()` | `DATE` | `datetime.date` | Day |
| `TimestampType()` | `TIMESTAMP` | `datetime.datetime` | Microsecond |

```python
from pyspark.sql.types import DateType, TimestampType

schema = StructType([
    StructField("event_date", DateType(),      nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])
```

## Parsing Strings

Always parse explicitly — never rely on automatic type coercion:

```python
df = (df_raw
      .withColumn("event_date", F.to_date(F.col("event_date"),   "yyyy-MM-dd"))
      .withColumn("created_at", F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss")))
```

Common format patterns:

| Format | Example |
| ------ | ------- |
| `yyyy-MM-dd` | `2024-01-15` |
| `yyyy-MM-dd HH:mm:ss` | `2024-01-15 08:30:00` |
| `dd/MM/yyyy` | `15/01/2024` |
| `yyyyMMdd` | `20240115` |

## Date Functions

| Function | Description |
| -------- | ----------- |
| `F.year(col)` | Extract year |
| `F.month(col)` | Extract month (1–12) |
| `F.dayofweek(col)` | Day of week (1=Sunday) |
| `F.datediff(end, start)` | Days between two dates |
| `F.date_add(col, days)` | Add days |
| `F.current_date()` | Today's date |

## Timestamp Functions

| Function | Description |
| -------- | ----------- |
| `F.hour(col)` | Extract hour |
| `F.minute(col)` | Extract minute |
| `F.unix_timestamp(col)` | Convert to epoch seconds |
| `F.date_trunc("hour", col)` | Truncate to hour boundary |
| `F.current_timestamp()` | Current timestamp |

## Code

```python title="src/schema_dates.py"
--8<-- "src/schema_dates.py"
```

## Run

```bash
SPARK_MASTER=local[*] python src/schema_dates.py
```

## Key Points

- Use `to_date` / `to_timestamp` with an **explicit format string** — implicit parsing is locale-dependent.
- `DateType` has no time component; casting `TimestampType` to `DateType` truncates the time.
- For timezone-aware timestamps, use `TimestampNTZType()` (Spark 3.4+) or set `spark.sql.session.timeZone`.
