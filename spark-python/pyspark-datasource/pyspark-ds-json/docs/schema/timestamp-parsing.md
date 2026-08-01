# Timestamp and Date Parsing

Handling mixed and inconsistent timestamp formats in JSON data.

## The Problem

Same field contains different formats:

```json
{"event_time": "2026-08-01T10:15:30Z"}
{"event_time": "01-08-2026 10:15:30"}
{"event_time": "1785579330000"}
{"event_time": ""}
{"event_time": "invalid text"}
```

## Known Consistent Format

Use `timestampFormat` option when all records share the same format:

```python
df = (
    spark.read
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")
    .schema("id BIGINT, event_time TIMESTAMP")
    .json(path)
)
```

| Format | Pattern |
|--------|---------|
| ISO 8601 | `yyyy-MM-dd'T'HH:mm:ssX` |
| Custom | `dd-MM-yyyy HH:mm:ss` |
| Slash-separated | `yyyy/MM/dd HH:mm:ss` |
| Date only | `dateFormat` option: `dd/MM/yyyy` |

## Mixed Formats: Read as String, Parse Conditionally

### Step 1: Read as StringType

```python
df = spark.read.schema("id BIGINT, event_time STRING").json(path)
```

### Step 2: Conditional parsing

```python
from pyspark.sql import functions as F

df_parsed = df.withColumn(
    "event_ts",
    F.when(
        F.col("event_time").rlike(r"^\d{13}$"),
        F.from_unixtime(F.col("event_time").cast("bigint") / 1000).cast("timestamp"),
    )
    .when(
        F.col("event_time").rlike(r"^\d{10}$"),
        F.from_unixtime(F.col("event_time").cast("bigint")).cast("timestamp"),
    )
    .when(
        F.col("event_time").rlike(r"^\d{4}-\d{2}-\d{2}T"),
        F.to_timestamp(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"),
    )
    .when(
        F.col("event_time").rlike(r"^\d{2}-\d{2}-\d{4}"),
        F.to_timestamp(F.col("event_time"), "dd-MM-yyyy HH:mm:ss"),
    )
    .otherwise(F.lit(None).cast("timestamp")),
)
```

## Coalesce Try-Parse Pattern

Alternative: try each format, first non-null wins:

```python
df_parsed = df.withColumn(
    "event_ts",
    F.coalesce(
        F.expr("try_to_timestamp(event_time, \"yyyy-MM-dd'T'HH:mm:ssX\")"),
        F.expr("try_to_timestamp(event_time, 'dd-MM-yyyy HH:mm:ss')"),
        F.expr("try_to_timestamp(event_time, 'yyyy/MM/dd HH:mm:ss')"),
        F.when(
            F.col("event_time").rlike(r"^\d{13}$"),
            F.from_unixtime(F.col("event_time").cast("bigint") / 1000).cast("timestamp"),
        ),
    ),
)
```

## Epoch Handling

```python
# Epoch seconds (10 digits)
F.from_unixtime(F.col("ts")).cast("timestamp")

# Epoch milliseconds (13 digits)
F.from_unixtime(F.col("ts") / 1000).cast("timestamp")
```

## Timezone Handling

Spark converts timezone-aware timestamps to the session timezone:

```python
spark.conf.get("spark.sql.session.timeZone")  # e.g., "America/New_York"

# All these → same session TZ:
# "2026-08-01T10:15:30+05:30"
# "2026-08-01T10:15:30-04:00"
# "2026-08-01T10:15:30Z"
```

## Production Pattern: Flag Unparseable

```python
df_flagged = df_parsed.withColumn(
    "parse_status",
    F.when(F.col("event_time").isNull(), "null_input")
    .when(F.col("event_time") == "", "empty_input")
    .when(F.col("event_ts").isNotNull(), "success")
    .otherwise("parse_failed"),
)
```

!!! tip "Monitoring"
    Alert when `parse_failed` count exceeds a threshold — signals
    new timestamp formats appearing in upstream data.

## Full Demo

```python title="examples/06_schema/22_timestamp_parsing.py"
--8<-- "examples/06_schema/22_timestamp_parsing.py"
```

## Run

```bash
python examples/06_schema/22_timestamp_parsing.py
```

## Quick Reference

| Scenario | Approach |
|----------|----------|
| All records same format | `timestampFormat` option + `TIMESTAMP` schema |
| Mixed formats | Read as `STRING`, parse with `when`/`coalesce` |
| Epoch seconds | `from_unixtime(col)` |
| Epoch milliseconds | `from_unixtime(col / 1000)` |
| Date only | `dateFormat` option + `DATE` schema |
| Timezone-aware | Spark normalizes to session TZ automatically |
