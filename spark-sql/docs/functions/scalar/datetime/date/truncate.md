# :material-calendar-remove: Date Truncation

`DATE_TRUNC` rounds a timestamp down to the start of a requested unit such as year, month, week,
hour, or millisecond. It is useful for bucketing event time for aggregation.

______________________________________________________________________

## :material-sitemap: Overview

```mermaid
graph LR
    A[Timestamp input] --> B[DATE_TRUNC(unit, value)]
    B --> C[Boundary-aligned timestamp]
```

### :material-animation-play: Interactive Visualization — Truncation Granularity

<div id="viz-date-trunc" class="ts-viz"></div>

Choose a truncation unit to watch the same timestamp snap to progressively coarser boundaries. This
makes it easier to see what information is retained and what is zeroed out.

## :material-pin: Syntax

```sql
DATE_TRUNC(format, timestamp)
```

Common units include `YEAR`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, and
`MICROSECOND`.

## :material-information-outline: Behavior

1. `DATE_TRUNC` returns the input rounded down to the start of the requested unit.
2. The result type is timestamp-like even if you pass a `DATE`.
3. Larger units zero out more fields.
4. `MILLISECOND` keeps three fractional digits; `MICROSECOND` keeps full microsecond precision.
5. `WEEK` truncation aligns to the start of Spark's week.
6. **Spark truncates weeks to Monday** — that detail matters when your business week starts on Sunday or when you compare Spark output with another BI tool.

______________________________________________________________________

## :material-flask-outline: Practical Examples

### :material-toy-brick: 1. Truncate to Year

```sql
SELECT DATE_TRUNC('YEAR', TIMESTAMP '2015-03-05 09:32:05.359') AS year_start;
-- 2015-01-01 00:00:00
```

### :material-toy-brick: 2. Truncate to Month

```sql
SELECT DATE_TRUNC('MONTH', TIMESTAMP '2015-03-05 09:32:05.359') AS month_start;
-- 2015-03-01 00:00:00
```

### :material-toy-brick: 3. Truncate to Hour

```sql
SELECT DATE_TRUNC('HOUR', TIMESTAMP '2015-03-05 09:32:05.359') AS hour_start;
-- 2015-03-05 09:00:00
```

### :material-toy-brick: 4. Truncate to Millisecond

```sql
SELECT DATE_TRUNC('MILLISECOND', TIMESTAMP '2015-03-05 09:32:05.123456') AS milli_value;
-- 2015-03-05 09:32:05.123
```

### :material-toy-brick: 5. Microsecond Truncation Keeps Full Precision

```sql
SELECT DATE_TRUNC('MICROSECOND', TIMESTAMP '2015-03-05 09:32:05.123456') AS micro_value;
-- 2015-03-05 09:32:05.123456
```

### :material-alert-circle-outline: 6. `WEEK` Starts on Monday

```sql
SELECT DATE_TRUNC('WEEK', TIMESTAMP '2024-07-17 14:35:10') AS week_start;
-- 2024-07-15 00:00:00
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario             | Why `DATE_TRUNC` helps                       |
| -------------------- | -------------------------------------------- |
| Monthly rollups      | Normalize timestamps to month starts         |
| Hourly dashboards    | Bucket event timestamps for grouping         |
| Week-based reporting | Create deterministic week boundaries         |
| Precision checks     | Compare millisecond vs microsecond retention |
