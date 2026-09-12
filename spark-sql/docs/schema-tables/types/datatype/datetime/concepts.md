# :material-book-open-variant: DateTime Concepts & Business Usage

Working with `DATE` / `TIMESTAMP` correctly is less about SQL syntax and more about
picking the right *semantic* — event time vs. processing time, which timezone a value
is stored/displayed in, and how a business defines "as of" a point in time. This page
is a glossary and a set of business-driven patterns to pair with
[Date](date.md), [Time](time.md), [Timestamp](timestamp.md), and [Formatting](formatting.md).

______________________________________________________________________

## :material-book-alphabet: Terminology

| Term                                 | Meaning                                                                                                                                                             |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Epoch**                            | Reference instant `1970-01-01 00:00:00 UTC`; `UNIX_TIMESTAMP`/`FROM_UNIXTIME` count seconds from here.                                                              |
| **UTC**                              | Coordinated Universal Time — the timezone-independent instant a `TIMESTAMP` is stored as internally.                                                                |
| **Session timezone**                 | `spark.sql.session.timeZone`; the zone `TIMESTAMP` values are *displayed* in, without changing the stored instant.                                                  |
| **TIMESTAMP_NTZ**                    | "No Time Zone" — a wall-clock value with no UTC conversion; two users in different zones see the same literal text.                                                 |
| **Naive vs. aware**                  | *Naive* = no timezone info attached (`TIMESTAMP_NTZ`); *aware* = anchored to UTC and converted on display (`TIMESTAMP`).                                            |
| **ISO 8601**                         | Standard textual format \`YYYY-MM-DDTHH:mm:ss[.ffffff]\[Z                                                                                                           |
| **Event time**                       | The instant something *actually happened* in the source system (e.g. `order_placed_at`).                                                                            |
| **Processing time**                  | The instant a pipeline *ingested or transformed* a record (e.g. `_ingested_at`, `etl_load_ts`). Never assume the two are equal.                                     |
| **Watermark**                        | In streaming, the point up to which event-time data is considered "complete enough" to finalize aggregates; see the Structured Streaming skill for `withWatermark`. |
| **Grain**                            | The finest time unit a fact table is recorded at (day, hour, minute) — determines valid `GROUP BY`/join keys.                                                       |
| **Effective-dated / temporal table** | A table where each row is valid only between `effective_start_date` and `effective_end_date` (or `NULL` = still current).                                           |
| **As-of date**                       | The single date used to answer "what was true on this day" against an effective-dated table.                                                                        |
| **Fiscal period**                    | A business-defined year/quarter/month that may not align with the calendar (e.g. fiscal year starting July 1).                                                      |
| **Business day**                     | A weekday excluding weekends/holidays — distinct from a calendar day (`DATEDIFF` counts calendar days only).                                                        |
| **SLA window / cutoff time**         | A recurring time-of-day boundary (e.g. "orders after 15:00 ship next business day").                                                                                |
| **Idempotency window**               | The time range a re-run/backfill is scoped to, so reprocessing doesn't double-count rows.                                                                           |

!!! warning "TIMESTAMP is not portable across sessions with different timezones"

    Because `TIMESTAMP` is stored as UTC and rendered in `spark.sql.session.timeZone`,
    the *same row* can display different wall-clock values in two sessions configured
    with different zones. Use `TIMESTAMP_NTZ` (or store an explicit UTC offset column)
    for values that must read identically everywhere — e.g. audit logs shared across
    regions.

______________________________________________________________________

## :material-office-building-outline: Business Usage Patterns

### :material-toy-brick: 1. Event Time vs. Processing Time (Late-Arriving Data)

```sql
SELECT
  order_id,
  order_placed_at,          -- event time: when the customer clicked "buy"
  _ingested_at,              -- processing time: when the pipeline landed the row
  DATEDIFF(SECOND, order_placed_at, _ingested_at) AS ingestion_lag_seconds
FROM raw_orders
WHERE DATEDIFF(SECOND, order_placed_at, _ingested_at) > 3600;
-- Rows that arrived more than an hour after the business event occurred.
```

Reporting and reconciliation should key off event time; pipeline health/SLA monitoring
should key off processing time — conflating them under-counts late-arriving data.

### :material-toy-brick: 2. Slowly Changing Dimension (Type 2) — "As Of" Lookups

```sql
-- Effective-dated dimension: one row per version of a customer's address
SELECT d.customer_id, d.address, d.tier
FROM dim_customer_history d
WHERE d.customer_id = 12345
  AND DATE '2024-06-01' >= d.effective_start_date
  AND (d.effective_end_date IS NULL OR DATE '2024-06-01' < d.effective_end_date);
-- "What was this customer's address/tier as of June 1, 2024?"
```

See [SCD Concepts](../../../../patterns/scd/concepts.md) for the full Type 1–6 pattern
catalog this relies on.

### :material-toy-brick: 3. Fiscal Calendar Rollups

```sql
-- Fiscal year starting July 1 -> shift the date 6 months before extracting YEAR
SELECT
  YEAR(DATE_ADD(order_date, -181)) + 1 AS fiscal_year_approx,
  SUM(amount) AS total
FROM orders
GROUP BY 1;
```

!!! note

    Approximate shifts like this drift across leap years — production fiscal reporting
    should join against a maintained `dim_fiscal_calendar` table rather than compute
    fiscal periods inline.

### :material-toy-brick: 4. Business-Day SLA Calculation

```sql
-- Exclude weekends when computing "business days to ship"
SELECT
  order_id,
  order_date,
  ship_date,
  SIZE(FILTER(
    SEQUENCE(order_date, ship_date),
    d -> DAYOFWEEK(d) NOT IN (1, 7)   -- 1=Sunday, 7=Saturday
  )) - 1 AS business_days_to_ship
FROM orders;
```

### :material-toy-brick: 5. Audit / Change-Tracking Columns

```sql
CREATE TABLE customer (
  customer_id BIGINT,
  name        STRING,
  created_at  TIMESTAMP,          -- when the row was first inserted (UTC, immutable)
  updated_at  TIMESTAMP,          -- when the row was last modified
  valid_from  TIMESTAMP_NTZ,      -- business-effective start, in local wall-clock time
  valid_to    TIMESTAMP_NTZ
);
```

`created_at`/`updated_at` are technical metadata (always UTC `TIMESTAMP`);
`valid_from`/`valid_to` are business semantics and often intentionally timezone-naive
because "valid starting Monday 9am" means 9am *local to the business*, not UTC.

### :material-toy-brick: 6. Reporting Cutoffs ("As Of Now" Snapshots)

```sql
-- Freeze a report at a consistent instant rather than re-evaluating CURRENT_TIMESTAMP()
-- once per row (which can drift across a long-running query).
WITH snapshot AS (
  SELECT CURRENT_TIMESTAMP() AS as_of_ts
)
SELECT o.*, s.as_of_ts
FROM orders o, snapshot s
WHERE o.order_date <= s.as_of_ts;
```

______________________________________________________________________

## :material-lightbulb-outline: When to Use

| Scenario                                                | Prefer                                                                     |
| ------------------------------------------------------- | -------------------------------------------------------------------------- |
| Cross-timezone audit logs, event streams                | `TIMESTAMP` (UTC-anchored)                                                 |
| Business hours, local schedules, "as of" business dates | `TIMESTAMP_NTZ` or `DATE`                                                  |
| Interchange with external systems/APIs                  | ISO 8601 strings via `DATE_FORMAT`/`TO_TIMESTAMP`                          |
| Dimensional history / point-in-time joins               | Effective-dated (`effective_start_date`/`effective_end_date`) pattern      |
| SLA/lag monitoring                                      | Separate event-time and processing-time columns, never one column for both |
| Fiscal reporting                                        | A maintained `dim_fiscal_calendar` join, not inline date arithmetic        |
