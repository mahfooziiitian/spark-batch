# Data Model

Schemas for every layer of the Medallion pipeline.

---

## Bronze — raw ingest

Stored exactly as received. All columns are strings.

| Column | Type | Notes |
|---|---|---|
| `store` | StringType | Group key (may be null — quarantined downstream) |
| `sale_date` | StringType | Raw date string (may be invalid) |
| `revenue` | StringType | Raw numeric string (may be negative or unparseable) |
| `batch_flag` | StringType | Source batch identifier |
| `ingested_at` | TimestampType | Ingest timestamp — used for deduplication ordering |
| `ingest_date` | StringType | **Partition key** — date the row was written to Bronze |

---

## Silver — clean daily series

One row per `(store, ds)` pair. Fully typed, deduplicated, gap-filled.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `store` | StringType | No | Group key |
| `ds` | DateType | No | Calendar date |
| `y` | DoubleType | No | Cleaned daily metric (forward-filled if imputed) |
| `is_imputed` | BooleanType | No | `true` if `y` was gap-filled |
| `day_of_week` | IntegerType | No | 1=Sun … 7=Sat |
| `is_weekend` | DoubleType | No | 1.0 if Sat/Sun, else 0.0 (regressor-ready) |
| `month` | IntegerType | No | Calendar month (1–12) |
| `is_month_end` | DoubleType | No | 1.0 on last day of month |

**Partition key:** `store`

---

## Quarantine — rejected rows

| Column | Type | Notes |
|---|---|---|
| `store` | StringType | May be null |
| `sale_date` | StringType | Original string value |
| `revenue` | StringType | Original string value |
| `ingested_at` | TimestampType | When the row arrived |
| `rejection_reason` | StringType | `null_store` / `invalid_date` / `invalid_revenue` / `unknown` |
| `quarantine_date` | StringType | **Partition key** |

---

## Gold — forecasts

One row per `(store, ds)` covering both historical fit and future forecast.

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `store` | StringType | No | Group key |
| `ds` | DateType | No | Date |
| `yhat` | DoubleType | Yes | Point forecast |
| `yhat_lower` | DoubleType | Yes | Lower credible bound |
| `yhat_upper` | DoubleType | Yes | Upper credible bound |
| `trend` | DoubleType | Yes | Trend component only |
| `split` | StringType | No | `"historical"` or `"forecast"` |
| `run_date` | StringType | No | Pipeline run date — **partition key** (with `store`) |

---

## Accuracy KPIs

One row per group per run.

| Column | Type | Description |
|---|---|---|
| `store` | StringType | Group key |
| `rmse` | DoubleType | Root mean squared error (historical fit) |
| `mae` | DoubleType | Mean absolute error |
| `mape_pct` | DoubleType | Mean absolute percentage error × 100 |
| `mdape_pct` | DoubleType | Median absolute percentage error × 100 |
| `forecast_date` | StringType | Pipeline run date |

---

## Prophet input/output contract

### Input

```
ds         DateType    NOT NULL
y          DoubleType  nullable (NaN = masked outlier / gap)
cap        DoubleType  required when growth="logistic"
floor      DoubleType  optional when growth="logistic"
<regressor> DoubleType  one column per add_regressor() call
```

### Output (from `predict()`)

```
ds             DateType
yhat           DoubleType   point forecast
yhat_lower     DoubleType   lower credible bound
yhat_upper     DoubleType   upper credible bound
trend          DoubleType
trend_lower    DoubleType
trend_upper    DoubleType
yearly         DoubleType   (when yearly_seasonality=True)
weekly         DoubleType   (when weekly_seasonality=True)
holidays       DoubleType   (when holidays configured)
<regressor>    DoubleType   (one per add_regressor())
```
