# Holidays & Regressors

---

## Holidays

### Custom holiday table

Pass a DataFrame with known event dates. Prophet adds a separate effect for each holiday.

```python
import pandas as pd
from prophet import Prophet

playoffs = pd.DataFrame({
    "holiday":       "playoff",
    "ds":            pd.to_datetime(["2008-01-13", "2009-01-03", "2010-01-16"]),
    "lower_window":  0,    # effect starts on this day (0 = event day)
    "upper_window":  1,    # effect ends 1 day after
})

superbowls = pd.DataFrame({
    "holiday":       "superbowl",
    "ds":            pd.to_datetime(["2010-02-07", "2014-02-02", "2016-02-07"]),
    "lower_window":  0,
    "upper_window":  1,
})

holidays = pd.concat([playoffs, superbowls], ignore_index=True)

m = Prophet(
    holidays=holidays,
    holidays_prior_scale=10.0,   # ↑ = larger holiday effects allowed
)
m.fit(df)
```

### Country-level public holidays

Prophet bundles public holiday calendars for many countries via the `holidays` package:

```python
m = Prophet()
m.add_country_holidays(country_name="US")
m.fit(df)

# See every holiday that was loaded
print(m.train_holiday_names)
```

Supported country codes include `"US"`, `"GB"`, `"DE"`, `"IN"`, `"AU"` and many others.

### Holiday window

The `lower_window` and `upper_window` columns define a **window of effect** in days
relative to the event date:

| lower_window | upper_window | Effect spans |
|---|---|---|
| 0 | 0 | Event day only |
| -1 | 0 | Day before + event day |
| 0 | 1 | Event day + day after |
| -1 | 1 | 3-day window centred on event |

---

## Additional Regressors

External time-series that help explain variance in `y` can be added as regressors.

!!! warning "The regressor column must be present in BOTH the training df AND the future df."

```python
m = Prophet()
m.add_regressor(
    "temperature",
    standardize=True,     # z-score the column before fitting (recommended)
    mode="additive",      # "additive" | "multiplicative"
)
m.fit(df_with_temperature)

future["temperature"] = ...   # must supply future values
forecast = m.predict(future)
```

### Regressor effect in the forecast

After prediction, the regressor contribution appears as its own column:

```python
forecast[["ds", "yhat", "temperature"]]
```

### Multiple regressors

```python
for col in ["temperature", "promo_flag", "competitor_price"]:
    m.add_regressor(col, standardize=True)
```

### Regressors inside a PySpark UDF

When using `applyInPandas`, the regressor columns flow through automatically —
they are part of the group DataFrame:

```python
def forecast_group(group_df: pd.DataFrame) -> pd.DataFrame:
    from prophet import Prophet
    model = Prophet()
    model.add_regressor("promo", standardize=False)
    train = group_df[["ds", "y", "promo"]].copy()
    train["ds"] = pd.to_datetime(train["ds"])
    model.fit(train)

    future        = model.make_future_dataframe(periods=90)
    future["promo"] = 0   # assume no promotion in forecast horizon
    return model.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]] \
                .assign(ds=lambda df: df["ds"].dt.date)
```

---

## Source file

```
src/01_prophet_fundamentals.py   ← sections 3, 4, 6
src/02_pyspark_prophet_distributed.py ← section 9 (regressor UDF)
```
