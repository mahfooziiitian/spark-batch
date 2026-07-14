# Cross-Validation & Model Evaluation

Prophet uses **simulated historical forecasts** to evaluate accuracy across multiple
forecast horizons without leaking future information.

---

## How it works

```
Training window (expanding)
────────────────────────────────────────────────────────►
│◄── initial ──►│◄── period ──►│◄── horizon ──►│
│  cutoff 1     │  cutoff 2    │  cutoff 3     │
```

1. Fit on the first `initial` days.
2. Forecast `horizon` days ahead; measure error.
3. Advance cutoff by `period` days; repeat.

---

## Running cross-validation

```python
from prophet.diagnostics import cross_validation, performance_metrics

df_cv = cross_validation(
    model,
    initial="730 days",    # minimum training window
    period="180 days",     # spacing between cutoffs
    horizon="365 days",    # forecast horizon to evaluate
    parallel="processes",  # "processes" | "threads" | None
)
```

### Output columns

| Column | Description |
|---|---|
| `ds` | Forecast date |
| `yhat` | Point forecast |
| `yhat_lower` | Lower credible bound |
| `yhat_upper` | Upper credible bound |
| `y` | Actual value |
| `cutoff` | Training cutoff date for this row |

---

## Performance metrics

```python
df_perf = performance_metrics(df_cv, rolling_window=0.1)
print(df_perf[["horizon", "rmse", "mae", "mape", "coverage"]])
```

| Metric | Formula | Notes |
|---|---|---|
| `mse` | Mean squared error | Penalises large errors heavily |
| `rmse` | √MSE | Same units as `y` |
| `mae` | Mean absolute error | Robust to outliers |
| `mape` | Mean abs % error | Scale-free; undefined when `y=0` |
| `mdape` | Median abs % error | More robust than MAPE |
| `smape` | Symmetric MAPE | Handles near-zero `y` better |
| `coverage` | Fraction of actuals inside credible interval | Should ≈ `interval_width` |

---

## Plotting metric vs horizon

```python
from prophet.plot import plot_cross_validation_metric

plot_cross_validation_metric(df_cv, metric="rmse")
```

---

## Hyperparameter grid search

Cross-validate over a parameter grid to find the best configuration:

```python
import itertools

param_grid = {
    "changepoint_prior_scale": [0.001, 0.01, 0.05, 0.1, 0.5],
    "seasonality_prior_scale": [0.01, 0.1, 1.0, 10.0],
    "seasonality_mode":        ["additive", "multiplicative"],
}

results = []
for params in [dict(zip(param_grid, v)) for v in itertools.product(*param_grid.values())]:
    m = Prophet(**params)
    m.fit(df)
    df_cv   = cross_validation(m, initial="730 days", period="180 days",
                               horizon="365 days", parallel="processes")
    df_p    = performance_metrics(df_cv, rolling_window=1)
    results.append({**params, "rmse": df_p["rmse"].values[0]})

best = min(results, key=lambda r: r["rmse"])
print(best)
```

!!! tip "Total combinations = 5 × 4 × 2 = 40"
    With `parallel="processes"` each CV run takes ~10–30 s depending on series length.
    A full grid search can take several minutes. Use a smaller `param_grid` first
    to identify promising regions.

---

## Source file

```
src/01_prophet_fundamentals.py        ← section 8
src/04_tuning_and_serialisation.py    ← sections 1–3
```
