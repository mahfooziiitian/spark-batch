# Deep Concepts

Advanced Prophet topics for production use.

---

## Outlier masking

!!! danger "Never delete outlier rows — set `y = NaN` instead."
    Deleting rows creates date gaps that can confuse the trend model.
    Setting `y = NaN` preserves the grid; Prophet skips NaN rows in the
    likelihood but still predicts those dates.

```python
outlier_dates = pd.to_datetime(["2020-03-15", "2021-01-06"])
df.loc[df["ds"].isin(outlier_dates), "y"] = float("nan")

m = Prophet()
m.fit(df)   # trains normally; NaN rows are ignored in the log-likelihood

forecast = m.predict(m.make_future_dataframe(periods=365))
# Outlier dates are present in forecast with yhat filled in
```

---

## Missing data tolerance

Prophet does **not** require a contiguous daily grid. The `ds` column simply
needs to be sorted. Arbitrary gaps (weekends, holidays, data outages) are handled
automatically.

For very large gaps (weeks/months), fill missing dates with `y = NaN` to
keep the time grid regular and prevent the model from misinterpreting the gap
as a trend change:

```python
full_range = pd.date_range(df["ds"].min(), df["ds"].max(), freq="D")
df = df.set_index("ds").reindex(full_range).rename_axis("ds").reset_index()
# y is NaN for missing dates — Prophet will interpolate over them
```

---

## Posterior predictive samples

By default, `m.predict()` returns the **MAP (maximum a posteriori) estimate** —
the single most probable forecast. To get full distributional forecasts, use
`predictive_samples()`:

```python
m = Prophet(uncertainty_samples=500, interval_width=0.95)
m.fit(df)

future = m.make_future_dataframe(periods=90)
raw = m.predictive_samples(future)

# raw["yhat"]: shape (n_dates, uncertainty_samples)
import numpy as np
q10 = np.percentile(raw["yhat"], 10, axis=1)
q50 = np.percentile(raw["yhat"], 50, axis=1)
q90 = np.percentile(raw["yhat"], 90, axis=1)
```

For full Bayesian posterior over **trend and seasonality** (not just observation
noise), use `mcmc_samples`:

```python
m = Prophet(mcmc_samples=300)   # runs Stan HMC — significantly slower
m.fit(df)
```

---

## Trend decomposition

Each component is a column in the forecast DataFrame:

```python
forecast[["ds", "trend", "yearly", "weekly", "holidays", "yhat"]]
```

**Residuals:**

```python
df_joined = df.merge(forecast[["ds", "yhat"]], on="ds")
df_joined["residual"] = df_joined["y"] - df_joined["yhat"]
```

Large systematic residuals suggest a missing regressor or seasonality.

---

## Limitations & workarounds

| Limitation | Workaround |
|---|---|
| Multi-variate forecasting (joint $y_1, y_2$) | Fit one model per series; ensemble results |
| Event-driven spikes not in seasonality | Add as a regressor (`= 1` on spike dates) or holiday with wide window |
| Horizons > 2× training length | Use logistic cap, or widen `interval_width` |
| Non-Gaussian noise (counts, %) | Use MCMC; post-process samples (e.g., Poisson link) |
| Very high-frequency data (< 1 min) | Aggregate to ≥ 1 min before fitting |
| Lag / autocorrelation-heavy series | Prefer ARIMA / statsmodels for short-memory patterns |
| Interaction effects between regressors | Fit separate models; stack predictions |

---

## Source file

```
src/05_prophet_deep_concepts.py   ← all 11 sections
```
