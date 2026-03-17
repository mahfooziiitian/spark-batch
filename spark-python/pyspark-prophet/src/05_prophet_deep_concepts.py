"""
Prophet — Deep Concepts & Edge Cases
======================================
Covers concepts NOT in 01_prophet_fundamentals.py:

  1.  Flat growth  (no trend at all)
  2.  Saturating bounds — cap AND floor simultaneously
  3.  Manual changepoint injection
  4.  Fourier order impact on seasonality shape
  5.  Sub-daily (hourly) forecasting
  6.  Outlier / anomaly masking before training
  7.  Missing data — Prophet's built-in tolerance
  8.  Multiplicative vs additive side-by-side comparison
  9.  Posterior predictive samples (raw Stan draws)
  10. Trend decomposition & extraction
  11. What Prophet cannot do  (and workarounds)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from prophet import Prophet

np.random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPER
# ─────────────────────────────────────────────────────────────────────────────
def quick_plot(m, forecast, title=""):
    fig = m.plot(forecast)
    plt.title(title)
    plt.tight_layout()
    plt.show()


# ── 1. FLAT GROWTH ─────────────────────────────────────────────────────────
# Use when the series has NO directional trend — only seasonality + noise.
# growth="flat"  prevents Prophet from fitting ANY linear or logistic slope.
# This avoids extrapolating an artificial upward/downward drift in the future.

dates  = pd.date_range("2022-01-01", periods=365, freq="D")
t      = np.arange(365)
# Pure seasonality — no trend
y_flat = (
    100
    + 20 * np.sin(2 * np.pi * t / 365)   # yearly
    +  8 * np.sin(2 * np.pi * t / 7)     # weekly
    + np.random.normal(0, 3, 365)
)

df_flat = pd.DataFrame({"ds": dates, "y": y_flat})

m_flat = Prophet(growth="flat", yearly_seasonality=True, weekly_seasonality=True)
m_flat.fit(df_flat)
fc_flat = m_flat.predict(m_flat.make_future_dataframe(periods=90))

quick_plot(m_flat, fc_flat, "Flat growth — no trend extrapolation")
print("Trend range (should be constant):",
      fc_flat["trend"].min(), "→", fc_flat["trend"].max())


# ── 2. SATURATING BOUNDS — CAP AND FLOOR SIMULTANEOUSLY ──────────────────
# Logistic growth with a lower floor models metrics that are bounded above
# AND below (e.g., capacity utilisation %, bounded inventory levels).
#
# Mathematical note:
#   Prophet uses the generalised logistic function:
#     y(t) = L / (1 + exp(-k(t - m)))
#   where L = cap - floor.  Prophet internally shifts y → y - floor.
#   Both `cap` and `floor` can be time-varying columns.

dates_lg = pd.date_range("2018-01-01", periods=365 * 4, freq="D")
t_lg     = np.arange(len(dates_lg))
# Simulate an S-curve approaching 90 % capacity with a 20 % floor
y_lg = 20 + 70 / (1 + np.exp(-0.005 * (t_lg - 500))) + np.random.normal(0, 1, len(dates_lg))

df_lg = pd.DataFrame({"ds": dates_lg, "y": y_lg})
df_lg["cap"]   = 95.0   # hard ceiling
df_lg["floor"] = 15.0   # hard floor

m_lg = Prophet(growth="logistic")
m_lg.fit(df_lg)

future_lg = m_lg.make_future_dataframe(periods=730)
future_lg["cap"]   = 95.0
future_lg["floor"] = 15.0
fc_lg = m_lg.predict(future_lg)

quick_plot(m_lg, fc_lg, "Logistic growth — bounded above and below")

# Time-varying cap: capacity expansion at a known future date
df_tvcap = df_lg.copy()
df_tvcap["cap"] = df_tvcap["ds"].apply(
    lambda d: 120.0 if d >= pd.Timestamp("2021-01-01") else 95.0
)
df_tvcap["floor"] = 15.0

m_tvcap = Prophet(growth="logistic")
m_tvcap.fit(df_tvcap)
future_tvcap = m_tvcap.make_future_dataframe(periods=365)
future_tvcap["cap"]   = 120.0  # new higher ceiling in forecast horizon
future_tvcap["floor"] = 15.0
fc_tvcap = m_tvcap.predict(future_tvcap)
quick_plot(m_tvcap, fc_tvcap, "Logistic growth — time-varying cap")


# ── 3. MANUAL CHANGEPOINT INJECTION ──────────────────────────────────────
# By default, Prophet places candidate changepoints automatically.
# You can specify exact dates when you KNOW a structural break occurred
# (product launches, policy changes, COVID lockdown, etc.).

df_cp = pd.read_csv(
    "https://raw.githubusercontent.com/facebook/prophet/main/examples/"
    "example_wp_log_peyton_manning.csv"
)

# Supply explicit changepoints — Prophet will only fit changes at these dates
m_manual = Prophet(
    changepoints=[
        "2010-02-07",   # Super Bowl
        "2012-11-04",   # US election
        "2014-02-02",   # Super Bowl XLVIII
    ],
    changepoint_prior_scale=0.5,   # allow large jumps at specified dates
)
m_manual.fit(df_cp)
fc_manual = m_manual.predict(m_manual.make_future_dataframe(periods=365))
quick_plot(m_manual, fc_manual, "Manually specified changepoints")

# Verify the fitted delta (magnitude of trend change at each specified point)
deltas = pd.DataFrame({
    "date":  m_manual.changepoints,
    "delta": m_manual.params["delta"].mean(axis=0),
})
print(deltas.to_string(index=False))


# ── 4. FOURIER ORDER — IMPACT ON SEASONALITY SHAPE ───────────────────────
# Each seasonality is modelled as a Fourier series:
#   s(t) = Σ_{n=1}^{N} [a_n·cos(2πnt/P) + b_n·sin(2πnt/P)]
#
# fourier_order N controls expressiveness:
#   Low N  → smooth, simple shape
#   High N → can capture sharp spikes and complex patterns
#            (but risks overfitting on short series)

fig, axes = plt.subplots(1, 4, figsize=(16, 4), sharey=True)
for ax, fo in zip(axes, [1, 3, 10, 20]):
    m_fo = Prophet(
        yearly_seasonality=fo,   # pass int to set fourier order directly
        weekly_seasonality=False,
        daily_seasonality=False,
    )
    m_fo.fit(df_cp)
    fc_fo = m_fo.predict(m_fo.make_future_dataframe(periods=0, include_history=True))
    yearly_component = fc_fo[["ds", "yearly"]].set_index("ds")
    ax.plot(yearly_component.index, yearly_component["yearly"])
    ax.set_title(f"Fourier order = {fo}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Yearly component")
plt.suptitle("Effect of Fourier order on yearly seasonality", fontsize=12)
plt.tight_layout()
plt.show()


# ── 5. SUB-DAILY (HOURLY) FORECASTING ────────────────────────────────────
# Prophet supports any frequency — hourly, 15-minute, etc.
# Set daily_seasonality=True (or a Fourier order) to capture intra-day patterns.
# Key point: use freq='H' in make_future_dataframe, NOT 'D'.

np.random.seed(0)
hours = pd.date_range("2023-01-01", periods=24 * 180, freq="h")
h     = np.arange(len(hours))

# Combine daily, weekly, and slow yearly seasonality
hourly_sales = (
    200
    + 50  * np.sin(2 * np.pi * h / (24 * 365))   # yearly
    + 30  * np.sin(2 * np.pi * h / (24 * 7))     # weekly
    + 40  * np.sin(2 * np.pi * h / 24 + np.pi)   # daily (peak in evening)
    + np.random.normal(0, 10, len(hours))
)

df_hourly = pd.DataFrame({"ds": hours, "y": hourly_sales.clip(0)})

m_hourly = Prophet(
    daily_seasonality=8,    # Fourier order for intra-day pattern
    weekly_seasonality=3,
    yearly_seasonality=5,
    seasonality_mode="additive",
    changepoint_prior_scale=0.01,  # slow-moving trend
)
m_hourly.fit(df_hourly)

# freq="h" for hourly future dataframe
future_hourly = m_hourly.make_future_dataframe(periods=7 * 24, freq="h")
fc_hourly = m_hourly.predict(future_hourly)

print("Hourly forecast (next 12 hours):")
print(fc_hourly[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(12).to_string(index=False))

fig = m_hourly.plot_components(fc_hourly)
plt.suptitle("Hourly forecast components", fontsize=11)
plt.tight_layout()
plt.show()


# ── 6. OUTLIER HANDLING — NaN MASKING ────────────────────────────────────
# Prophet is NOT robust to extreme outliers if they coincide with known events.
# The cleanest approach: set y = NaN on known outlier dates.
# Prophet interpolates over NaN rows during fitting and still forecasts them.
#
# DO NOT simply delete outlier rows — that creates gaps Prophet can misread.

df_out = df_cp.copy()
df_out["ds"] = pd.to_datetime(df_out["ds"])

# Simulate a recording error on two specific dates
outlier_dates = pd.to_datetime(["2010-02-07", "2014-02-02"])
df_out.loc[df_out["ds"].isin(outlier_dates), "y"] = np.nan  # mask, not delete

m_out = Prophet(interval_width=0.95)
m_out.fit(df_out)   # Prophet skips NaN rows in the likelihood but predicts them

fc_out = m_out.predict(m_out.make_future_dataframe(periods=365))

# Verify: the outlier dates are still in the forecast output with yhat filled in
print("Forecast on previously NaN dates:")
print(fc_out[fc_out["ds"].isin(outlier_dates)][["ds", "yhat", "yhat_lower", "yhat_upper"]])


# ── 7. MISSING DATA TOLERANCE ────────────────────────────────────────────
# Prophet handles irregular / sparse time series automatically — it does NOT
# require a contiguous daily grid.  The ds column just needs to be sorted.
#
# However, if gaps are large (weeks/months), the model may extrapolate poorly.
# Fill large gaps with NaN rows to keep the time grid regular.

df_sparse = df_cp.copy()
df_sparse["ds"] = pd.to_datetime(df_sparse["ds"])

# Drop random 30 % of rows to simulate irregular collection
mask = np.random.rand(len(df_sparse)) > 0.30
df_sparse = df_sparse[mask].reset_index(drop=True)
print(f"Sparse dataset: {len(df_sparse)} rows ({mask.sum()} original)")

m_sparse = Prophet()
m_sparse.fit(df_sparse)
fc_sparse = m_sparse.predict(m_sparse.make_future_dataframe(periods=365))
print("Forecast summary (sparse input):")
print(fc_sparse[["ds", "yhat"]].describe())


# ── 8. ADDITIVE vs MULTIPLICATIVE SEASONALITY SIDE-BY-SIDE ───────────────
# Additive:       y(t) = trend(t) + seasonality(t) + noise
#   → seasonal swings are CONSTANT in absolute terms as trend grows
#   → use when amplitude does NOT scale with the level
#
# Multiplicative: y(t) = trend(t) * seasonality(t) * noise
#   → seasonal swings SCALE with trend level (percentage variation)
#   → use for revenue, page-views, and most business metrics

fig, axes = plt.subplots(1, 2, figsize=(14, 4))
for ax, mode in zip(axes, ["additive", "multiplicative"]):
    m_mode = Prophet(seasonality_mode=mode, yearly_seasonality=True)
    m_mode.fit(df_cp)
    fc_mode = m_mode.predict(m_mode.make_future_dataframe(periods=365))
    ax.plot(
        pd.to_datetime(fc_mode["ds"]),
        fc_mode["yhat"],
        label="yhat",
    )
    ax.fill_between(
        pd.to_datetime(fc_mode["ds"]),
        fc_mode["yhat_lower"],
        fc_mode["yhat_upper"],
        alpha=0.3,
        label="95 % CI",
    )
    ax.set_title(f"seasonality_mode='{mode}'")
    ax.set_xlabel("Date")
    ax.legend()
plt.tight_layout()
plt.show()


# ── 9. POSTERIOR PREDICTIVE SAMPLES ──────────────────────────────────────
# m.predict() returns the posterior mean (MAP).
# For full distributional forecasts, access the raw Stan samples.
# This requires mcmc_samples > 0  OR  uncertainty_samples > 0 (default 1000).

m_samples = Prophet(uncertainty_samples=500, interval_width=0.95)
m_samples.fit(df_cp)
fc_samples = m_samples.predict(m_samples.make_future_dataframe(periods=90))

# Prophet stores raw samples in m.params  (when using MCMC)
# With MAP estimation, predictive samples are draws from the observation noise model.
# Access the raw yhat samples via the internal predictive_samples() method:
raw = m_samples.predictive_samples(m_samples.make_future_dataframe(periods=90))
# raw["yhat"] shape: (n_future_rows, uncertainty_samples)
yhat_samples = raw["yhat"]
print("Sample array shape:", yhat_samples.shape)

# Compute empirical quantiles from samples
q10 = np.percentile(yhat_samples, 10, axis=1)
q50 = np.percentile(yhat_samples, 50, axis=1)
q90 = np.percentile(yhat_samples, 90, axis=1)

future_dates = m_samples.make_future_dataframe(periods=90)["ds"]
plt.figure(figsize=(12, 4))
plt.plot(future_dates, q50, label="Median")
plt.fill_between(future_dates, q10, q90, alpha=0.3, label="10–90 %ile")
plt.title("Forecast from raw posterior samples")
plt.legend()
plt.tight_layout()
plt.show()


# ── 10. TREND DECOMPOSITION ──────────────────────────────────────────────
# After fitting, access each component directly from the forecast DataFrame:
#
#   forecast["trend"]          — the trend component alone
#   forecast["yearly"]         — yearly Fourier seasonality
#   forecast["weekly"]         — weekly Fourier seasonality
#   forecast["holidays"]       — holiday effects (if configured)
#   forecast["<regressor>"]    — each additional regressor effect

m_decomp = Prophet(yearly_seasonality=True, weekly_seasonality=True)
m_decomp.fit(df_cp)
fc_decomp = m_decomp.predict(m_decomp.make_future_dataframe(periods=365))

components = ["trend", "yearly", "weekly"]
fig, axes = plt.subplots(len(components), 1, figsize=(12, 8), sharex=True)
for ax, comp in zip(axes, components):
    ax.plot(pd.to_datetime(fc_decomp["ds"]), fc_decomp[comp])
    ax.set_ylabel(comp)
    ax.grid(True, alpha=0.3)
plt.suptitle("Trend decomposition — individual components", fontsize=12)
plt.tight_layout()
plt.show()

# Residuals: actual − (trend + seasonality)
df_joined = df_cp.copy()
df_joined["ds"] = pd.to_datetime(df_joined["ds"])
df_joined = df_joined.merge(
    fc_decomp[["ds", "trend", "yearly", "weekly", "yhat"]],
    on="ds",
    how="inner",
)
df_joined["residual"] = df_joined["y"] - df_joined["yhat"]
print("Residual stats:")
print(df_joined["residual"].describe())

plt.figure(figsize=(12, 3))
plt.plot(df_joined["ds"], df_joined["residual"], linewidth=0.7)
plt.axhline(0, color="red", linestyle="--")
plt.title("Residuals (actual − yhat)")
plt.tight_layout()
plt.show()


# ── 11. WHAT PROPHET CANNOT DO — AND WORKAROUNDS ────────────────────────
"""
Limitation                          Workaround
──────────────────────────────────  ──────────────────────────────────────────
Multi-variate forecasting           Add external predictors as regressors;
  (joint forecast of several y)       but each y still gets its own model.

Irregular / event-driven spikes     Add a regressor column = 1 on spike days;
  not captured by seasonality         or use holidays with wide windows.

Very long horizons (> 2 × history)  Prophet extrapolates the last trend;
  produce unreliable uncertainty      widen interval_width or use logistic cap.

Non-Gaussian observation noise      Post-process samples; consider MCMC to get
  (count data, binary, %)             a full posterior, then transform yhat.

Extremely high-frequency data       Aggregate first (e.g., 1-min → 15-min);
  (< 1 min)                          Prophet's Fourier approach is slow O(N·F).

Automatic lag selection /           Use classical ARIMA / statsmodels for
  ARIMA-style dependencies            autocorrelation-heavy series.

Multivariate interaction effects    Fit separate models and combine predictions
  between regressors                  (e.g., ensemble / stacking).
"""
print("See docstring above for Prophet limitations and workarounds.")
