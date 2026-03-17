"""
Prophet Fundamentals — in-depth examples
=========================================
Covers:
  1. Basic trend + seasonality forecasting
  2. Logistic growth (capacity-bounded)
  3. Holidays & special events
  4. Custom seasonalities
  5. Additional regressors
  6. Changepoint detection & tuning
  7. Cross-validation & performance metrics
  8. Uncertainty intervals
"""

import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import add_changepoints_to_plot, plot_cross_validation_metric
import matplotlib.pyplot as plt

# ── 1. BASIC LINEAR-TREND FORECAST ──────────────────────────────────────────
# Prophet expects a DataFrame with exactly two columns:
#   ds  — datestamp (datetime or date string)
#   y   — the metric to forecast

df = pd.read_csv(
    "https://raw.githubusercontent.com/facebook/prophet/main/examples/"
    "example_wp_log_peyton_manning.csv"
)
print(df.head())
# ds            y
# 2007-12-10    9.590761
# ...

m = Prophet(
    interval_width=0.95,        # 95 % credible interval (default 0.80)
    growth="linear",            # "linear" | "logistic" | "flat"
    changepoint_prior_scale=0.05,  # flexibility of trend changepoints (↑ = more flexible)
    seasonality_prior_scale=10,    # strength of seasonality (↑ = fits more tightly)
    seasonality_mode="additive",   # "additive" | "multiplicative"
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False,
)

m.fit(df)

# make_future_dataframe extends historical dates by `periods` steps
future = m.make_future_dataframe(periods=365, freq="D", include_history=True)

forecast = m.predict(future)
# Key output columns:
#   yhat        — point forecast
#   yhat_lower  — lower bound of interval
#   yhat_upper  — upper bound of interval
#   trend, trend_lower, trend_upper
#   yearly, weekly  (additive components)
print(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail())

fig = m.plot(forecast)
plt.title("Basic linear-trend forecast")
plt.tight_layout()
plt.show()

# Component plot: trend + each seasonality on separate axes
fig2 = m.plot_components(forecast)
plt.tight_layout()
plt.show()

# ── 2. LOGISTIC GROWTH (capacity-bounded) ────────────────────────────────────
# Use when the metric cannot grow beyond a physical / business ceiling.
# You MUST supply a `cap` column (and optionally `floor` for bounded below).

df_log = df.copy()
df_log["cap"] = 8.5   # upper saturation point (in log-page-views space here)
df_log["floor"] = 6.0  # lower bound (optional)

m_log = Prophet(growth="logistic")
m_log.fit(df_log)

future_log = m_log.make_future_dataframe(periods=1826)  # 5-year horizon
future_log["cap"] = 8.5
future_log["floor"] = 6.0

forecast_log = m_log.predict(future_log)
m_log.plot(forecast_log)
plt.title("Logistic (S-curve) growth")
plt.tight_layout()
plt.show()

# ── 3. HOLIDAYS & SPECIAL EVENTS ─────────────────────────────────────────────
# Add a DataFrame of known holidays / events.
# Columns: holiday (str), ds (date), lower_window (int ≤ 0), upper_window (int ≥ 0)
# lower/upper_window define a window of effect around the event date.

playoffs = pd.DataFrame(
    {
        "holiday": "playoff",
        "ds": pd.to_datetime(
            [
                "2008-01-13", "2009-01-03", "2010-01-16",
                "2010-01-24", "2010-02-07", "2011-01-08",
                "2013-01-12", "2014-01-12", "2014-01-19",
                "2014-02-02", "2015-01-11", "2016-01-17",
                "2016-01-24", "2016-02-07",
            ]
        ),
        "lower_window": 0,
        "upper_window": 1,
    }
)

superbowls = pd.DataFrame(
    {
        "holiday": "superbowl",
        "ds": pd.to_datetime(["2010-02-07", "2014-02-02", "2016-02-07"]),
        "lower_window": 0,
        "upper_window": 1,
    }
)

holidays = pd.concat([playoffs, superbowls], ignore_index=True)

m_hol = Prophet(holidays=holidays, holidays_prior_scale=10.0)
# holidays_prior_scale (default 10): higher ⟹ more impact allowed

m_hol.fit(df)
future_hol = m_hol.make_future_dataframe(periods=365)
forecast_hol = m_hol.predict(future_hol)

# The holidays component appears in the components plot
m_hol.plot_components(forecast_hol)
plt.tight_layout()
plt.show()

# ── 4. BUILT-IN COUNTRY HOLIDAYS ─────────────────────────────────────────────
# Prophet can auto-load public holiday calendars for many countries.

m_country = Prophet()
m_country.add_country_holidays(country_name="US")
m_country.fit(df)
forecast_country = m_country.predict(m_country.make_future_dataframe(periods=365))
print(m_country.train_holiday_names)  # lists every holiday loaded

# ── 5. CUSTOM SEASONALITIES ───────────────────────────────────────────────────
# Add any periodic pattern with add_seasonality().
# period     — length of cycle in days
# fourier_order — number of sin/cos terms (higher = more complex shape)
# mode       — "additive" | "multiplicative" (overrides global seasonality_mode)

m_custom = Prophet(weekly_seasonality=False)   # disable default weekly
m_custom.add_seasonality(
    name="monthly",
    period=30.5,
    fourier_order=5,
)
m_custom.add_seasonality(
    name="quarterly",
    period=91.25,
    fourier_order=7,
    mode="multiplicative",
)
# Custom weekly split by whether it's an on-season period
df["on_season"] = df["ds"].apply(lambda x: 1 if pd.Timestamp(x).month > 6 else 0)
df["off_season"] = 1 - df["on_season"]

m_custom.add_seasonality(
    name="weekly_on_season",
    period=7,
    fourier_order=3,
    condition_name="on_season",    # only active when on_season == 1
)
m_custom.add_seasonality(
    name="weekly_off_season",
    period=7,
    fourier_order=3,
    condition_name="off_season",
)
m_custom.fit(df)

future_c = m_custom.make_future_dataframe(periods=365)
future_c["on_season"] = future_c["ds"].apply(
    lambda x: 1 if pd.Timestamp(x).month > 6 else 0
)
future_c["off_season"] = 1 - future_c["on_season"]
forecast_c = m_custom.predict(future_c)

# ── 6. ADDITIONAL REGRESSORS ─────────────────────────────────────────────────
# Include external time-series features that explain variance.
# The regressor column must be present in BOTH the training df AND future df.

np.random.seed(0)
df_reg = df.copy()
df_reg["temperature"] = 20 + 10 * np.sin(
    2 * np.pi * pd.to_datetime(df["ds"]).dt.dayofyear / 365
) + np.random.normal(0, 2, len(df))

m_reg = Prophet()
# standardize=True (default) z-scores the regressor before fitting
m_reg.add_regressor("temperature", standardize=True, mode="additive")
m_reg.fit(df_reg)

future_r = m_reg.make_future_dataframe(periods=365)
# You must supply the regressor values for the future period too
future_r["temperature"] = 20 + 10 * np.sin(
    2 * np.pi * future_r["ds"].dt.dayofyear / 365
)
forecast_r = m_reg.predict(future_r)
print(forecast_r[["ds", "yhat", "temperature"]].tail())

# ── 7. CHANGEPOINT DETECTION & TUNING ────────────────────────────────────────
# Prophet auto-detects trend changepoints in the first 80 % of the data.

m_cp = Prophet(
    n_changepoints=25,            # number of potential changepoints (default 25)
    changepoint_range=0.8,        # fraction of history to look in (default 0.8)
    changepoint_prior_scale=0.5,  # higher ⟹ more changepoints allowed
)
m_cp.fit(df)
forecast_cp = m_cp.predict(m_cp.make_future_dataframe(periods=365))

fig_cp = m_cp.plot(forecast_cp)
add_changepoints_to_plot(fig_cp.gca(), m_cp, forecast_cp)
plt.title("Detected trend changepoints")
plt.tight_layout()
plt.show()

# Inspect the actual changepoint dates and magnitudes
cp_df = pd.DataFrame(
    {
        "changepoint": m_cp.changepoints,
        "delta": m_cp.params["delta"].mean(axis=0),
    }
)
print(cp_df.sort_values("delta", ascending=False).head(10))

# ── 8. CROSS-VALIDATION & PERFORMANCE METRICS ────────────────────────────────
# Simulated historical forecasts: train on expanding windows,
# evaluate on a fixed horizon.

# initial  — minimum training period
# period   — spacing between cutoff dates
# horizon  — forecast horizon to evaluate

df_cv = cross_validation(
    m,
    initial="730 days",   # first training window
    period="180 days",    # new cutoff every 180 days
    horizon="365 days",   # evaluate each forecast 365 days ahead
    parallel="processes", # "processes" | "threads" | None
)
print(df_cv.head())
# ds, yhat, yhat_lower, yhat_upper, y, cutoff

df_perf = performance_metrics(df_cv, rolling_window=0.1)
# Metrics: mse, rmse, mae, mape, mdape, smape, coverage
print(df_perf[["horizon", "rmse", "mae", "mape", "coverage"]].head(10))

# Plot RMSE over forecast horizon
fig_cv = plot_cross_validation_metric(df_cv, metric="rmse")
plt.tight_layout()
plt.show()

# ── 9. UNCERTAINTY INTERVALS (MCMC sampling) ─────────────────────────────────
# Default: MAP estimation (fast, uncertainty only on observation noise).
# For full posterior uncertainty on trend & seasonality, use MCMC.

m_mcmc = Prophet(
    mcmc_samples=300,       # number of MCMC samples (300–1000 typical)
    interval_width=0.95,
)
m_mcmc.fit(df)  # slower — runs Stan HMC
forecast_mcmc = m_mcmc.predict(m_mcmc.make_future_dataframe(periods=365))
m_mcmc.plot_components(forecast_mcmc)
plt.tight_layout()
plt.show()
