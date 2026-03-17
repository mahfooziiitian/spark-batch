"""
Prophet Hyperparameter Tuning & Model Selection
================================================
Covers:
  1. Cross-validation over a grid of Prophet hyperparameters
  2. Selecting the best model by RMSE / MAPE
  3. Visualising metric vs horizon for the winning config
  4. Serialising (pickle) a trained Prophet model for reuse
  5. Loading a serialised model and predicting on new data
"""

import itertools
import pickle
import os
import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import matplotlib.pyplot as plt

# ── DATA ─────────────────────────────────────────────────────────────────────
df = pd.read_csv(
    "https://raw.githubusercontent.com/facebook/prophet/main/examples/"
    "example_wp_log_peyton_manning.csv"
)
df["ds"] = pd.to_datetime(df["ds"])

# ── 1. HYPERPARAMETER GRID ────────────────────────────────────────────────────
# The two most impactful parameters to tune:
#   changepoint_prior_scale  — trend flexibility  (common range: 0.001 – 0.5)
#   seasonality_prior_scale  — seasonality strength (common range: 0.01 – 10)
#   seasonality_mode         — "additive" vs "multiplicative"

param_grid = {
    "changepoint_prior_scale": [0.001, 0.01, 0.05, 0.1, 0.5],
    "seasonality_prior_scale": [0.01,  0.1,  1.0,  10.0],
    "seasonality_mode":        ["additive", "multiplicative"],
}

all_params = [
    dict(zip(param_grid.keys(), combo))
    for combo in itertools.product(*param_grid.values())
]
print(f"Total parameter combinations: {len(all_params)}")

# ── 2. CROSS-VALIDATION LOOP ──────────────────────────────────────────────────
results = []

for params in all_params:
    m = Prophet(**params)
    m.fit(df)

    df_cv = cross_validation(
        m,
        initial="730 days",
        period="180 days",
        horizon="365 days",
        parallel="processes",
        disable_tqdm=True,
    )
    df_p = performance_metrics(df_cv, rolling_window=1)  # aggregate over full horizon

    results.append(
        {
            **params,
            "rmse": df_p["rmse"].values[0],
            "mae":  df_p["mae"].values[0],
            "mape": df_p["mape"].values[0],
        }
    )

results_df = pd.DataFrame(results).sort_values("rmse")
print(results_df.head(10).to_string(index=False))

# ── 3. BEST CONFIGURATION ─────────────────────────────────────────────────────
best = results_df.iloc[0].to_dict()
best_params = {
    k: best[k]
    for k in ["changepoint_prior_scale", "seasonality_prior_scale", "seasonality_mode"]
}
print(f"\nBest params: {best_params}")
print(f"Best RMSE:   {best['rmse']:.4f}")

# Refit on the full dataset with best params and plot metric vs horizon
m_best = Prophet(**best_params)
m_best.fit(df)

df_cv_best = cross_validation(
    m_best,
    initial="730 days",
    period="180 days",
    horizon="365 days",
    parallel="processes",
)
df_p_best = performance_metrics(df_cv_best)

# RMSE over forecast horizon
fig, axes = plt.subplots(1, 3, figsize=(14, 4))
for ax, metric in zip(axes, ["rmse", "mae", "mape"]):
    ax.plot(df_p_best["horizon"].dt.days, df_p_best[metric])
    ax.set_xlabel("Forecast horizon (days)")
    ax.set_ylabel(metric.upper())
    ax.set_title(f"{metric.upper()} vs Horizon")
    ax.grid(True, alpha=0.3)
plt.suptitle("Best-model cross-validation metrics", fontsize=13)
plt.tight_layout()
plt.show()

# ── 4. SERIALISE MODEL ────────────────────────────────────────────────────────
MODEL_PATH = "/tmp/prophet_best_model.pkl"

with open(MODEL_PATH, "wb") as f:
    pickle.dump(m_best, f)

print(f"Model saved → {MODEL_PATH}  ({os.path.getsize(MODEL_PATH):,} bytes)")

# ── 5. LOAD & PREDICT ─────────────────────────────────────────────────────────
with open(MODEL_PATH, "rb") as f:
    m_loaded = pickle.load(f)

future = m_loaded.make_future_dataframe(periods=180)
forecast = m_loaded.predict(future)

print(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(10).to_string(index=False))

m_loaded.plot(forecast)
plt.title("Forecast from reloaded model")
plt.tight_layout()
plt.show()

# ── 6. QUICK REFERENCE: KEY PROPHET PARAMETERS ────────────────────────────────
PARAM_REFERENCE = """
Parameter                    | Default | Effect
-----------------------------|---------|----------------------------------------------
growth                       | linear  | linear / logistic / flat
changepoint_prior_scale      | 0.05    | ↑ = more flexible trend (risk: overfitting)
changepoint_range            | 0.8     | fraction of history to place changepoints
n_changepoints               | 25      | candidate changepoint count
seasonality_prior_scale      | 10      | ↑ = stronger seasonality fit
holidays_prior_scale         | 10      | ↑ = larger holiday effects
seasonality_mode             | additive| additive / multiplicative
interval_width               | 0.80    | credible interval width
mcmc_samples                 | 0       | 0 = MAP estimate; >0 = full Bayesian (slow)
yearly_seasonality           | auto    | True / False / int (fourier order)
weekly_seasonality           | auto    | True / False / int (fourier order)
daily_seasonality            | auto    | True / False / int (fourier order)
"""
print(PARAM_REFERENCE)
