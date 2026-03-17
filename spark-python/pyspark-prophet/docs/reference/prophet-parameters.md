# Prophet Parameters

Complete parameter reference for `Prophet()`.

---

## Constructor parameters

| Parameter | Default | Type | Description |
|---|---|---|---|
| `growth` | `"linear"` | str | `"linear"` / `"logistic"` / `"flat"` |
| `changepoints` | `None` | list[str] | Explicit changepoint dates. If `None`, auto-detected. |
| `n_changepoints` | `25` | int | Number of candidate changepoints (auto mode only) |
| `changepoint_range` | `0.8` | float | Fraction of history to place candidates in |
| `changepoint_prior_scale` | `0.05` | float | Flexibility of trend; ↑ = more bends |
| `yearly_seasonality` | `"auto"` | bool/int | `True` / `False` / Fourier order |
| `weekly_seasonality` | `"auto"` | bool/int | `True` / `False` / Fourier order |
| `daily_seasonality` | `"auto"` | bool/int | `True` / `False` / Fourier order |
| `seasonality_mode` | `"additive"` | str | `"additive"` / `"multiplicative"` |
| `seasonality_prior_scale` | `10.0` | float | Strength of seasonality; ↑ = fits more tightly |
| `holidays` | `None` | pd.DataFrame | Custom holiday table (`holiday`, `ds`, `lower_window`, `upper_window`) |
| `holidays_prior_scale` | `10.0` | float | Strength of holiday effects |
| `interval_width` | `0.80` | float | Width of credible interval (0–1) |
| `uncertainty_samples` | `1000` | int | Samples for prediction intervals |
| `mcmc_samples` | `0` | int | MCMC samples for full Bayes (0 = MAP estimate) |
| `stan_backend` | `"CMDSTANPY"` | str | Stan backend: `"CMDSTANPY"` / `"PYSTAN"` |

---

## `add_seasonality()` parameters

| Parameter | Required | Description |
|---|---|---|
| `name` | ✓ | Label for the seasonality (appears in forecast columns) |
| `period` | ✓ | Cycle length in days (e.g., `30.5`, `91.25`) |
| `fourier_order` | ✓ | Number of sin/cos terms; ↑ = more complex shape |
| `mode` | — | `"additive"` / `"multiplicative"` (inherits global if not set) |
| `prior_scale` | — | Overrides `seasonality_prior_scale` for this component |
| `condition_name` | — | Column that must equal `1` for this seasonality to apply |

---

## `add_regressor()` parameters

| Parameter | Default | Description |
|---|---|---|
| `name` | — | Column name in the DataFrame |
| `prior_scale` | `10.0` | Prior scale for regressor coefficient |
| `standardize` | `"auto"` | `True` / `False` / `"auto"` |
| `mode` | `"additive"` | `"additive"` / `"multiplicative"` |

---

## `cross_validation()` parameters

| Parameter | Description |
|---|---|
| `initial` | Minimum training window (`"730 days"`) |
| `period` | Spacing between cutoffs (`"180 days"`) |
| `horizon` | Forecast horizon to evaluate (`"365 days"`) |
| `parallel` | `"processes"` / `"threads"` / `None` |
| `disable_tqdm` | Suppress progress bar |

---

## `performance_metrics()` parameters

| Parameter | Default | Description |
|---|---|---|
| `df_cv` | — | Output of `cross_validation()` |
| `metrics` | all | List of metric names to compute |
| `rolling_window` | `0.1` | Fraction of horizon to smooth over (1 = aggregate all) |

---

## Tuning guide

| Goal | Parameter to adjust |
|---|---|
| Smoother trend | ↓ `changepoint_prior_scale` (try 0.001–0.01) |
| More flexible trend | ↑ `changepoint_prior_scale` (try 0.1–0.5) |
| Stronger seasonal fit | ↑ `seasonality_prior_scale` |
| Wider confidence bands | ↑ `interval_width` |
| Full uncertainty quantification | ↑ `mcmc_samples` (300–1000, slow) |
| Complex intra-week pattern | ↑ `weekly_seasonality` Fourier order (5–10) |
| Prevent trend runaway | Switch to `growth="logistic"` + set `cap` |
