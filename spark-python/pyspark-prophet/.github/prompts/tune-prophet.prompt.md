---
mode: ask
description: Tune Prophet hyperparameters for a specific time-series group
---

# Tune Prophet Hyperparameters

I need to tune Prophet for a specific time-series group in this project.

**Group / series description:** [FILL IN — e.g. "weekly electronics sales, 2 years of history"]
**Series characteristics:**
- Trend type: [linear / logistic / flat]
- Seasonality variance: [stable (additive) / scales with level (multiplicative)]
- Known structural breaks: [list dates or "none"]
- External regressors available: [list or "none"]
- Minimum acceptable MAPE: [e.g. < 10 %]

Please generate:
1. A `param_grid` dict covering `changepoint_prior_scale`, `seasonality_prior_scale`, `seasonality_mode`
2. A cross-validation loop using `prophet.diagnostics.cross_validation` with appropriate `initial`, `period`, `horizon`
3. A results DataFrame sorted by RMSE
4. Code to refit the best model on full history
5. A quick-reference table of what each tuned parameter controls

Follow the patterns in `src/04_tuning_and_serialisation.py`.
