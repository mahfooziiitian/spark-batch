---
applyTo: "src/**/*.py"
---

# Prophet Patterns

## Data Contract
Prophet always requires exactly two columns in the training DataFrame:
```python
df = pd.DataFrame({"ds": <date_or_datetime>, "y": <numeric>})
```
- `ds`: `datetime64` or `date` string (`YYYY-MM-DD` / `YYYY-MM-DD HH:MM:SS`)
- `y`: numeric (float or int); no NaN except for intentional outlier masking

## Growth Modes
| Mode       | When to use | Extra columns required |
|------------|-------------|------------------------|
| `linear`   | Default; unbounded trend | — |
| `logistic` | S-curve; bounded above (and optionally below) | `cap` (and `floor`) in both train **and** future |
| `flat`     | No trend; seasonality only | — |

For logistic growth, `cap` and `floor` must be present in the future DataFrame:
```python
future["cap"]   = 95.0
future["floor"] = 15.0
```

## Seasonality
- Pass `int` to `yearly_seasonality` / `weekly_seasonality` to set Fourier order directly.
- Add custom periods with `model.add_seasonality(name, period, fourier_order)`.
- Use `condition_name` for seasonalities that apply only in certain periods.
- Rule of thumb: `fourier_order` ≤ `period / 2`; higher values risk overfitting.

## Regressors
```python
model.add_regressor("col_name", standardize=True, mode="additive")
# col_name must exist in both train df AND future df
```

## Holidays
```python
# Custom
model = Prophet(holidays=holidays_df)        # holidays_df: holiday, ds, lower_window, upper_window
# Country
model.add_country_holidays(country_name="US")
```

## Outliers
Never delete outlier rows — set `y = NaN`:
```python
df.loc[df["ds"].isin(outlier_dates), "y"] = np.nan
```
Prophet skips NaN in the likelihood but still predicts those dates.

## Cross-Validation
```python
df_cv   = cross_validation(model, initial="730 days", period="180 days", horizon="365 days", parallel="processes")
df_perf = performance_metrics(df_cv, rolling_window=0.1)
# Metrics: mse, rmse, mae, mape, mdape, smape, coverage
```

## Serialisation
```python
import pickle
with open("model.pkl", "wb") as f: pickle.dump(model, f)
with open("model.pkl", "rb") as f: model = pickle.load(f)
```

## Key Parameters Quick Reference
```
changepoint_prior_scale   default 0.05   ↑ = more flexible trend
seasonality_prior_scale   default 10     ↑ = stronger seasonality
holidays_prior_scale      default 10     ↑ = larger holiday effects
interval_width            default 0.80   credible interval width
seasonality_mode          default additive  | multiplicative
mcmc_samples              default 0      >0 enables full Bayesian sampling (slow)
```
