# Changepoints

A **changepoint** is a moment in time where the trend rate changes abruptly.
Prophet models these as sparse changes in the slope of the trend function.

---

## Automatic changepoint detection

By default, Prophet places **25 candidate changepoints** uniformly in the first
**80 %** of the training data, then uses a sparse prior (`changepoint_prior_scale`)
to shrink most changes to zero.

```python
m = Prophet(
    n_changepoints=25,           # candidate locations
    changepoint_range=0.8,       # fraction of history to search (default 0.8)
    changepoint_prior_scale=0.05 # sparsity — lower = smoother trend
)
m.fit(df)
```

### `changepoint_prior_scale` tuning

| Value | Effect |
|---|---|
| 0.001 – 0.01 | Very rigid trend; may underfit long-run changes |
| **0.05** | **Default — good starting point** |
| 0.1 – 0.5 | Flexible; captures many small bends |
| > 0.5 | Very flexible; risk of overfitting noise as trend |

---

## Visualising detected changepoints

```python
from prophet.plot import add_changepoints_to_plot
import matplotlib.pyplot as plt

fig = m.plot(forecast)
add_changepoints_to_plot(fig.gca(), m, forecast)
plt.show()
```

---

## Inspecting changepoint magnitudes

After fitting, the `delta` array holds the slope change at each candidate location.
Large values indicate significant trend shifts.

```python
import pandas as pd

cp_df = pd.DataFrame({
    "date":  m.changepoints,
    "delta": m.params["delta"].mean(axis=0),   # posterior mean slope change
})
print(cp_df.sort_values("delta", ascending=False).head(10))
```

---

## Manual changepoint injection

If you **know** when a structural break occurred (product launch, regulation change,
pandemic), specify it directly. Prophet will only allow trend changes at those dates.

```python
m = Prophet(
    changepoints=["2020-03-15", "2021-06-01"],   # exact dates
    changepoint_prior_scale=0.5,    # allow large changes at specified points
)
m.fit(df)
```

!!! tip
    Combine manual changepoints with a **high `changepoint_prior_scale`** when the
    break is known to be large (e.g., COVID lockdown). This lets the model absorb
    the full shift without diffusing it across many candidate points.

---

## How changepoints affect the forecast

The trend beyond the last training date is the **continuation of the last detected
slope**. If the most recent changepoint is an upward acceleration, the forecast
will extrapolate that acceleration indefinitely.

To prevent runaway extrapolation:

- Use `growth="logistic"` with a `cap` to bound the forecast.
- Use `growth="flat"` when no future trend is expected.
- Reduce `changepoint_prior_scale` to produce a more conservative final slope.

---

## Source file

```
src/01_prophet_fundamentals.py   ← section 7
src/05_prophet_deep_concepts.py  ← section 3
```
