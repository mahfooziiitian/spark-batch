# Seasonality

Prophet models periodic patterns as **Fourier series**:

$$
s(t) = \sum_{n=1}^{N} \left[ a_n \cos\!\left(\frac{2\pi n t}{P}\right) + b_n \sin\!\left(\frac{2\pi n t}{P}\right) \right]
$$

- $P$ — period in days
- $N$ — Fourier order (number of sin/cos terms)
- Higher $N$ → more expressive curve; risk of overfitting on short series

---

## Built-in seasonalities

| Seasonality | Period | Default Fourier order | Auto-enabled when |
|---|---|---|---|
| Yearly | 365.25 days | 10 | ≥ 2 years of data |
| Weekly | 7 days | 3 | ≥ 2 weeks of data |
| Daily | 1 day | 4 | Sub-daily data present |

Control them explicitly:

```python
m = Prophet(
    yearly_seasonality=True,   # True / False / int (sets Fourier order)
    weekly_seasonality=10,     # pass int to override default order
    daily_seasonality=False,
)
```

---

## Additive vs multiplicative mode

=== "Additive"
    Seasonal swings are **constant** in absolute terms regardless of trend level.

    $$y(t) = \text{trend}(t) + \text{seasonality}(t) + \varepsilon$$

    ```python
    m = Prophet(seasonality_mode="additive")   # default
    ```

    Use for: temperature, counts with stable variance.

=== "Multiplicative"
    Seasonal swings **scale proportionally** with the trend level.

    $$y(t) = \text{trend}(t) \times \text{seasonality}(t) \times \varepsilon$$

    ```python
    m = Prophet(seasonality_mode="multiplicative")
    ```

    Use for: revenue, page-views, most business metrics.

---

## Fourier order — effect on shape

A higher Fourier order allows the seasonality curve to capture sharper,
more complex patterns — but risks overfitting on short series.

| Fourier order | Shape | Risk |
|---|---|---|
| 1–3 | Smooth sinusoid | Under-fitting sharp peaks |
| 5–10 | Moderate complexity | Balanced |
| 15–20 | Very detailed | Overfitting with < 2 years |

```python
# Side-by-side comparison
for order in [1, 3, 10, 20]:
    m = Prophet(yearly_seasonality=order, weekly_seasonality=False)
    m.fit(df)
```

---

## Custom seasonalities

Add any periodic pattern not covered by the built-ins:

```python
m = Prophet(weekly_seasonality=False)   # disable default first if replacing

m.add_seasonality(
    name="monthly",
    period=30.5,
    fourier_order=5,
    mode="additive",          # can differ from global seasonality_mode
)

m.add_seasonality(
    name="quarterly",
    period=91.25,
    fourier_order=7,
    mode="multiplicative",
)
```

---

## Conditional seasonalities

Apply a seasonality only when a condition column equals 1.
The condition column must be present in both train and future DataFrames.

```python
df["in_season"]  = (df["ds"].dt.month > 6).astype(int)
df["off_season"] = 1 - df["in_season"]

m = Prophet(weekly_seasonality=False)
m.add_seasonality("weekly_in",  period=7, fourier_order=3, condition_name="in_season")
m.add_seasonality("weekly_off", period=7, fourier_order=3, condition_name="off_season")
m.fit(df)

future["in_season"]  = (future["ds"].dt.month > 6).astype(int)
future["off_season"] = 1 - future["in_season"]
```

---

## Sub-daily (hourly) seasonality

For hourly or finer data, enable `daily_seasonality` and use `freq="h"`:

```python
m = Prophet(
    daily_seasonality=8,     # Fourier order for intra-day pattern
    weekly_seasonality=3,
    yearly_seasonality=5,
    seasonality_mode="additive",
)
m.fit(df_hourly)

future = m.make_future_dataframe(periods=7 * 24, freq="h")
```

---

## Source file

```
src/01_prophet_fundamentals.py   ← sections 5 (custom), 8 (additive vs mult)
src/05_prophet_deep_concepts.py  ← sections 4 (Fourier order), 5 (hourly)
```
