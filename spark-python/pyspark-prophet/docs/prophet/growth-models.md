# Growth Models

Prophet supports three growth modes, controlled by the `growth` parameter.

---

## Linear growth (default)

```python
m = Prophet(growth="linear")
```

The trend is an unbounded piecewise-linear function with automatic changepoint detection.

**Use when:** the metric has no natural ceiling or floor and its long-run direction
can be extrapolated from history.

```python
m = Prophet(
    growth="linear",
    changepoint_prior_scale=0.05,   # flexibility of trend bends
)
m.fit(df)
forecast = m.predict(m.make_future_dataframe(periods=365))
```

---

## Logistic growth (S-curve, capacity-bounded)

```python
m = Prophet(growth="logistic")
```

Uses the generalised logistic function:

$$g(t) = \frac{L}{1 + \exp(-k(t - m))}$$

where $L = \text{cap} - \text{floor}$.

**Use when:** the metric approaches a known ceiling (market saturation, server capacity,
percentage utilisation) and/or has a hard floor.

!!! warning "You must supply `cap` (and optionally `floor`) in both `df` AND `future`"

```python
df["cap"]   = 8.5    # upper saturation
df["floor"] = 0.0    # lower bound (optional)

m = Prophet(growth="logistic")
m.fit(df)

future         = m.make_future_dataframe(periods=1826)
future["cap"]  = 8.5
future["floor"]= 0.0
forecast = m.predict(future)
```

### Time-varying cap

The cap can change over time to model known capacity expansions:

```python
df["cap"] = df["ds"].apply(lambda d: 120.0 if d >= pd.Timestamp("2024-01-01") else 95.0)

future["cap"] = 120.0   # new ceiling applies in the forecast horizon
```

### Cap and floor simultaneously

```python
df["cap"]   = 95.0
df["floor"] = 15.0

future["cap"]   = 95.0
future["floor"] = 15.0
```

---

## Flat growth

```python
m = Prophet(growth="flat")
```

Forces the trend to be completely constant — no slope at all.
Seasonality and holiday effects are still modelled.

**Use when:** the metric has no directional trend, only seasonal variation and noise
(e.g., temperature deviation from climatological normal, residual after detrending).

```python
m = Prophet(growth="flat", yearly_seasonality=True, weekly_seasonality=True)
m.fit(df)
```

!!! tip "Verify the trend is truly constant"
    After fitting, check that `forecast["trend"].min() ≈ forecast["trend"].max()`.

---

## Comparison

| Mode | Trend shape | Requires extra columns | Best for |
|------|-------------|----------------------|----------|
| `linear` | Piecewise linear, unbounded | — | Default; general-purpose |
| `logistic` | S-curve, bounded | `cap` (+ `floor`) | Revenue %, utilisation, adoption |
| `flat` | Constant horizontal line | — | Pure seasonality series |

---

## Source file

```
src/05_prophet_deep_concepts.py   ← sections 1 & 2
```
