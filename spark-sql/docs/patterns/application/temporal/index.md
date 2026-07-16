# :material-clock-time-four: Temporal

Query, aggregate, and compare data across date hierarchies, time bands, and seasonal patterns.

---

## :material-sitemap: Temporal Analysis Pipeline

```mermaid
flowchart LR
    EVENTS["Timestamped events"] --> TRUNC["Truncate\nDATE_TRUNC\nhour / day / week / month"]
    TRUNC --> AGG["Aggregate per period\nSUM, COUNT, AVG"]
    AGG --> COMPARE["Compare periods\nLAG for MoM / YoY\nRATIO_TO_REPORT"]
    COMPARE --> BANDS["Time bands\nweekday / weekend\nbusiness hours\nseasonality"]
    BANDS --> OUT["Temporal insights"]
```

---

## :material-book-open-variant: In This Section

| Page | Problem | Technique |
|------|---------|-----------|
| [Time Series](time_series/index.md) | Date hierarchy, weekday patterns, time bands | `DATE_TRUNC`, `DAYOFWEEK`, `HOUR` |

---

## :material-lightbulb-outline: When to Use

- Reporting — daily, weekly, monthly aggregations with period-over-period comparison.
- Operational analytics — identify peak hours, weekday vs weekend patterns.
- Seasonal analysis — compare same-month across years, detect trends.
