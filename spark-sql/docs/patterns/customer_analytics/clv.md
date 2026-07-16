# :material-cash-multiple: Customer Lifetime Value (CLV)

Estimate the **total value a customer generates** over their entire relationship
with your business — using purchase frequency, average order value, and retention
duration to predict future revenue contribution.

---

## :material-sitemap: Execution Flow

```mermaid
flowchart LR
    ORDERS["Order history"] --> METRICS["Per-customer metrics\nrecency · frequency · monetary"]
    METRICS --> AOV["Average order value\nrevenue / orders"]
    AOV --> FREQ["Purchase frequency\norders / active months"]
    FREQ --> LIFESPAN["Retention duration\nfirst to last purchase span"]
    LIFESPAN --> CLV["CLV estimate\nAOV × frequency × lifespan"]
```

---

## :material-code-tags: Syntax

### Core CLV metrics per customer

```sql
WITH customer_metrics AS (
    SELECT
        customer_id,
        COUNT(*)                                           AS total_orders,
        ROUND(SUM(amount), 2)                              AS total_revenue,
        ROUND(AVG(amount), 2)                              AS avg_order_value,
        MIN(order_date)                                    AS first_purchase,
        MAX(order_date)                                    AS last_purchase,
        DATEDIFF(MAX(order_date), MIN(order_date))         AS lifespan_days,
        ROUND(
            DATEDIFF(MAX(order_date), MIN(order_date)) / 30.0,
            1
        )                                                  AS lifespan_months
    FROM orders
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    total_revenue,
    avg_order_value,
    first_purchase,
    last_purchase,
    lifespan_months,
    -- Purchase frequency: orders per month
    ROUND(
        CASE WHEN lifespan_months > 0
             THEN total_orders / lifespan_months
             ELSE total_orders
        END, 2
    )                                                      AS orders_per_month,
    -- Simple CLV: AOV × frequency × projected lifespan (e.g., 24 months)
    ROUND(
        avg_order_value
        * (CASE WHEN lifespan_months > 0
                THEN total_orders / lifespan_months
                ELSE total_orders END)
        * 24,
        2
    )                                                      AS projected_clv_24m
FROM customer_metrics
ORDER BY projected_clv_24m DESC;
```

---

### Cohort-based CLV

Compare lifetime value across signup cohorts to measure improvements over time.

```sql
WITH cohorts AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM orders
    GROUP BY customer_id
),
customer_revenue AS (
    SELECT
        c.cohort_month,
        o.customer_id,
        SUM(o.amount)                                      AS lifetime_revenue,
        COUNT(*)                                           AS lifetime_orders,
        DATEDIFF(MAX(o.order_date), MIN(o.order_date))     AS lifespan_days
    FROM orders o
    JOIN cohorts c ON o.customer_id = c.customer_id
    GROUP BY c.cohort_month, o.customer_id
)
SELECT
    cohort_month,
    COUNT(*)                                               AS customers,
    ROUND(AVG(lifetime_revenue), 2)                        AS avg_clv,
    ROUND(PERCENTILE_APPROX(lifetime_revenue, 0.5), 2)    AS median_clv,
    ROUND(AVG(lifetime_orders), 1)                         AS avg_orders,
    ROUND(AVG(lifespan_days / 30.0), 1)                    AS avg_lifespan_months
FROM customer_revenue
GROUP BY cohort_month
ORDER BY cohort_month;
```

---

### CLV segmentation with NTILE

Bucket customers into value tiers for targeted marketing.

```sql
WITH customer_clv AS (
    SELECT
        customer_id,
        SUM(amount)                                        AS total_revenue,
        COUNT(*)                                           AS total_orders,
        ROUND(AVG(amount), 2)                              AS avg_order_value,
        DATEDIFF(MAX(order_date), MIN(order_date)) / 30.0  AS lifespan_months
    FROM orders
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_revenue,
    total_orders,
    avg_order_value,
    lifespan_months,
    NTILE(5) OVER (ORDER BY total_revenue DESC)            AS value_tier,
    CASE NTILE(5) OVER (ORDER BY total_revenue DESC)
        WHEN 1 THEN 'Platinum'
        WHEN 2 THEN 'Gold'
        WHEN 3 THEN 'Silver'
        WHEN 4 THEN 'Bronze'
        ELSE 'At-Risk'
    END                                                    AS tier_label
FROM customer_clv
ORDER BY total_revenue DESC;
```

---

### RFM-based CLV scoring

Combine Recency, Frequency, and Monetary scores for a composite CLV indicator.

```sql
WITH params AS (
    SELECT DATE '2024-06-30' AS reference_date
),
rfm AS (
    SELECT
        customer_id,
        DATEDIFF(p.reference_date, MAX(order_date))        AS recency_days,
        COUNT(*)                                           AS frequency,
        ROUND(SUM(amount), 2)                              AS monetary
    FROM orders
    CROSS JOIN params p
    GROUP BY customer_id, p.reference_date
),
scored AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days ASC)          AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)            AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)             AS m_score
    FROM rfm
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    ROUND((r_score + f_score + m_score) / 3.0, 2)         AS composite_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champion'
        WHEN r_score >= 3 AND f_score >= 3                   THEN 'Loyal'
        WHEN r_score >= 4 AND f_score <= 2                   THEN 'New High-Value'
        WHEN r_score <= 2 AND f_score >= 3                   THEN 'At Risk'
        ELSE 'Nurture'
    END                                                    AS segment
FROM scored
ORDER BY composite_score DESC;
```

---

## :material-information-outline: Key Concepts

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| Average Order Value (AOV) | `total_revenue / total_orders` | Spending per transaction |
| Purchase Frequency | `total_orders / lifespan_months` | How often they buy |
| Retention Duration | `last_purchase - first_purchase` | How long they stay active |
| Simple CLV | `AOV × frequency × projected_lifespan` | Expected future revenue |
| RFM Score | Quintile of recency + frequency + monetary | Composite value indicator |

!!! tip "Projected vs historical CLV"
    Historical CLV sums past revenue. Projected CLV multiplies current behaviour
    by an expected future lifespan. Use projected CLV for marketing budget allocation;
    use historical CLV for reporting and segmentation.

---

## :material-lightbulb-outline: When to Use

| Scenario | Approach |
|----------|----------|
| Marketing budget allocation | Invest more in high-CLV customer acquisition channels |
| Churn prevention | Prioritise retention efforts for high-CLV at-risk customers |
| Pricing strategy | Understand price sensitivity by CLV tier |
| Product recommendations | Tailor offers based on predicted lifetime spend |
| Cohort comparison | Measure whether newer cohorts have higher CLV than older ones |

---

## :material-speedometer: Performance Notes

| Tip | Reason |
|-----|--------|
| Pre-aggregate orders to customer level first | Reduces row count before window/NTILE computations |
| Use `PERCENTILE_APPROX` over `PERCENTILE` | Approximate is much faster on large datasets |
| Partition NTILE by region/segment if needed | Avoids global sort across all customers |
| Filter to recent orders (e.g., last 2 years) | Excludes inactive customers that skew averages |

---

## :material-arrow-right: Related

- [Retention Analysis](retention.md) — cohort-based return rates
- [Churn Detection](churn_detection.md) — identify customers about to leave
- [ABC Classification](abc_classification.md) — Pareto-based customer segmentation
- [Survival Analysis](survival_analysis.md) — time-to-churn modelling
