# :material-cart: Basket Analysis

Find **products frequently purchased together** — enabling cross-sell recommendations,
store layout optimisation, and bundle pricing strategies.

---

## :material-sitemap: Analysis Flow

```mermaid
flowchart LR
    TXN[Transaction Data] --> PAIRS[Pair Generation\nSelf-join on basket_id]
    PAIRS --> FREQ[Frequency Count\nCo-occurrence matrix]
    FREQ --> METRICS[Association Metrics\nSupport · Confidence · Lift]
    METRICS --> ACTION[Actionable Insights\nBundles · Recommendations]

    style TXN fill:#e3f2fd,stroke:#1e88e5
    style PAIRS fill:#e8f5e9,stroke:#43a047
    style FREQ fill:#fff3e0,stroke:#fb8c00
    style ACTION fill:#fce4ec,stroke:#e53935
```

---

## :material-code-tags: Syntax

### Sample data

```sql
CREATE OR REPLACE TEMP VIEW basket_items AS
SELECT * FROM VALUES
  (1, 'Milk'),    (1, 'Bread'),   (1, 'Butter'),
  (2, 'Milk'),    (2, 'Bread'),   (2, 'Eggs'),
  (3, 'Bread'),   (3, 'Butter'),  (3, 'Jam'),
  (4, 'Milk'),    (4, 'Bread'),   (4, 'Butter'),  (4, 'Eggs'),
  (5, 'Milk'),    (5, 'Eggs'),    (5, 'Cheese'),
  (6, 'Bread'),   (6, 'Butter'),
  (7, 'Milk'),    (7, 'Bread'),   (7, 'Butter'),  (7, 'Cheese'),
  (8, 'Eggs'),    (8, 'Cheese'),  (8, 'Milk')
AS t(basket_id, product);
```

---

### Product pair generation (self-join)

Generate all unique product pairs within the same basket.

```sql
SELECT
    a.product                                      AS product_a,
    b.product                                      AS product_b,
    COUNT(DISTINCT a.basket_id)                    AS co_occurrence
FROM basket_items a
JOIN basket_items b
    ON a.basket_id = b.basket_id
    AND a.product < b.product
GROUP BY a.product, b.product
ORDER BY co_occurrence DESC;
-- Result:
-- +----------+----------+---------------+
-- |product_a |product_b |co_occurrence  |
-- +----------+----------+---------------+
-- |Bread     |Milk      |5              |
-- |Butter    |Bread     |4              |
-- |Butter    |Milk      |3              |
-- |Eggs      |Milk      |3              |
-- |Bread     |Eggs      |2              |
-- |...       |...       |...            |
-- +----------+----------+---------------+
```

---

### Association metrics (support, confidence, lift)

Calculate classic market basket metrics for each product pair.

```sql
WITH total_baskets AS (
    SELECT COUNT(DISTINCT basket_id) AS n FROM basket_items
),
product_freq AS (
    SELECT
        product,
        COUNT(DISTINCT basket_id)                  AS baskets_with
    FROM basket_items
    GROUP BY product
),
pair_freq AS (
    SELECT
        a.product                                  AS product_a,
        b.product                                  AS product_b,
        COUNT(DISTINCT a.basket_id)                AS pair_count
    FROM basket_items a
    JOIN basket_items b
        ON a.basket_id = b.basket_id
        AND a.product < b.product
    GROUP BY a.product, b.product
)
SELECT
    pf.product_a,
    pf.product_b,
    pf.pair_count,
    -- Support: P(A ∩ B)
    ROUND(pf.pair_count * 1.0 / tb.n, 3)          AS support,
    -- Confidence: P(B | A)
    ROUND(
        pf.pair_count * 1.0 / fa.baskets_with, 3
    )                                              AS confidence_a_to_b,
    -- Lift: P(A ∩ B) / (P(A) × P(B))
    ROUND(
        (pf.pair_count * 1.0 / tb.n)
        / ((fa.baskets_with * 1.0 / tb.n) * (fb.baskets_with * 1.0 / tb.n)),
        3
    )                                              AS lift
FROM pair_freq pf
CROSS JOIN total_baskets tb
JOIN product_freq fa ON pf.product_a = fa.product
JOIN product_freq fb ON pf.product_b = fb.product
ORDER BY lift DESC;
```

---

### Top-N recommendations per product

For each product, find the top 3 most associated products.

```sql
WITH pair_metrics AS (
    SELECT
        a.product                                  AS source_product,
        b.product                                  AS recommended,
        COUNT(DISTINCT a.basket_id)                AS co_occurrence,
        ROW_NUMBER() OVER (
            PARTITION BY a.product
            ORDER BY COUNT(DISTINCT a.basket_id) DESC
        )                                          AS rank
    FROM basket_items a
    JOIN basket_items b
        ON a.basket_id = b.basket_id
        AND a.product != b.product
    GROUP BY a.product, b.product
)
SELECT
    source_product,
    recommended,
    co_occurrence
FROM pair_metrics
WHERE rank <= 3
ORDER BY source_product, rank;
```

---

### Frequent itemsets (3-product combinations)

Extend beyond pairs to find frequent triples.

```sql
SELECT
    a.product                                      AS product_1,
    b.product                                      AS product_2,
    c.product                                      AS product_3,
    COUNT(DISTINCT a.basket_id)                    AS frequency
FROM basket_items a
JOIN basket_items b
    ON a.basket_id = b.basket_id
    AND a.product < b.product
JOIN basket_items c
    ON a.basket_id = c.basket_id
    AND b.product < c.product
GROUP BY a.product, b.product, c.product
HAVING COUNT(DISTINCT a.basket_id) >= 2
ORDER BY frequency DESC;
-- Result:
-- +----------+----------+----------+-----------+
-- |product_1 |product_2 |product_3 |frequency  |
-- +----------+----------+----------+-----------+
-- |Bread     |Butter    |Milk      |3          |
-- |Bread     |Eggs      |Milk      |2          |
-- +----------+----------+----------+-----------+
```

---

## :material-information-outline: Key Concepts

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **Support** | `P(A ∩ B) = pair_count / total_baskets` | How common is the pair overall |
| **Confidence** | `P(B\|A) = pair_count / baskets_with_A` | If A is bought, how likely is B |
| **Lift** | `support / (P(A) × P(B))` | >1 = positive association, 1 = independent, <1 = negative |

!!! tip "Filter by minimum support"
    In large catalogues, most pairs have near-zero support. Filter to
    `support >= 0.01` (1%) to focus on commercially meaningful associations.

!!! note "Self-join ordering trick"
    `a.product < b.product` ensures each pair appears only once (Bread-Milk,
    not also Milk-Bread), halving the output and avoiding duplicates.

---

## :material-lightbulb-outline: When to Use

| Scenario | Action |
|----------|--------|
| Cross-sell recommendations | "Customers who bought X also bought Y" |
| Bundle pricing | Discount frequently co-purchased items as a package |
| Store layout | Place high-lift pairs in proximity |
| Inventory planning | Stock associated items together for fulfilment efficiency |
| Promotional campaigns | Feature complementary products in same campaign |

---

## :material-speedometer: Performance Notes

| Tip | Reason |
|-----|--------|
| Filter to minimum basket size ≥ 2 | Single-item baskets cannot form pairs |
| Use `product < product` not `!=` | Halves the join output; eliminates duplicate pairs |
| Pre-aggregate product frequency | Avoids repeated full scans for support calculation |
| Limit triple/quad generation to high-support pairs | Self-join cost grows combinatorially |
| Broadcast small product dimension | `/*+ BROADCAST(product_freq) */` for large transaction tables |

---

## :material-arrow-right: Related

- [RFM Segmentation](rfm_segmentation.md) — segment customers by purchase behaviour
- [Customer Lifetime Value](clv.md) — estimate total customer value
- [Conditional Aggregation](../aggregation/conditional_agg.md) — pivot techniques used in co-occurrence matrices
