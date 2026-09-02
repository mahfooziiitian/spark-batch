-- ============================================================
-- Topic: Outlier detection
-- Dialect: Databricks / Spark SQL 3.5
-- Description: Identifies data points that deviate significantly from the
--              norm using z-score, IQR fencing, percentile thresholds,
--              modified z-score (MAD), moving-window, and multi-method
--              consensus techniques.
-- ============================================================

-- =============================================================================
-- Sample data 1: Daily transaction amounts by merchant
-- =============================================================================
CREATE OR REPLACE TEMP VIEW transactions AS
SELECT * FROM VALUES
    ('merchant_A', DATE '2024-06-01', 250.00),
    ('merchant_A', DATE '2024-06-02', 310.00),
    ('merchant_A', DATE '2024-06-03', 280.00),
    ('merchant_A', DATE '2024-06-04', 295.00),
    ('merchant_A', DATE '2024-06-05', 320.00),
    ('merchant_A', DATE '2024-06-06', 270.00),
    ('merchant_A', DATE '2024-06-07', 290.00),
    ('merchant_A', DATE '2024-06-08', 4500.00),
    ('merchant_A', DATE '2024-06-09', 305.00),
    ('merchant_A', DATE '2024-06-10', 285.00),
    ('merchant_A', DATE '2024-06-11', 330.00),
    ('merchant_A', DATE '2024-06-12', 15.00),
    ('merchant_B', DATE '2024-06-01', 1200.00),
    ('merchant_B', DATE '2024-06-02', 1350.00),
    ('merchant_B', DATE '2024-06-03', 1180.00),
    ('merchant_B', DATE '2024-06-04', 1290.00),
    ('merchant_B', DATE '2024-06-05', 1410.00),
    ('merchant_B', DATE '2024-06-06', 1250.00),
    ('merchant_B', DATE '2024-06-07', 12800.00),
    ('merchant_B', DATE '2024-06-08', 1300.00),
    ('merchant_B', DATE '2024-06-09', 1220.00),
    ('merchant_B', DATE '2024-06-10', 1380.00)
    AS t(merchant, txn_date, amount);

-- =============================================================================
-- Sample data 2: Hourly server response times
-- =============================================================================
CREATE OR REPLACE TEMP VIEW response_times AS
SELECT * FROM VALUES
    ('api-gw', TIMESTAMP '2024-04-10 08:00:00', 120),
    ('api-gw', TIMESTAMP '2024-04-10 09:00:00', 135),
    ('api-gw', TIMESTAMP '2024-04-10 10:00:00', 128),
    ('api-gw', TIMESTAMP '2024-04-10 11:00:00', 142),
    ('api-gw', TIMESTAMP '2024-04-10 12:00:00', 890),
    ('api-gw', TIMESTAMP '2024-04-10 13:00:00', 155),
    ('api-gw', TIMESTAMP '2024-04-10 14:00:00', 130),
    ('api-gw', TIMESTAMP '2024-04-10 15:00:00', 145),
    ('api-gw', TIMESTAMP '2024-04-10 16:00:00', 1250),
    ('api-gw', TIMESTAMP '2024-04-10 17:00:00', 138),
    ('api-gw', TIMESTAMP '2024-04-10 18:00:00', 125),
    ('api-gw', TIMESTAMP '2024-04-10 19:00:00', 132),
    ('db-svc', TIMESTAMP '2024-04-10 08:00:00', 45),
    ('db-svc', TIMESTAMP '2024-04-10 09:00:00', 52),
    ('db-svc', TIMESTAMP '2024-04-10 10:00:00', 48),
    ('db-svc', TIMESTAMP '2024-04-10 11:00:00', 55),
    ('db-svc', TIMESTAMP '2024-04-10 12:00:00', 380),
    ('db-svc', TIMESTAMP '2024-04-10 13:00:00', 50),
    ('db-svc', TIMESTAMP '2024-04-10 14:00:00', 47),
    ('db-svc', TIMESTAMP '2024-04-10 15:00:00', 53)
    AS t(service, ts, p95_ms);

-- =============================================================================
-- Sample data 3: Employee expense claims
-- =============================================================================
CREATE OR REPLACE TEMP VIEW expense_claims AS
SELECT * FROM VALUES
    ('Engineering', 'Alice', DATE '2024-05-01', 'Travel', 450.00),
    ('Engineering', 'Alice', DATE '2024-05-08', 'Meals', 85.00),
    ('Engineering', 'Bob', DATE '2024-05-02', 'Travel', 520.00),
    ('Engineering', 'Bob', DATE '2024-05-10', 'Travel', 8200.00),
    ('Engineering', 'Carol', DATE '2024-05-03', 'Meals', 72.00),
    ('Engineering', 'Carol', DATE '2024-05-12', 'Travel', 490.00),
    ('Engineering', 'Dave', DATE '2024-05-04', 'Travel', 380.00),
    ('Engineering', 'Dave', DATE '2024-05-15', 'Meals', 95.00),
    ('Sales', 'Eve', DATE '2024-05-01', 'Travel', 680.00),
    ('Sales', 'Eve', DATE '2024-05-09', 'Meals', 120.00),
    ('Sales', 'Frank', DATE '2024-05-02', 'Travel', 710.00),
    ('Sales', 'Frank', DATE '2024-05-11', 'Meals', 1950.00),
    ('Sales', 'Grace', DATE '2024-05-03', 'Travel', 650.00),
    ('Sales', 'Grace', DATE '2024-05-14', 'Meals', 105.00),
    ('Sales', 'Hank', DATE '2024-05-05', 'Travel', 590.00),
    ('Sales', 'Hank', DATE '2024-05-16', 'Meals', 88.00)
    AS t(department, employee, claim_date, category, amount);

-- =============================================================================
-- Section 1: Z-score outlier detection per merchant
-- =============================================================================
SELECT
    merchant,
    txn_date,
    amount,
    ROUND(AVG(amount) OVER w, 2) AS group_avg,
    ROUND(STDDEV(amount) OVER w, 2) AS group_stddev,
    ROUND((amount - AVG(amount) OVER w) / NULLIF(STDDEV(amount) OVER w, 0), 2) AS z_score,
    CASE
        WHEN ABS((amount - AVG(amount) OVER w) / NULLIF(STDDEV(amount) OVER w, 0)) > 3 THEN 'EXTREME'
        WHEN ABS((amount - AVG(amount) OVER w) / NULLIF(STDDEV(amount) OVER w, 0)) > 2 THEN 'SUSPICIOUS'
        ELSE 'NORMAL'
    END AS flag
FROM transactions
WINDOW w AS (PARTITION BY merchant)
ORDER BY merchant, txn_date;

-- =============================================================================
-- Section 2: IQR fence method
-- =============================================================================
WITH stats AS (
    SELECT
        merchant,
        PERCENTILE(amount, 0.25) AS q1,
        PERCENTILE(amount, 0.75) AS q3
    FROM transactions
    GROUP BY merchant
)

SELECT
    t.merchant,
    t.txn_date,
    t.amount,
    ROUND(s.q1, 2) AS q1,
    ROUND(s.q3, 2) AS q3,
    ROUND(s.q3 - s.q1, 2) AS iqr,
    ROUND(s.q1 - 1.5 * (s.q3 - s.q1), 2) AS lower_fence,
    ROUND(s.q3 + 1.5 * (s.q3 - s.q1), 2) AS upper_fence,
    CASE
        WHEN t.amount < s.q1 - 1.5 * (s.q3 - s.q1) THEN 'LOW_OUTLIER'
        WHEN t.amount > s.q3 + 1.5 * (s.q3 - s.q1) THEN 'HIGH_OUTLIER'
        ELSE 'NORMAL'
    END AS flag
FROM transactions AS t
INNER JOIN stats AS s ON t.merchant = s.merchant
ORDER BY t.merchant, t.txn_date;

-- =============================================================================
-- Section 3: Percentile threshold (top/bottom 5%)
-- =============================================================================
WITH pcts AS (
    SELECT
        merchant,
        PERCENTILE(amount, 0.05) AS p5,
        PERCENTILE(amount, 0.95) AS p95
    FROM transactions
    GROUP BY merchant
)

SELECT
    t.merchant,
    t.txn_date,
    t.amount,
    ROUND(p.p5, 2) AS p5_threshold,
    ROUND(p.p95, 2) AS p95_threshold,
    CASE
        WHEN t.amount < p.p5 THEN 'BOTTOM_5PCT'
        WHEN t.amount > p.p95 THEN 'TOP_5PCT'
        ELSE 'NORMAL'
    END AS flag
FROM transactions AS t
INNER JOIN pcts AS p ON t.merchant = p.merchant
ORDER BY t.merchant, t.txn_date;

-- =============================================================================
-- Section 4: Modified Z-score using MAD (Median Absolute Deviation)
-- More robust than standard z-scores for skewed data.
-- =============================================================================
WITH medians AS (
    SELECT
        merchant,
        PERCENTILE(amount, 0.5) AS median_val
    FROM transactions
    GROUP BY merchant
),

abs_devs AS (
    SELECT
        t.merchant,
        t.txn_date,
        t.amount,
        m.median_val,
        ABS(t.amount - m.median_val) AS abs_dev
    FROM transactions AS t
    INNER JOIN medians AS m ON t.merchant = m.merchant
),

mad AS (
    SELECT
        merchant,
        PERCENTILE(abs_dev, 0.5) AS mad_val
    FROM abs_devs
    GROUP BY merchant
)

SELECT
    a.merchant,
    a.txn_date,
    a.amount,
    ROUND(a.median_val, 2) AS median_val,
    ROUND(m.mad_val, 2) AS mad,
    ROUND(0.6745 * (a.amount - a.median_val) / NULLIF(m.mad_val, 0), 2) AS modified_z,
    CASE
        WHEN ABS(0.6745 * (a.amount - a.median_val) / NULLIF(m.mad_val, 0)) > 3.5 THEN 'OUTLIER'
        ELSE 'NORMAL'
    END AS flag
FROM abs_devs AS a
INNER JOIN mad AS m ON a.merchant = m.merchant
ORDER BY a.merchant, a.txn_date;

-- =============================================================================
-- Section 5: Moving-window anomaly detection (local context)
-- Detect spikes relative to a sliding 4-hour window rather than the global
-- distribution.
-- =============================================================================
SELECT
    service,
    ts,
    p95_ms,
    ROUND(AVG(p95_ms) OVER w, 0) AS local_avg,
    ROUND(STDDEV(p95_ms) OVER w, 0) AS local_stddev,
    ROUND((p95_ms - AVG(p95_ms) OVER w) / NULLIF(STDDEV(p95_ms) OVER w, 0), 2) AS local_z,
    CASE
        WHEN ABS((p95_ms - AVG(p95_ms) OVER w) / NULLIF(STDDEV(p95_ms) OVER w, 0)) > 2 THEN 'SPIKE'
        ELSE 'NORMAL'
    END AS flag
FROM response_times
WINDOW w AS (
    PARTITION BY service
    ORDER BY ts
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
)
ORDER BY service, ts;

-- =============================================================================
-- Section 6: Outlier-excluded statistics (trimmed mean)
-- Compute statistics after removing outliers to get a "clean" baseline.
-- =============================================================================
WITH stats AS (
    SELECT
        merchant,
        PERCENTILE(amount, 0.25) AS q1,
        PERCENTILE(amount, 0.75) AS q3
    FROM transactions
    GROUP BY merchant
),

filtered AS (
    SELECT
        t.merchant,
        t.amount,
        CASE
            WHEN t.amount < s.q1 - 1.5 * (s.q3 - s.q1) OR t.amount > s.q3 + 1.5 * (s.q3 - s.q1) THEN TRUE
            ELSE FALSE
        END AS is_outlier
    FROM transactions AS t
    INNER JOIN stats AS s ON t.merchant = s.merchant
)

SELECT
    merchant,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_count,
    ROUND(AVG(amount), 2) AS raw_avg,
    ROUND(AVG(CASE WHEN NOT is_outlier THEN amount END), 2) AS trimmed_avg,
    ROUND(STDDEV(CASE WHEN NOT is_outlier THEN amount END), 2) AS trimmed_stddev
FROM filtered
GROUP BY merchant
ORDER BY merchant;

-- =============================================================================
-- Section 7: Expense claim outlier detection by department and category
-- =============================================================================
WITH stats AS (
    SELECT
        department,
        category,
        ROUND(AVG(amount), 2) AS avg_amt,
        ROUND(STDDEV(amount), 2) AS stddev_amt,
        PERCENTILE(amount, 0.75) AS q3,
        PERCENTILE(amount, 0.25) AS q1
    FROM expense_claims
    GROUP BY department, category
)

SELECT
    e.department,
    e.employee,
    e.claim_date,
    e.category,
    e.amount,
    s.avg_amt,
    ROUND((e.amount - s.avg_amt) / NULLIF(s.stddev_amt, 0), 2) AS z_score,
    ROUND(s.q3 + 1.5 * (s.q3 - s.q1), 2) AS iqr_upper_fence,
    CASE
        WHEN e.amount > s.q3 + 1.5 * (s.q3 - s.q1) THEN 'IQR_OUTLIER'
        WHEN ABS((e.amount - s.avg_amt) / NULLIF(s.stddev_amt, 0)) > 2 THEN 'Z_OUTLIER'
        ELSE 'NORMAL'
    END AS flag
FROM expense_claims AS e
INNER JOIN stats AS s ON e.department = s.department AND e.category = s.category
WHERE e.amount > s.q3 + 1.5 * (s.q3 - s.q1)
    OR ABS((e.amount - s.avg_amt) / NULLIF(s.stddev_amt, 0)) > 2
ORDER BY e.department, e.category, e.amount DESC;

-- =============================================================================
-- Section 8: Multi-method consensus (flag if 2+ methods agree)
-- Reduce false positives by requiring agreement across detection methods.
-- =============================================================================
WITH base AS (
    SELECT
        merchant,
        txn_date,
        amount,
        AVG(amount) OVER w AS grp_avg,
        STDDEV(amount) OVER w AS grp_stddev,
        PERCENTILE(amount, 0.25) OVER w AS q1,
        PERCENTILE(amount, 0.75) OVER w AS q3,
        PERCENTILE(amount, 0.05) OVER w AS p5,
        PERCENTILE(amount, 0.95) OVER w AS p95
    FROM transactions
    WINDOW w AS (PARTITION BY merchant)
),

scored AS (
    SELECT
        *,
        CASE WHEN ABS((amount - grp_avg) / NULLIF(grp_stddev, 0)) > 2 THEN 1 ELSE 0 END AS z_flag,
        CASE WHEN amount < q1 - 1.5 * (q3 - q1) OR amount > q3 + 1.5 * (q3 - q1) THEN 1 ELSE 0 END AS iqr_flag,
        CASE WHEN amount < p5 OR amount > p95 THEN 1 ELSE 0 END AS pct_flag
    FROM base
)

SELECT
    merchant,
    txn_date,
    amount,
    z_flag,
    iqr_flag,
    pct_flag,
    z_flag + iqr_flag + pct_flag AS agreement_count,
    CASE WHEN z_flag + iqr_flag + pct_flag >= 2 THEN 'CONFIRMED_OUTLIER' ELSE 'NORMAL' END AS verdict
FROM scored
WHERE z_flag + iqr_flag + pct_flag >= 1
ORDER BY agreement_count DESC, merchant, txn_date;

-- =============================================================================
-- Section 9: Outlier summary report per group
-- =============================================================================
WITH stats AS (
    SELECT
        merchant,
        PERCENTILE(amount, 0.25) AS q1,
        PERCENTILE(amount, 0.75) AS q3
    FROM transactions
    GROUP BY merchant
),

flagged AS (
    SELECT
        t.merchant,
        t.amount,
        CASE
            WHEN t.amount < s.q1 - 1.5 * (s.q3 - s.q1) OR t.amount > s.q3 + 1.5 * (s.q3 - s.q1) THEN TRUE
            ELSE FALSE
        END AS is_outlier
    FROM transactions AS t
    INNER JOIN stats AS s ON t.merchant = s.merchant
)

SELECT
    merchant,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_count,
    ROUND(SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS outlier_pct,
    ROUND(SUM(CASE WHEN is_outlier THEN amount ELSE 0 END), 2) AS outlier_total_amount,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(SUM(CASE WHEN is_outlier THEN amount ELSE 0 END) * 100.0 / SUM(amount), 1) AS outlier_amount_pct
FROM flagged
GROUP BY merchant
ORDER BY outlier_count DESC;

-- =============================================================================
-- Section 10: Quarantine outliers into a separate table
-- =============================================================================
WITH stats AS (
    SELECT
        merchant,
        PERCENTILE(amount, 0.25) AS q1,
        PERCENTILE(amount, 0.75) AS q3,
        AVG(amount) AS grp_avg,
        STDDEV(amount) AS grp_stddev
    FROM transactions
    GROUP BY merchant
),

scored AS (
    SELECT
        t.*,
        ROUND((t.amount - s.grp_avg) / NULLIF(s.grp_stddev, 0), 2) AS z_score,
        CASE
            WHEN t.amount < s.q1 - 1.5 * (s.q3 - s.q1) OR t.amount > s.q3 + 1.5 * (s.q3 - s.q1) THEN 'IQR'
        END AS iqr_flag,
        CASE
            WHEN ABS((t.amount - s.grp_avg) / NULLIF(s.grp_stddev, 0)) > 3 THEN 'Z_SCORE'
        END AS z_flag
    FROM transactions AS t
    INNER JOIN stats AS s ON t.merchant = s.merchant
)

SELECT
    merchant,
    txn_date,
    amount,
    z_score,
    CONCAT_WS(', ', iqr_flag, z_flag) AS detection_methods,
    'quarantined' AS status
FROM scored
WHERE iqr_flag IS NOT NULL OR z_flag IS NOT NULL
ORDER BY merchant, txn_date;
