# :material-puzzle: Query Patterns

Self-contained Spark SQL recipes — each pattern includes inline sample data, the query, and the exact result output so you can read, run, and adapt immediately.

______________________________________________________________________

## :material-sitemap: Pattern Taxonomy

```mermaid
mindmap
  root((Query Patterns))
    Aggregation
      Running Total
      Moving Average
      Conditional Agg
      String Agg
      Relational Division
    Ranking
      Top-N
      Pagination
      Pivot / Unpivot
    Sequence
      Gaps & Islands
      Sessionization
      Interval Merge
      Sequence Mining
      State Machine
    Customer Analytics
      Funnel & Retention
      CLV & RFM
      Basket & Path
      Attribution
      Churn & Survival
    Data Quality
      Change Detection
      Outlier Detection
      Fraud Detection
    Structural
      Hierarchy
      Graph Analytics
      Network Analysis
    SCD
      Type 1–6
      Hybrid Approaches
    Temporal SQL
      Point-in-Time Lookup
      Effective-Dated Join
      Temporal Overlap
      Temporal Gaps
      As-of Join
      Event Ordering
    Time Series
      Windowing
        Tumbling · Hopping · Sliding · Session
      Analysis
        Lag/Lead · Gap Fill · Rolling Stats
        Peak · Trend · Seasonality
        Capacity · Queue · Utilization
        Cost · Efficiency · Reliability
    Query Optimization
      Partition Pruning
      Predicate Pushdown
      Broadcast vs Shuffle Join
      Skew Handling
      Repartition vs Coalesce
    Applications
      ETL Pipelines
      Transformations
      Enrichment
```

______________________________________________________________________

## :material-pin: Pattern Catalogue

### :material-sigma: Aggregation

| Pattern                                                   | Problem                             | Key Technique                               |
| --------------------------------------------------------- | ----------------------------------- | ------------------------------------------- |
| [Running Total](aggregation/running-total.md)             | Cumulative sums                     | `SUM() OVER (ORDER BY)`                     |
| [Moving Average](aggregation/moving-average.md)           | Smoothed trends                     | `AVG() OVER (ROWS BETWEEN)`                 |
| [Growing Window](aggregation/growing-window.md)           | Expanding aggregates                | Unbounded preceding frame                   |
| [Conditional Aggregation](aggregation/conditional-agg.md) | Pivot without `PIVOT`               | `SUM(CASE WHEN ...)`                        |
| [String Aggregation](aggregation/string-agg.md)           | Concatenate values                  | `COLLECT_LIST`, `ARRAY_JOIN`                |
| [Relational Division](aggregation/relational-division.md) | Bought every item in a required set | `HAVING COUNT(DISTINCT ...) = divisor size` |

### :material-podium: Ranking

| Pattern                             | Problem                  | Key Technique                     |
| ----------------------------------- | ------------------------ | --------------------------------- |
| [Top-N](ranking/top-n.md)           | Highest/lowest per group | `ROW_NUMBER`, `DENSE_RANK`        |
| [Pagination](ranking/pagination.md) | Page through results     | `LIMIT` / `OFFSET`, keyset cursor |

### :material-transit-connection-variant: Sequence

| Pattern                                                      | Problem                          | Key Technique                    |
| ------------------------------------------------------------ | -------------------------------- | -------------------------------- |
| [Gaps & Islands](sequence/gaps-islands.md)                   | Consecutive sequences and breaks | `ROW_NUMBER` delta grouping      |
| [Sessionization](sequence/sessionization.md)                 | Session boundary detection       | Gap-based splitting              |
| [Interval Merge](sequence/interval-merge.md)                 | Merge overlapping ranges         | Running max of end times         |
| [Period Comparison](sequence/period-comparison.md)           | YoY / MoM change                 | `LAG`, `LEAD`, `DATE_TRUNC`      |
| [Nearest Time](sequence/nearest-time.md)                     | Find closest timestamp           | `ABS(DATEDIFF)`, window min      |
| [Event Stream Analytics](sequence/event-stream-analytics.md) | Process clickstreams             | `LAG`/`LEAD`, session gaps       |
| [Sequence Mining](sequence/sequence-mining.md)               | Ordered event patterns           | N-grams, support, confidence     |
| [State Machine Analysis](sequence/state-machine.md)          | Validate state transitions       | Transition rules, loop detection |

### :material-account-group: Customer Analytics

| Pattern                                                            | Problem                    | Key Technique                  |
| ------------------------------------------------------------------ | -------------------------- | ------------------------------ |
| [Funnel Analysis](customer_analytics/funnel-analysis.md)           | Conversion drop-off        | `COUNT(IF(...))`, window funcs |
| [Retention](customer_analytics/retention.md)                       | Cohort return rates        | `DATE_TRUNC`, cohort join      |
| [Churn Detection](customer_analytics/churn-detection.md)           | Identify at-risk customers | Inactivity thresholds, scoring |
| [Survival Analysis](customer_analytics/survival-analysis.md)       | Time-to-event modelling    | Kaplan-Meier, censoring        |
| [CLV](customer_analytics/clv.md)                                   | Customer lifetime value    | AOV × frequency × lifespan     |
| [RFM Segmentation](customer_analytics/rfm-segmentation.md)         | Behavioural segmentation   | `NTILE`, composite scoring     |
| [Basket Analysis](customer_analytics/basket-analysis.md)           | Products bought together   | Self-join, support, lift       |
| [Path Analysis](customer_analytics/path-analysis.md)               | Navigation sequences       | `LEAD`/`LAG`, `COLLECT_LIST`   |
| [Attribution Modeling](customer_analytics/attribution-modeling.md) | Channel revenue credit     | First/last touch, time decay   |
| [ABC Classification](customer_analytics/abc-classification.md)     | Pareto segmentation        | Cumulative %, `NTILE`          |
| [Pareto](customer_analytics/pareto.md)                             | 80/20 analysis             | Running sum percentage         |

### :material-shield-check: Data Quality

| Pattern                                                                  | Problem                                    | Key Technique                                       |
| ------------------------------------------------------------------------ | ------------------------------------------ | --------------------------------------------------- |
| [Change Detection](data_quality/change-detection.md)                     | Find row-level changes                     | Hash comparison, `EXCEPT`                           |
| [Outlier Detection](data_quality/outlier-detection.md)                   | Spot anomalous values                      | Z-score, IQR, percentiles                           |
| [Slowly Changing Comparison](data_quality/slowly-changing-comparison.md) | Compare dimension snapshots                | Full outer join, `<=>`                              |
| [Fraud Pattern Detection](data_quality/fraud-detection.md)               | Multi-account, velocity, impossible travel | Window counts, self-join                            |
| [Duplicates](data_quality/duplicates/index.md)                           | Find and remove duplicate rows             | `GROUP BY HAVING`, `ROW_NUMBER`, `QUALIFY`, `MERGE` |

### :material-file-tree: Structural

| Pattern                                            | Problem                | Key Technique              |
| -------------------------------------------------- | ---------------------- | -------------------------- |
| [Hierarchy](structural/hierarchy.md)               | Parent-child traversal | Recursive CTE, self-join   |
| [Graph Analytics](structural/graph-analytics.md)   | Relationship networks  | Connected components, BFS  |
| [Network Analysis](structural/network-analysis.md) | IP-device-user mapping | Multi-factor link analysis |

### :material-delta: SCD

| Pattern                      | Problem                 | Key Technique            |
| ---------------------------- | ----------------------- | ------------------------ |
| [SCD Overview](scd/index.md) | Track dimension history | Type 1–6, merge patterns |

### :material-clock-outline: Temporal SQL

Temporal queries — "what was true at time X," "which version matches this event,"
"do these two periods overlap" — are notoriously easy to get subtly wrong (off-by-one
interval boundaries, silent row multiplication, non-deterministic tie-breaks). Each
row below links to a dedicated page with verified SQL, not just a technique name.

| Pattern                    | Problem                                     | Key Technique                                                                                                                               |
| -------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Point-in-time lookup       | What was the customer's address on date X?  | [Point-in-Interval range join](../querying/joins/types/non_equi_join/range_join/point-in-interval.md) — `value BETWEEN low AND high`        |
| Effective-dated join       | Match a fact to the valid dimension version | [SCD Type 2 Join Problems](../querying/joins/issues/scd-type2-join.md) — half-open interval `[valid_from, valid_to)`                        |
| SCD Type 2                 | Historical dimension tracking               | [SCD Overview](scd/index.md) — Type 1–6, `MERGE INTO` maintenance                                                                           |
| Temporal overlap           | Find overlapping validity periods           | [Interval Overlap range join](../querying/joins/types/non_equi_join/range_join/interval-overlap.md) — `a.start < b.end AND b.start < a.end` |
| Temporal gaps              | Missing periods                             | [Gap Fill](timeseries/analysis/gap-filling.md) — `SEQUENCE` + `EXPLODE` + left join                                                         |
| As-of join                 | Match each event to state at that moment    | [Nearest Time — as-of join](sequence/nearest-time.md) — nearest *preceding* match, not nearest-absolute                                     |
| Slowly changing attributes | Detect attribute validity / what changed    | [Slowly Changing Comparison](data_quality/slowly-changing-comparison.md) — full outer join, `<=>`                                           |
| Event ordering             | Reconstruct state from events               | [State Machine Analysis](sequence/state-machine.md) — transition rules, `LAG`-based validation                                              |

!!! warning "The recurring trap: boundary semantics, not syntax"

    Every temporal-overlap and effective-dated-join bug in this table reduces to
    the same root cause: an inclusive `BETWEEN`/`<=` where a half-open
    `[start, end)` interval was needed, so a row at the exact boundary matches
    two versions (or zero). See [SCD Type 2 Join Problems](../querying/joins/issues/scd-type2-join.md)
    for a worked, verified example of this exact failure and its fix. Diagnose
    with the [Six-Axis Quality Rubric](../optimization/optimization.md#the-six-axis-quality-rubric) —
    temporal bugs are a cardinality problem first, a syntax problem second.

### :material-chart-timeline-variant: Time Series — Windowing

| Pattern                                                    | Problem                         | Key Technique              |
| ---------------------------------------------------------- | ------------------------------- | -------------------------- |
| [Tumbling Window](timeseries/windowing/tumbling-window.md) | Fixed non-overlapping intervals | `DATE_TRUNC`, `GROUP BY`   |
| [Hopping Window](timeseries/windowing/hopping-window.md)   | Fixed overlapping intervals     | `EXPLODE` + `SEQUENCE`     |
| [Sliding Window](timeseries/windowing/sliding-window.md)   | Per-row rolling window          | `ROWS BETWEEN n PRECEDING` |
| [Session Window](timeseries/windowing/session-window.md)   | Activity-based intervals        | Gap detection + grouping   |

### :material-chart-timeline-variant: Time Series — Analysis

| Pattern                                                                   | Problem                       | Key Technique                         |
| ------------------------------------------------------------------------- | ----------------------------- | ------------------------------------- |
| [Time Aggregation](timeseries/analysis/time-aggregation.md)               | Temporal GROUP BY             | `DATE_TRUNC`                          |
| [Lag & Lead](timeseries/analysis/lag-and-lead.md)                         | Previous/next value           | `LAG`, `LEAD`                         |
| [Gap Filling](timeseries/analysis/gap-filling.md)                         | Missing timestamps            | `SEQUENCE` + `EXPLODE` + left join    |
| [Forecast Features](timeseries/analysis/forecast-features.md)             | ML-ready lag features         | `LAG(n)`, rolling AVG/MAX             |
| [Feature Engineering](timeseries/analysis/feature-engineering.md)         | General ML features           | Category counts, composite scores     |
| [Seasonality Detection](timeseries/analysis/seasonality-detection.md)     | WoW / MoM / YoY               | `LAG`, seasonal index                 |
| [Rolling Statistics](timeseries/analysis/rolling-statistics.md)           | Median, variance, stddev      | `PERCENTILE_APPROX`, `STDDEV` OVER    |
| [Peak Detection](timeseries/analysis/peak-detection.md)                   | Maximum values, local peaks   | `ROW_NUMBER`, `LAG`/`LEAD` comparison |
| [Trend Detection](timeseries/analysis/trend-detection.md)                 | Upward/downward trends        | Consecutive streaks, rolling slope    |
| [Utilization Analysis](timeseries/analysis/utilization-analysis.md)       | Busy/idle/maintenance time    | `LEAD` duration, conditional SUM      |
| [Queue Analysis](timeseries/analysis/queue-analysis.md)                   | Wait time, queue length       | Arrival/start/end timing              |
| [Capacity Planning](timeseries/analysis/capacity-planning.md)             | Forecast resource exhaustion  | Growth rate, linear projection        |
| [Inventory Analytics](timeseries/analysis/inventory-analytics.md)         | Turnover, stock-outs, DOI     | Demand rate, threshold alerts         |
| [Interval Analytics](timeseries/analysis/interval-analytics.md)           | Overlaps, concurrency, gaps   | Self-join on time range               |
| [Time Allocation](timeseries/analysis/time-allocation.md)                 | State-based time split        | Conditional aggregation               |
| [Resource Contention](timeseries/analysis/resource-contention.md)         | Competing jobs                | Running sum ±1 concurrency            |
| [Reliability Metrics](timeseries/analysis/reliability-metrics.md)         | MTBF, MTTR, availability      | Failure/recovery intervals            |
| [Workload Classification](timeseries/analysis/workload-classification.md) | Interactive/Batch/ETL/BI/ML   | Rule-based query tagging              |
| [Cost Attribution](timeseries/analysis/cost-attribution.md)               | Per-query/user/warehouse cost | DBU aggregation **[Databricks]**      |
| [Idle Time Analysis](timeseries/analysis/idle-time-analysis.md)           | Wasted compute periods        | Gap detection, auto-suspend           |
| [Resource Efficiency](timeseries/analysis/resource-efficiency.md)         | CPU, cache, queries/DBU       | Efficiency scorecard                  |

### :material-speedometer: Query Optimization

| Pattern                                                      | Problem                                | Key Technique                                         |
| ------------------------------------------------------------ | -------------------------------------- | ----------------------------------------------------- |
| [Query Optimization as a SQL Problem](optimization/index.md) | Same SQL, radically different cost     | `EXPLAIN FORMATTED`, `Exchange`/`Expand` inspection   |
| Partition pruning & predicate pushdown                       | Full scans despite selective filters   | Avoid UDFs/opaque functions on filtered columns       |
| Broadcast vs. sort-merge vs. shuffled-hash join              | Wrong join strategy chosen             | Broadcast hints, `ANALYZE TABLE`, AQE join conversion |
| Skew handling                                                | One task runs far longer than the rest | Salting, skew join hints, AQE skew splitting          |
| `REPARTITION` vs. `COALESCE`                                 | Unneeded shuffle to change file count  | `COALESCE` when reducing partitions without rebalance |

### :material-application-cog: Applications

| Pattern                                                      | Problem                      | Key Technique               |
| ------------------------------------------------------------ | ---------------------------- | --------------------------- |
| [Applications Overview](application/index.md)                | End-to-end pipeline patterns | CTE chains, ETL recipes     |
| [Pivot / Unpivot](application/transformation/pivot/index.md) | Reshape rows ↔ columns       | `PIVOT`, `UNPIVOT`, `STACK` |

______________________________________________________________________

## :material-animation-play: Interactive Demo

Explore the pattern landscape — click a category to see its patterns and complexity.

<div id="viz-patterns-overview" class="ts-viz"></div>

______________________________________________________________________

## :material-information-outline: How to Read Each Pattern

Every page follows the same structure:

```mermaid
flowchart LR
    A[Sample Data] --> B[Pattern Query]
    B --> C[Variations]
    C --> D[When to Use]

    style A fill:#e3f2fd,stroke:#1e88e5
    style B fill:#e8f5e9,stroke:#43a047
    style C fill:#fff3e0,stroke:#fb8c00
    style D fill:#fce4ec,stroke:#e53935
```

1. **Sample Data** — a `VALUES`-based temp view you can run directly.
2. **Pattern Query** — the SQL with inline `-- Result:` annotations.
3. **Variations** — alternative approaches for edge cases.
4. **When to Use** — decision table for choosing the right pattern.

______________________________________________________________________

## :material-toy-brick: Shared Dataset

Several patterns share a common `orders` and `employees` dataset.

```sql
-- orders — used in pagination, period comparison, conditional aggregation
CREATE OR REPLACE TEMP VIEW orders AS
SELECT * FROM VALUES
  (1,  'alice',  'electronics', 1200.00, DATE '2024-01-15'),
  (2,  'bob',    'clothing',     89.50, DATE '2024-01-22'),
  (3,  'alice',  'books',        34.99, DATE '2024-02-03'),
  (4,  'carol',  'electronics',  799.00, DATE '2024-02-14'),
  (5,  'bob',    'electronics',  249.00, DATE '2024-03-01'),
  (6,  'alice',  'clothing',     125.00, DATE '2024-03-10'),
  (7,  'carol',  'books',         19.99, DATE '2024-04-05'),
  (8,  'dave',   'electronics',  599.00, DATE '2024-04-18'),
  (9,  'alice',  'electronics',  349.00, DATE '2024-05-02'),
  (10, 'bob',    'clothing',      67.00, DATE '2024-05-20'),
  (11, 'carol',  'electronics', 1099.00, DATE '2023-11-10'),
  (12, 'dave',   'books',         45.00, DATE '2023-12-22')
AS t(order_id, customer, category, amount, order_date);

-- employees — used in hierarchy pattern
CREATE OR REPLACE TEMP VIEW employees AS
SELECT * FROM VALUES
  (1,  'Eve',    NULL, 'CEO',        200000),
  (2,  'Alice',  1,    'VP Eng',     150000),
  (3,  'Bob',    1,    'VP Sales',   140000),
  (4,  'Carol',  2,    'Engineer',    95000),
  (5,  'Dave',   2,    'Engineer',    92000),
  (6,  'Frank',  3,    'Sales Rep',   70000),
  (7,  'Grace',  3,    'Sales Rep',   68000),
  (8,  'Hank',   4,    'Junior Eng',  60000)
AS t(emp_id, name, manager_id, title, salary);
```
