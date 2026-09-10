# :material-table-large: Join Troubleshooting Matrix

A single at-a-glance lookup for production join debugging. Where the
[Join Debugging Workflow](debugging_workflow.md) walks you through *what to check in what
order*, this matrix lets you **jump straight from a symptom to a root cause and fix**. Each
row links to a deep-dive page with runnable, Spark-verified data examples.

!!! tip "How to use this page"
    Scan the **Symptom** and **`EXPLAIN` signature** columns for what you're seeing, then
    follow the **Fix** and the linked problem page. Rows are grouped
    correctness-first — resolve correctness before performance.

---

## :material-table: Master Matrix

### :material-shield-check: Correctness / Data-Quality

| Problem | Symptom | `EXPLAIN` signature | Root cause | Fix | Production example |
|---------|---------|---------------------|------------|-----|--------------------|
| [Duplicate key](data_explosion.md) | Row count increases | Normal join | Non-unique key on one side | Deduplicate / pre-aggregate | Fact → dimension |
| [Many-to-many explosion](duplicate_key_explosion.md) | Row count multiplies (`N x M`) | Normal join | Duplicate keys on **both** sides | Dedup/aggregate **both** sides | Clicks × purchases |
| [NULL join keys](null_key_trap.md) | Rows silently don't match | Normal join | `NULL = NULL` is `UNKNOWN` | Null-safe `<=>` or `COALESCE` sentinel | Customer matching |
| [Data-type mismatch](type_mismatch.md) | `CAST_INVALID_INPUT`, or dropped rows | Cast on a key in the plan | Implicit cast fails/`NULL`s under ANSI | `TRY_TO_DATE`/`TRY_CAST`, quarantine | CSV date ingestion |
| [Case / whitespace mismatch](case_whitespace_mismatch.md) | Keys that "look" equal don't match | `Filter`/expr on key | Byte-level inequality | `LOWER(TRIM())` both sides | Email dedup |
| [Floating-point keys](floating_point_join.md) | `DOUBLE` keys never match | Normal join | Binary FP imprecision | Tolerance / `ROUND()` join | Sensor value match |
| [Incomplete composite key](incomplete_join_conditions.md) | Wrong / duplicated matches | Row count > fact side | Missing discriminator predicate | Add the missing key/range term | Versioned price lookup |
| [ON vs WHERE](predicate_vs_filter.md) | Different results by clause | Filter above vs. in join | Post-join filter on nullable side | Put predicate in `ON` (outer joins) | — |
| [WHERE after LEFT JOIN](outer_join_filter_pitfall.md) | Missing left rows | Still shows `LeftOuter`… but filtered to inner | Predicate eliminates the `NULL` side | Move predicate into `ON` | Active orders report |
| [Accidental cross join](cartesian_join.md) | Massive output; job hangs | `CartesianProduct` | Missing join predicate | Add an `ON` clause | Accidental cross join |
| [Functions on join keys](functions_on_join_keys.md) | False matches, or slow full scan | Function wraps key; no partition prune | Non-injective fn / lost pushdown | Keep keys raw; materialize normalized key | Account-code strip |
| [Duplicate columns](column_duplicate.md) | `AnalysisException: ambiguous` | Fails at analysis | Same column name both sides | Aliases / explicit column list | Star-select join |

### :material-history: Advanced / Temporal Joins

| Problem | Symptom | `EXPLAIN` signature | Root cause | Fix | Production example |
|---------|---------|---------------------|------------|-----|--------------------|
| [SCD Type 2 multiple matches](scd_type2_join.md) | Fact rows duplicated | Range/`BETWEEN` join | Overlapping validity intervals | Half-open `[valid_from, valid_to)` | Customer tier history |
| [SCD2 as-is vs as-was](scd_type2_join.md) | Current attribute on old facts | Join on `is_current` | Wrong version selected | Point-in-time interval join | Effective-dated pricing |
| [Non-equi / range join](range_join_pitfalls.md) | Quadratic slowdown | `BroadcastNestedLoopJoin` / `CartesianProduct` | No equality component | Add an equi-key; bound both ends | Event-in-window |
| [Overlapping intervals](range_join_pitfalls.md) | Over-matching rows | Nested-loop range join | One-sided / inclusive bounds | Half-open bounds + equi-key | Session windows |

### :material-speedometer: Performance / Execution

| Problem | Symptom | `EXPLAIN` signature | Root cause | Fix | Production example |
|---------|---------|---------------------|------------|-----|--------------------|
| [Data skew](skewed_keys.md) | One task very slow | `SortMergeJoin` + skewed partition | Hot key | AQE skew-join / salting | Tenant workload |
| [Broadcast OOM](broadcast_pitfalls.md) | Executor / driver failure | `BroadcastHashJoin` | Broadcast side too large | Drop hint / raise resources / `-1` threshold | Oversized dimension |
| [Huge shuffle](partition_count.md) | Long single stage | `Exchange hashpartitioning` | Large ⋈ large | Broadcast / repartition / pre-aggregate | Fact ⋈ fact |
| [Too many/few partitions](partition_count.md) | Tiny files, or spill/OOM | 200-task shuffle stage | Fixed `shuffle.partitions=200` | AQE coalesce / tune count | Small ⋈ small, or 200 GB shuffle |
| [Bad join order](join_order_optimizer.md) | Giant intermediate results | `EXPLAIN COST` has no `rowCount` | CBO/stats off | Enable CBO + `ANALYZE TABLE` | Multi-table star |
| [Unexpected AQE strategy](join_order_optimizer.md) | Plan differs from expectation | `AdaptiveSparkPlan isFinalPlan=…` | Runtime re-optimization | Tune AQE / advisory size | Large Delta tables |

---

## :material-cog-transfer: Physical Join Strategies

The **strategy** is the single most important thing to read from `EXPLAIN` (step 10 of the
[workflow](debugging_workflow.md)). All signatures below are Spark-verified.

| Strategy | `EXPLAIN` line | When Spark picks it | Force with hint |
|----------|----------------|---------------------|-----------------|
| Broadcast hash join | `BroadcastHashJoin ... BuildRight` | One side < `autoBroadcastJoinThreshold` (10 MB) | `/*+ BROADCAST(t) */` |
| Sort-merge join | `SortMergeJoin [k], [k]` | Two large tables, equi-join | `/*+ MERGE(t) */` |
| Shuffled hash join | `ShuffledHashJoin ... BuildRight` | Medium side, `preferSortMergeJoin=false` | `/*+ SHUFFLE_HASH(t) */` |
| Broadcast nested loop | `BroadcastNestedLoopJoin` | Non-equi join, one side small | — |
| Cartesian product | `CartesianProduct` | Non-equi join, neither side small; or missing `ON` | `/*+ SHUFFLE_REPLICATE_NL(t) */` |

!!! warning "A nested-loop or Cartesian strategy on a real join is almost always a bug"
    `BroadcastNestedLoopJoin` and `CartesianProduct` are `O(N x M)`. Seeing either on a
    join you expected to be keyed means a missing equality — see
    [Accidental Cross Join](cartesian_join.md) (no `ON`) or
    [Range / Non-Equi Join](range_join_pitfalls.md) (range-only condition).

### :material-magnify: `EXPLAIN` Signature Cheat Sheet

Beyond the join operator itself, these plan tokens tell you *how* data moves. The
`AdaptiveSparkPlan` / `*QueryStage` / `AQEShuffleRead` nodes appear only under AQE (on by
default in Spark 3.x+) and reflect **runtime** re-optimization — all Spark-verified.

| `EXPLAIN` token | Meaning | What to investigate |
|-----------------|---------|---------------------|
| `BroadcastHashJoin` | Broadcast-based equality join | Broadcast side size / OOM risk |
| `BroadcastExchange` | A side is being broadcast | *Which* side, and how big |
| `SortMergeJoin` | Shuffle + sort equi-join | Shuffle volume, skew |
| `ShuffledHashJoin` | Shuffle hash join | Build-side memory, partitioning |
| `BroadcastNestedLoopJoin` | Nested loop with broadcast | Non-equi / cross-like join |
| `CartesianProduct` | Full cross product | Missing join predicate |
| `LeftSemi` / `LeftAnti` | Existence / non-existence join | Usually expected — verify intent |
| `Exchange hashpartitioning(k, N)` | Shuffle on key `k` into `N` partitions | Volume; `N` vs data size |
| `Sort` | Sort before a merge join | Sort cost / spill |
| `AdaptiveSparkPlan isFinalPlan=true` | AQE finished re-optimizing at runtime | Plan may differ from the initial one |
| `ShuffleQueryStage` / `BroadcastQueryStage` | An AQE stage materialized at runtime | Dynamic strategy / broadcast switch |
| `AQEShuffleRead coalesced` / `skewed` | AQE coalesced small partitions or split a skewed one | Partition count ([count](partition_count.md)) / skew ([skew](skewed_keys.md)) |

---

## :material-database-cog: Databricks / Photon Considerations

The rows above target open-source Spark 4. On Databricks Runtime, these extras affect join
behavior:

- **[Databricks] Photon** — the vectorized engine accelerates `BroadcastHashJoin`,
  `SortMergeJoin`, and `ShuffledHashJoin`, but falls back to non-Photon for unsupported
  expressions (check for `PhotonBroadcastHashJoin` vs a plain `BroadcastHashJoin` in the
  plan). A function-wrapped join key ([Functions on Join Keys](functions_on_join_keys.md))
  can cause a Photon fallback and a silent slowdown.
- **[Databricks] Delta statistics** — `ANALYZE TABLE ... COMPUTE STATISTICS` and
  per-column stats feed CBO join ordering ([Join Order / Optimizer](join_order_optimizer.md));
  Delta also keeps file-level min/max stats used for data skipping on join keys.
- **[Databricks] `OPTIMIZE ... ZORDER BY (join_key)`** — co-locates rows by the join key so
  probe-side reads skip more files; run after bulk loads / SCD merges.
- **[Databricks] Liquid clustering** (`CLUSTER BY`) — an alternative to Z-Ordering that
  keeps frequently-joined keys clustered without manual re-`OPTIMIZE` tuning.
- **[Databricks] SQL Warehouse** — enables Photon and AQE by default; broadcast/skew
  behavior may differ from a self-managed cluster, so re-validate plans per environment.

```sql
-- [Databricks] Cluster a fact table by its join key so joins skip more files
OPTIMIZE fact_orders ZORDER BY (customer_id);

-- [Databricks] Or use liquid clustering at table creation
CREATE TABLE fact_orders (...) CLUSTER BY (customer_id);
```

---

## :material-stethoscope: Diagnostic Playbook

| Check | Command / signal |
|-------|------------------|
| NULL & duplicate keys | `COUNT(*)`, `COUNT(key)`, `COUNT(DISTINCT key)` — see [workflow §Diagnostic Queries](debugging_workflow.md) |
| Cardinality | Compare join output rows to the "many" side's `COUNT(*)` |
| Physical plan | `EXPLAIN FORMATTED` — numbered operator tree + per-node detail |
| Join strategy | The join operator name (table above) |
| Shuffle | `Exchange hashpartitioning` node; task count in the Spark UI stage |
| Broadcast | `BroadcastExchange` node; broadcast size in the SQL query profile |
| Skew | Spark UI: one task's duration/shuffle-read far exceeds the median |
| AQE active | `AdaptiveSparkPlan` at the plan root; "final plan" differs from initial |
| Before/after validation | Re-run `EXPLAIN` and compare stage metrics after each change |

!!! danger "Validate correctness before *and* after optimizing"
    Every performance fix (broadcast, repartition, salting, Z-Order) changes *how* a join
    runs, not *what* it should return. Capture the correct row count and a checksum
    (`SELECT COUNT(*), SUM(hash(...))`) before tuning, and re-check it after — an
    "optimization" that changes results is a regression, not a win. Full ordered
    procedure: [Join Debugging Workflow](debugging_workflow.md).
