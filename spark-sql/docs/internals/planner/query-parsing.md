# :material-map-legend: Query Parsing & Execution

Spark SQL processes queries through a multi-stage pipeline: parsing → analysis → optimization
→ physical planning → execution.

## :material-pin: Execution Pipeline

### :material-animation-play: Interactive Visualization — Step Through the Pipeline

<div id="viz-pipeline-stages" class="ts-viz"></div>

Click each stage to see what the plan looks like as `SELECT * FROM sales WHERE amount > 100`
moves from a raw string to executable bytecode.

```
SQL String
  │
  ▼
┌─────────────────┐
│  Parser (ANTLR)  │  SQL → Unresolved Logical Plan (AST)
└────────┬────────┘
         ▼
┌─────────────────┐
│    Analyzer      │  Resolve tables, columns, functions → Resolved Logical Plan
└────────┬────────┘
         ▼
┌─────────────────┐
│ Catalyst         │  Rule-based + cost-based optimization → Optimized Logical Plan
│ Optimizer        │  (predicate pushdown, constant folding, join reordering)
└────────┬────────┘
         ▼
┌─────────────────┐
│ Physical         │  Choose algorithms (sort merge join, broadcast hash join, etc.)
│ Planner          │  → Physical Plan
└────────┬────────┘
         ▼
┌─────────────────┐
│ Code Generation  │  Whole-stage codegen (Tungsten) → JVM bytecode
└────────┬────────┘
         ▼
     Execution
```

## :material-magnify: Stage Details

### 1. Parsing

The SQL string is parsed by an ANTLR-based parser into an **Abstract Syntax Tree (AST)** —
an unresolved logical plan where table and column names are just strings.

### 2. Analysis

The Analyzer resolves references by looking up the catalog:

- Table names → actual table metadata
- Column names → actual column positions and types
- Function names → registered function implementations

### 3. Catalyst Optimization

The rule-based optimizer applies transformations:

- **Predicate pushdown** — push WHERE conditions closer to the data source
- **Constant folding** — evaluate constant expressions at compile time
- **Column pruning** — read only needed columns
- **Join reordering** — choose optimal join order based on statistics

### 4. Physical Planning

Selects concrete algorithms for each operation:

- **Join strategies**: broadcast hash join, sort merge join, shuffle hash join
- **Aggregation**: hash-based or sort-based
- **Scan**: file scan, in-memory scan, push-down scan

### 5. Code Generation (Tungsten)

Whole-stage code generation compiles query stages into optimized JVM bytecode,
avoiding virtual method dispatch overhead and enabling CPU-efficient processing.

## :material-flask-outline: Inspect the Query Plan

```sql
-- Logical plan
EXPLAIN SELECT * FROM sales WHERE amount > 100;

-- Extended (logical + physical + optimized)
EXPLAIN EXTENDED SELECT * FROM sales WHERE amount > 100;

-- Formatted (readable tree)
EXPLAIN FORMATTED SELECT * FROM sales WHERE amount > 100;

-- Cost information
EXPLAIN COST SELECT * FROM sales WHERE amount > 100;

-- Codegen details
EXPLAIN CODEGEN SELECT * FROM sales WHERE amount > 100;
```

### :material-flask-outline: A Real `EXPLAIN EXTENDED` Trace

Running `EXPLAIN EXTENDED` on a filter over a view built from `sales(id, amount)` shows all
four stages named in the pipeline above, in order:

```text
== Parsed Logical Plan ==
'Project [*]
+- 'Filter ('amount > 100)
   +- 'UnresolvedRelation [sales], [], false

== Analyzed Logical Plan ==
id: bigint, amount: bigint
Project [id#0L, amount#1L]
+- Filter (amount#1L > cast(100 as bigint))
   +- SubqueryAlias sales
      +- View (`sales`, [id#0L, amount#1L])
         +- Project [id#0L, (id#0L * cast(10 as bigint)) AS amount#1L]
            +- Range (0, 10, step=1, splits=Some(1))

== Optimized Logical Plan ==
Project [id#0L, (id#0L * 10) AS amount#1L]
+- Filter ((id#0L * 10) > 100)
   +- Range (0, 10, step=1, splits=Some(1))

== Physical Plan ==
*(1) Project [id#0L, (id#0L * 10) AS amount#1L]
+- *(1) Filter ((id#0L * 10) > 100)
   +- *(1) Range (0, 10, step=1, splits=1)
```

Notice each stage's output shrinks and resolves further: the **Parsed** plan carries
`'`-prefixed unresolved nodes and no types; the **Analyzed** plan resolves `sales` to its
underlying view and infers `bigint` types (note the implicit `cast(100 as bigint)` the
Analyzer inserted so the literal matches `amount`'s type); the **Optimized** plan inlines
the view (`Range` replaces `SubqueryAlias sales`/`View`) and simplifies `id * cast(10 as bigint)` to `id * 10`; the **Physical** plan adds the `*(1)` whole-stage-codegen markers
around every operator, meaning `Project`, `Filter`, and `Range` are all fused into a single
generated function for stage `1`.

## :material-brain: Key Concepts

| Concept             | Description                                              |
| ------------------- | -------------------------------------------------------- |
| DataFrame           | Abstraction over RDD with schema — optimized by Catalyst |
| Catalyst            | Rule-based query optimizer                               |
| Tungsten            | Memory management and code generation engine             |
| Encoder             | Serializes objects to optimized binary format            |
| Whole-stage codegen | Compiles multiple operators into a single JVM function   |
