# Query Parsing & Execution

Spark SQL processes queries through a multi-stage pipeline: parsing → analysis → optimization
→ physical planning → execution.

## 📌 Execution Pipeline

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

## 🔍 Stage Details

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

## 🧪 Inspect the Query Plan

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

## 🧠 Key Concepts

| Concept | Description |
|---------|-------------|
| DataFrame | Abstraction over RDD with schema — optimized by Catalyst |
| Catalyst | Rule-based query optimizer |
| Tungsten | Memory management and code generation engine |
| Encoder | Serializes objects to optimized binary format |
| Whole-stage codegen | Compiles multiple operators into a single JVM function |
