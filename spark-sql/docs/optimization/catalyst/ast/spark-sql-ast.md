# :material-file-tree: Spark SQL AST

The Abstract Syntax Tree (AST) represents the parsed structure of a SQL query
before it becomes a logical plan.

### :material-sitemap: Overview

```mermaid
graph LR
    A[SQL String] --> B[ANTLR Parser]
    B --> C[Abstract Syntax Tree]
    C --> D[Logical Plan]
```

---

## :material-pin: Why It Matters

1. The parser produces an AST from the SQL string.
2. The AST is converted into a logical plan for optimization.
3. Understanding ASTs helps debug parser or analysis issues.

---

## :material-flask-outline: Example

```sql
EXPLAIN PARSED SELECT * FROM orders WHERE amount > 100;
```

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Parser errors | Inspect the parsed plan |
| SQL debugging | Use `EXPLAIN PARSED` |
