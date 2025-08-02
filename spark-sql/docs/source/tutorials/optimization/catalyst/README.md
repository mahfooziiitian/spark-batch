# Spark Catalyst

The Catalyst is responsible for optimizing SQL queries.

The optimization process goes through multiple steps:

1. Query abstraction into an AST
2. Logical optimization
3. Physical optimization
4. Code generation

```mermaid
flowchart TD
    subgraph catalyst
        SQLQuery[SQL Query] -->|r| UnresolvedLogicalPlan
        DataFrame -->|d| UnresolvedLogicalPlan

        UnresolvedLogicalPlan -->|r<br/>Catalog| LogicalPlan
        LogicalPlan -->|r<br/>Logical Plan Optimization| OptimizedLogicalPlan
        OptimizedLogicalPlan -->|d<br/>Physical Planning| PhysicalPlan

        PhysicalPlan -->|l<br/>Cost Based Model| SelectedPhysicalPlan
        SelectedPhysicalPlan -->|l<br/>Code generation| RDD
    end
```
