# Spark SQL & Catalyst Optimizer

Spark SQL uses the Catalyst Optimizer for:

## Logical Optimization

Predicate pushdown, constant folding.

## Physical Optimization

Choosing the best execution plan (e.g., Sort-Merge Join vs. Broadcast Join).
