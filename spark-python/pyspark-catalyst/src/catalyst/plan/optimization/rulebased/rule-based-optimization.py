"""
The rule-based optimizations include constant folding, project pruning, predicate pushdown, and others.
They are all based on the rule-based optimizer.
For example, during this optimization phase, the Catalyst may decide to move the filter condition before performing a
join.

The list of rule-based optimizations is defined in the org.apache.spark.sql.catalyst.optimizer.Optimizer.
"""
