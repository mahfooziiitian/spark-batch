# Physical Planning

There are about 500 lines of code in the physical planning rules.

In this phase, one or more physical plan is formed from the logical plan, using physical operator matches the  Spark execution engine.

And it selects the plan using the cost model.

It uses Cost-based optimization only to select join algorithms.

For small relation  SQL uses broadcast join, the framework supports broader use of cost-based optimization.

It can estimate the cost recursively for the whole tree using the rule.

`Rule-based physical optimization`, such as `pipelining projections` or `filters into one  Spark map Operation` is also carried out by the physical planner.

Apart from this, it can also push operations from the logical plan into data sources that support `predicate` or `projection pushdown`.

The physical optimization phase further optimizes the workload by taking the optimized AST as `input`, `generating multiple physical plans`, running them through a cost model an selecting the least expensive one.

On a practical level, this phase is mostly responsible for selecting the `join strategy` for each join present in the query.

As it can be seen in the image below, Spark uses a pretty complicated flowchart for selecting the join strategy.

For the sake of brevity, and given that we will consider them when talking about dynamic optimization, we will only discuss the Broadcast Hash Join and the Sort Merge Join strategies.
