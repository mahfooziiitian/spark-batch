# Logical Optimization

Spark SQL optimization, the standard `rule-based optimization` is applied to the `logical plan`.

It includes

1. constant folding
2. predicate pushdown
3. projection pruning
4. other rules

It became extremely easy to add a rule for various situations.

Once the AST representation is obtained, the Catalyst will transform that `AST` applying a `set of rules iteratively`, until no more rules can be applied.

The rule set has been defined by the programmers behind Spark, however it can be expanded as needed in a relatively easy fashion.

An example of rule that can be applied in this phase is the `filter pushdown`: if a filtering transformation is found it will be pushed as close to the data source as possible.

This way less data will be taken from `storage to memory` and, therefore, less data will have to be elaborated, lightening the workload.

An interesting thing to point out is how well this step and the lazy evaluation of transformations interact: were the evaluation eager some optimization opportunities might be missed.

Let's consider a query that joins two dataframes and then filters the result:

    dfA.join(dfB, dfA.field===dfB.field,"INNER").filter(A_filtering_field==True)

With an eager evaluation first the join would be executed, then the result will be filtered, therefore wasting resources on data that will be filtered out.

With a lazy evaluation, instead, data will first be filtered and then joined, thus saving resources.

Another interesting thing to point out is that with `parquet files` `the filter` can be pushed down to the data before it is even serialized into memory, thus reading only what satisfies the filter.
