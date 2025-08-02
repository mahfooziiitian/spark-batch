# Skew salted sort merge

1. `Sort Merge` approach is very robust in handling Joins in case of resource constraints.
2. Extending the same, the salted version of `Sort Merge` can be used very effectively when one wants to join a large skewed dataset with a smaller non-skewed dataset but there are constraints on the executor's memory.
3. Additionally, the `Salted Sort Merge` version can also be used to perform Left Join of smaller non-skewed dataset with the larger skewed dataset which is not possible with `Broadcast Hash Join` even when the smaller dataset can be broadcasted to executors.
4. However, to make sure that Sort Merge Join is selected by the Spark, one has to turn off the `Broadcast Hash Join` approach.
5. This can be done by setting `spark.sql.autoBroadcastJoinThreshold=-1`.

The working of `Salted Sort Merge` Join is kind of similar to `Iterative Broadcast Hash` Join.

An additional column `salt key` is introduced in one of the skewed input dataset.

After this, for every record, a number is randomly assigned from a selected range of salt key values for the `salt key` column.

After salting the skewed input dataset, a loop is initiated on salt key values in the selected range.

For every salt key value being iterated in the loop, the salted input dataset is first filtered for the iterated salt key value, after filtration, the salted filtered input dataset is joined together with the other unsalted input dataset to produce a partial joined output. To produce the final joined output, all the partial joined outputs are combined together using the Union operator.

An alternative approach also exists for `Salted Sort Merge` approach.

In this, for every salt key value being iterated in the loop, the second non skewed input dataset is enriched with the current iterated salt key value by repeating the the same value in the new `salt` column to produce a partial salt enriched dataset.

All these partial enriched datasets are combined using the `Union` operator to produce a combined salt enriched dataset version of the second non-skewed dataset.

After this, the first skewed salted dataset is Joined with the second salt enriched dataset to produce the final joined output.

`Salted Sort Merge Join` cannot handle `Full Outer Join`.

Also, it cannot handle skewness on both the input dataset.

It can handle skew only in the left dataset in the Left Joins category (`Outer, Semi and Anti)`.

Similarly, it can handle skew only in the right dataset in the Right Joins category.
