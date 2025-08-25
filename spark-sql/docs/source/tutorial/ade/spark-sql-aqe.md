# Dynamic Optimization — AQE

As of Spark 3.3.0, AQE has four main capabilities:

1. Automatically coalescing post-shuffle partitions
2. Converting Sort Merge Joins into Broadcast Hash Joins
3. Converting Sort Merge Joins into Shuffle Hash Joins (as the Shuffle Hash Join is disabled by default I will not talk about this property in this post)
4. Optimizing skewed joins

AQE converts `sort-merge join` to `broadcast hash join` when the runtime statistics of any join side is smaller than the adaptive broadcast hash join threshold.
This is not as efficient as planning a `broadcast hash join` in the first place, but it’s better than keep doing the `sort-merge join`,as we can save the sorting of both the join sides, and read shuffle files locally to save network traffic(if spark.sql.adaptive.localShuffleReader.enabled is true)

    spark.sql.adaptive.autoBroadcastJoinThreshold
    spark.sql.adaptive.localShuffleReader.enabled

The above is done by sending new data to the Driver during execution, specifically during shuffle phases, and reingaging the Catalyst

AQE does coalesce post shuffle partitions
AQE is able to recognize skewed joins and correct them when the join strategy is a Sort Merge Join
AQE can transform a Sort Merge Join into a Broadcast Hash join if and only if there is a shuffle phase between the filtering action and the subsequent join
