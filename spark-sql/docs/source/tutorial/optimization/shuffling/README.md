# Data shuffling

## Strategies to Minimize Data Shuffling

### 1. `Co-located Joins`:

Prioritize joins where the join columns are already co-located (partitioned) on the same data blocks across all participating tables. This eliminates the need to shuffle data for the join operation.Example: If both customer and order tables are partitioned by customer ID, a join on customer ID can be performed efficiently without shuffling.

### 2. `Broadcast Joins`

For small tables used in many joins, consider using broadcast joins. Spark broadcasts the smaller table to all worker nodes, eliminating the need for individual nodes to fetch it during the join.This is efficient when the broadcast size is relatively small compared to the larger table.

### 3. `Bucketing:`

Use bucketing to distribute data across partitions based on a hash function applied to a specific join column. This ensures rows with the same join value end up in the same bucket, minimizing shuffling during joins.Bucketing is particularly effective for equi-joins (joins on equal values).

### 4. `Data Salting (Advanced)`

In some scenarios, data might be skewed, meaning certain values appear more frequently than others.

This can lead to uneven distribution during bucketing and still require `shuffling.Salting` involves adding a random value ("salt") to the join column before bucketing.

This helps distribute skewed data more evenly across buckets, reducing shuffling.
