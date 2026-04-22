# :material-cog-transfer: Broadcast Nested Loop Join (BNLJ)?

This join type:

1. Is used when no join keys are present or when Spark cannot use hash/sort joins.
2. Broadcasts the smaller DataFrame (typically the left one) to all worker nodes.
3. Each executor scans the larger DataFrame (right side) and applies the join logic for every row of the broadcasted small table — hence, a nested loop.


### :material-sitemap: Overview

```mermaid
graph LR
    D[Driver] -->|broadcast| E1[Executor 1]
    D -->|broadcast| E2[Executor 2]
    E1 -->|nested loop| L1[Large DF Part 1]
    E2 -->|nested loop| L2[Large DF Part 2]
```

## :material-cog-outline:️ When Spark Uses BNLJ

1. Cross joins (without predicates)
2. Join conditions with `non-equi logic` (e.g., <, !=, LIKE, BETWEEN)
3. Explicit use:  `JOIN HINT /*+ BROADCAST(df) */`

## Flowchart

```mermaid
flowchart TB

subgraph Driver
    smallDF[Small DataFrame]
end

subgraph WorkerNode1
    smallCopy1[Small DF Copy]
    largePart1[Large DF Partition 1]
end

subgraph WorkerNode2
    smallCopy2[Small DF Copy]
    largePart2[Large DF Partition 2]
end

smallDF -- Broadcast --> smallCopy1
smallDF -- Broadcast --> smallCopy2

smallCopy1 -- Nested Loop Join --> largePart1
smallCopy2 -- Nested Loop Join --> largePart2

largePart1 --> out1[Join Output 1]
largePart2 --> out2[Join Output 2]
```

`Explanation:`

1. Driver broadcasts the small DataFrame to all executors.
2. Each executor performs:

   - For each row in the broadcasted DataFrame:
     - For each row in the local partition of the large DataFrame:  
       - Evaluate the join condition (can be any expression, not just equality).
3. Matched pairs are returned as result.
