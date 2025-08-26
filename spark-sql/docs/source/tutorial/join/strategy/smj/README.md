# Sort Merge Join

Sort Merge Join (SMJ) is the default join strategy used by Apache Spark when joining DataFrames, especially for large datasets where broadcast joins are not feasible.

## How Sort Merge Join Works

The process involves two main steps:

1. **Shuffle and Sort Phase**  
    - Spark shuffles the data across the network so that rows with the same join key end up in the same partition.
    - Each partition is then sorted by the join key.
    - After this phase, corresponding partitions from both DataFrames contain rows with matching keys, ready for merging.

2. **Merge Phase**  
    - Spark performs an efficient merge operation on the sorted partitions to produce the final joined result.

## Configuration

To force Spark to use Sort Merge Join (and disable broadcast joins), set the following configuration:

```shell
spark.sql.autoBroadcastJoinThreshold=-1
```

## Sort Merge Join Diagram

```{mermaid}
flowchart TD
    subgraph Executor1
        A1[DataFrame A<br>Partition 1] --> S1[Shuffle & Sort by Key]
        B1[DataFrame B<br>Partition 1] --> S2[Shuffle & Sort by Key]
        S1 --> M1[Merge Phase<br>Join on Key]
        S2 --> M1
        M1 --> R1[Joined Result Partition 1]
    end

    subgraph Executor2
        A2[DataFrame A<br>Partition 2] --> S3[Shuffle & Sort by Key]
        B2[DataFrame B<br>Partition 2] --> S4[Shuffle & Sort by Key]
        S3 --> M2[Merge Phase<br>Join on Key]
        S4 --> M2
        M2 --> R2[Joined Result Partition 2]
    end

    R1 -.-> F[Final Joined Result]
    R2 -.-> F
```

## When to Use Sort Merge Join

- Suitable for large tables that cannot be broadcasted.
- Efficient when join keys are well-distributed and data is already partitioned or sorted.

---

**Tip:** For optimal performance, ensure your join keys are evenly distributed to avoid data skew during the shuffle phase.
