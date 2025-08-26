# Broadcast MapPartitions Join

The **Broadcast MapPartitions Join** is an effective technique to accelerate skewed joins between a large, skewed dataset and a smaller, non-skewed dataset. In this approach:

- The smaller dataset is **broadcasted** to all executors.
- The join logic is **manually implemented** within a `MapPartitions` transformation, which processes the larger, non-broadcasted dataset.

> **Note:**  
> While Broadcast MapPartitions Join supports all join types and can handle skew in either or both datasets, it does require substantial executor memory. This is necessary to:
>
> - Broadcast the smaller dataset.
> - Maintain intermediate in-memory collections for the manual join logic.

---

## Key Takeaways

- **Handles Data Skew:** Efficiently manages skewed joins, reducing stragglers and memory overruns.
- **Flexible:** Applicable to all join types and skew scenarios.
- **Memory Intensive:** Ensure your executors have enough memory to accommodate the broadcasted dataset and intermediate data structures.
