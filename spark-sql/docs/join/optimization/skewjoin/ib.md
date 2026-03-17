# Iterative Broadcast Technique

The **Iterative Broadcast** technique is an adaptation of the `Broadcast Hash Join` designed to efficiently handle large, skewed datasets that cannot be broadcasted in their entirety due to executor memory constraints.

## When to Use

Use this technique when **neither input dataset can be fully broadcasted** to executors because of memory limitations. It is especially helpful when dealing with skewed data distributions.

## How It Works

1. **Chunking the Smaller Dataset:**  
    - The smaller input dataset is divided into multiple manageable chunks.
    - This is typically done by adding a new column, such as `chunkId`, and assigning each record a random chunk number (from 0 to N-1, where N is the desired number of chunks).

2. **Iterative Join Process:**  
    - For each chunk:
      - Filter the dataset to include only records belonging to the current `chunkId`.
      - Broadcast this chunk and perform a standard `Broadcast Hash Join` with the unbroken (larger) dataset.
      - Collect the partial join result.

3. **Combining Results:**  
    - After all chunks have been processed, combine the partial results using a `Union` operation to produce the final joined output.

## Example Workflow

```scala
// Pseudocode
for (chunkId <- 0 until numChunks) {
  val chunk = smallDataset.filter($"chunkId" === chunkId)
  val partialResult = chunk.join(largeDataset, joinCondition, "inner")
  results = results.union(partialResult)
}
```

## Limitations

- **Join Type:**  
  - Only supports **Inner Joins**.
  - Does **not** support Full Outer, Left, or Right Joins.

- **Skew Handling:**  
  - Can handle skewness on both input datasets for inner joins.

---

By breaking the smaller dataset into broadcastable chunks and joining iteratively, the Iterative Broadcast technique enables scalable joins on large, skewed datasets that would otherwise exceed memory limits.
