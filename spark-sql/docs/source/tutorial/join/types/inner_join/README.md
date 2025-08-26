# Inner Join

An **inner join** returns only the rows where the join condition matches in *both* tables. It's the default and most widely used join type in SQL and Spark.

---

## 🚀 Why Use Inner Joins?

| Use Case                   | Benefit                                      | Example                        |
|----------------------------|----------------------------------------------|--------------------------------|
| Combine related data       | Merge datasets with shared keys              | Orders + Customers             |
| Data integrity enforcement | Ensures referenced keys exist in both tables | Foreign key checks             |
| Filter matching rows       | Efficiently narrows data to relevant matches | Only customers with purchases  |

---

## ⚡ Spark Join Strategies for Inner Join

Depending on data size and configuration, Spark automatically selects the most efficient join strategy:

| Join Strategy               | When Spark Chooses It                                             |
|-----------------------------|-------------------------------------------------------------------|
| **Broadcast Hash Join**     | One side fits `autoBroadcastJoinThreshold`                        |
| **Sort-Merge Join**         | Both sides are large, join keys are sortable                      |
| **Shuffle Hash Join**       | `preferSortMergeJoin` is disabled and enough memory is available  |
| **Broadcast Nested Loop**   | Non-equi joins (e.g., `<`, `<>`, `!=`, etc.)                     |

---

## 🔄 Join Strategy Flow

```{mermaid}
flowchart TD
    A[Start Inner Join] --> B{Is join condition equi-join?}
    B -- No --> C[Use Broadcast Nested Loop Join]
    B -- Yes --> D{Can one side be broadcasted?}
    D -- Yes --> E[Broadcast Hash Join]
    D -- No --> F{Is Sort-Merge Join enabled?}
    F -- Yes --> G[Sort-Merge Join]
    F -- No --> H{Enough memory for Hash Join?}
    H -- Yes --> I[Shuffle Hash Join]
    H -- No --> J[Fallback to Sort-Merge Join]
```

---

## 💡 Tips for Efficient Inner Joins

| Tip                                   | Why It Helps                                      |
|----------------------------------------|---------------------------------------------------|
| **Broadcast small tables**             | Avoids expensive shuffles                         |
| **Repartition on join keys**           | Ensures data co-location for faster joins         |
| **Use `JOIN ... USING` if keys match** | Cleaner syntax, avoids duplicate columns          |
| **Avoid skewed join keys**             | Prevents out-of-memory and performance bottlenecks|

---

## ⚠️ Common Pitfalls & Solutions

| Issue                  | Solution                                  |
|------------------------|-------------------------------------------|
| Data skew on join keys | Use salting or Spark's skew join hints    |
| Nulls in join keys     | Inner join skips those rows automatically |
| Over-partitioned data  | Tune partitions to avoid tiny tasks       |

---

> **Pro Tip:**  
> Always analyze your data distribution and table sizes before joining. Use `EXPLAIN` to see Spark's chosen strategy!

