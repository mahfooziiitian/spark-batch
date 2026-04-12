# :material-auto-fix: AQE: Sort-Merge to Shuffled Hash Join

AQE can replace a sort-merge join (SMJ) with a shuffled hash join (SHJ)
when runtime statistics show it will be faster.

---

## 📌 Why It Happens

| Condition | Effect |
|-----------|--------|
| Small build side | SHJ can be cheaper than sorting |
| Reduced data size | AQE re-optimizes join choice |

---

## 🧪 Example

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.shuffle.targetPostShuffleInputSize = 67108864;
```

---

## 🔍 Behavior Notes

1. Spark compares estimated costs at runtime.
2. SHJ avoids sort cost but may use more memory.
3. Verify with `EXPLAIN` after enabling AQE.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Uncertain data sizes | Enable AQE |
| High sort overhead | Allow SHJ conversion |
