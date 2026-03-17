# AQE: Sort-Merge to Broadcast Join

With AQE enabled, Spark can replace a sort-merge join (SMJ) with a broadcast
hash join (BHJ) if runtime statistics show one side is small enough.

---

## 📌 Why It Happens

| Condition | Result |
|-----------|--------|
| Build side becomes small | Spark switches to broadcast |
| Broadcast threshold met | Avoids shuffle and sorting |

---

## �� Example

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.autoBroadcastJoinThreshold = 104857600; -- 100MB
```

---

## 🔍 Behavior Notes

1. AQE evaluates partition sizes after shuffle.
2. Broadcast joins are faster but require memory for the build side.
3. Verify the change with `EXPLAIN` or the Spark UI.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Small dimension table | Allow broadcast conversion |
| Large sort cost | Enable AQE |
