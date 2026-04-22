# :material-auto-fix: AQE: Sort-Merge to Broadcast Join

With AQE enabled, Spark can replace a sort-merge join (SMJ) with a broadcast
hash join (BHJ) if runtime statistics show one side is small enough.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Sort Merge Join Planned] --> B{Runtime: small table?}
    B -->|Yes size < threshold| C[Switch to Broadcast Join]
    B -->|No| D[Keep Sort Merge Join]
```

---

## :material-pin: Why It Happens

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

## :material-magnify: Behavior Notes

1. AQE evaluates partition sizes after shuffle.
2. Broadcast joins are faster but require memory for the build side.
3. Verify the change with `EXPLAIN` or the Spark UI.

---

## :material-brain: When to Use

| Scenario | Recommendation |
|----------|----------------|
| Small dimension table | Allow broadcast conversion |
| Large sort cost | Enable AQE |
