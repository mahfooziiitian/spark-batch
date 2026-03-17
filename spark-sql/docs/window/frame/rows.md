# ROWS Window Frame

`ROWS` defines a frame based on physical row offsets around the current row.
It is used inside `OVER (...)` window definitions.

---

## 📌 Syntax

```sql
ROWS BETWEEN n PRECEDING AND m FOLLOWING
```

---

## 🧪 Example

```sql
SELECT order_id, amount,
       SUM(amount) OVER (
         PARTITION BY customer_id
         ORDER BY order_date
         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS rolling_sum
FROM orders;
```

---

## 🔍 Behavior

1. `ROWS` counts physical rows, not values.
2. It differs from `RANGE`, which uses value ranges.

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Rolling window by row count | Use `ROWS` |
| Value-based frames | Use `RANGE` |
