# Skew Join in Spark SQL

Skew join hints are **not required** in most cases.

> **Note:** Skew is automatically handled if both  
>
> - `Adaptive Query Execution (AQE)`  
> - `spark.sql.adaptive.skewJoin.enabled`  
> are enabled.

---

## Examples of Skew Join Hints

### 1. Table with Skew

```sql
SELECT /*+ SKEW('orders') */ *
FROM orders, customers
WHERE c_custId = o_custId
```

---

### 2. Subquery with Skew

```sql
SELECT /*+ SKEW('C1') */ *
FROM (
  SELECT * FROM customers WHERE c_custId < 100
) C1, orders
WHERE C1.c_custId = o_custId
```

---

## Configuring Skew Hints

You can specify relation names, column names, and skew values.

### Single Column

```sql
SELECT /*+ SKEW('orders', 'o_custId') */ *
FROM orders, customers
WHERE o_custId = c_custId
```

---

### Multiple Columns

```sql
SELECT /*+ SKEW('orders', ('o_custId', 'o_storeRegionId')) */ *
FROM orders, customers
WHERE o_custId = c_custId AND o_storeRegionId = c_regionId
```

---

### Single Column, Single Skew Value

```sql
SELECT /*+ SKEW('orders', 'o_custId', 0) */ *
FROM orders, customers
WHERE o_custId = c_custId
```

---

### Single Column, Multiple Skew Values

```sql
SELECT /*+ SKEW('orders', 'o_custId', (0, 1, 2)) */ *
FROM orders, customers
WHERE o_custId = c_custId
```

---

### Multiple Columns, Multiple Skew Values

```sql
SELECT /*+ SKEW('orders', ('o_custId', 'o_storeRegionId'), ((0, 1001), (1, 1002))) */ *
FROM orders, customers
WHERE o_custId = c_custId AND o_storeRegionId = c_regionId
```

---

> ℹ️ **Tip:**  
> Use skew hints only if you have disabled AQE or need fine-grained control over skew handling.
