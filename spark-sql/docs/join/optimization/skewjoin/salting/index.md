# Handling Skewed Joins in Spark SQL: Salting Techniques

Learn how to fix skewed joins in Spark SQL using various salting strategies, complete with practical, end-to-end examples.

---

## 🚩 The Problem: Skewed Joins

Suppose you want to join a large `transactions` table with a medium-sized `customers` table:

```sql
SELECT t.txn_id, c.customer_name
FROM transactions t
JOIN customers c
    ON t.customer_id = c.id;
```

If `customer_id = 12345` appears **50 million times** (while others appear ~1,000 times), Spark will put all 50M rows into a single partition—causing severe skew and performance issues.

---

## 💡 Salting Techniques in Spark SQL

### 1. **Random Salting (Basic Hash Salting)**

**Goal:** Break up skewed keys by adding a random suffix.

#### Step 1: Salt the skewed side

```sql
WITH salted_txn AS (
    SELECT t.*, CONCAT(customer_id, '_', CAST(FLOOR(RAND() * 10) AS INT)) AS salted_id
    FROM transactions t
)
```

#### Step 2: Expand the smaller table

```sql
WITH expanded_cust AS (
    SELECT c.*, CONCAT(id, '_', salt) AS salted_id
    FROM customers c
    LATERAL VIEW posexplode(split(repeat(',', 9), ',')) AS salt_pos, salt
)
```

#### Step 3: Join on salted keys

```sql
SELECT st.txn_id, ec.customer_name
FROM salted_txn st
JOIN expanded_cust ec
    ON st.salted_id = ec.salted_id;
```

> **Result:** Large skewed keys are distributed across 10 partitions.

---

### 2. **Deterministic Salting (Modulo Hashing)**

**Goal:** Use a reproducible salt for stability.

```sql
WITH salted_txn AS (
    SELECT t.*, CONCAT(customer_id, '_', pmod(hash(txn_id), 10)) AS salted_id
    FROM transactions t
),
expanded_cust AS (
    SELECT c.*, CONCAT(id, '_', n) AS salted_id
    FROM customers c
    LATERAL VIEW posexplode(split(repeat(',', 9), ',')) AS n, dummy
)
SELECT st.txn_id, ec.customer_name
FROM salted_txn st
JOIN expanded_cust ec
    ON st.salted_id = ec.salted_id;
```

> **Result:** More stable and reproducible than random salting.

---

### 3. **Selective Salting (Skewed Keys Only)**

**Goal:** Salt only the problematic keys, leaving others untouched.

```sql
WITH salted_txn AS (
    SELECT t.*,
                 CASE WHEN customer_id = 12345
                            THEN CONCAT(customer_id, '_', CAST(FLOOR(RAND() * 10) AS INT))
                            ELSE CAST(customer_id AS STRING)
                    END AS salted_id
    FROM transactions t
),
expanded_cust AS (
    SELECT c.*,
                 CASE WHEN id = 12345
                            THEN CONCAT(id, '_', n)
                            ELSE CAST(id AS STRING)
                    END AS salted_id
    FROM customers c
    LATERAL VIEW posexplode(split(repeat(',', 9), ',')) AS n, dummy
)
SELECT st.txn_id, ec.customer_name
FROM salted_txn st
JOIN expanded_cust ec
    ON st.salted_id = ec.salted_id;
```

> **Result:** Only skewed keys are split, minimizing overhead.

---

### 4. **Hybrid: Salting + Broadcast Join**

**Goal:** If the smaller side fits in memory, combine salting with broadcasting for maximum performance.

```sql
SELECT /*+ BROADCAST(expanded_cust) */ st.txn_id, ec.customer_name
FROM salted_txn st
JOIN expanded_cust ec
    ON st.salted_id = ec.salted_id;
```

---

## 📝 Summary Table

| Technique           | Use Case              | Pros                   | Cons                        |
|---------------------|----------------------|------------------------|-----------------------------|
| Random Salting      | General skew         | Simple, quick          | Non-deterministic           |
| Modulo Hashing      | Stable partitioning  | Deterministic          | May not balance evenly      |
| Selective Salting   | Few known skew keys  | Efficient              | Needs prior skew detection  |
| Hybrid (Broadcast)  | One side small       | Fast, avoids shuffle   | Only if small fits in memory|

---

## ✅ End-to-End Approach

1. **Detect skewed keys:**  

     ```sql
     SELECT customer_id, COUNT(*) FROM transactions GROUP BY customer_id;
     ```

2. **Choose a salting method:**  
     (Random, modulo, selective, or hybrid)
3. **Apply salting:**  
     Salt the large table, expand the smaller one.
4. **Join on salted keys.**
5. **(Optional) Re-aggregate** on the original key if needed.

---

> **Tip:** Always analyze your data distribution before choosing a salting strategy!
