# Left semi join

A left semi join returns rows from the left DataFrame that have at least one matching row in the right DataFrame — but it doesn’t return any columns from the right DataFrame.

It’s equivalent to saying:

`Give me all rows from A that exist in B, based on the join condition.`

## Use case

Use Case                     | Why Semi Join?
-----------------------------|----------------------------------
Existence filtering          | Return only rows that match
Faster than inner join       | Doesn't return right-side columns
Efficient replacement for IN | Scalable, no duplicates
Joins in subquery filters    | Often used by Spark internally

## 🔁 Semi Join Flow

1. Spark evaluates the join condition.
2. If at least one match is found, the row from the left DataFrame is retained.
3. Right-side columns are discarded.

## Flow

```{mermaid}
flowchart TD
    A["Left Table (L)"] --> J{Does L.id match R.id?}
    R["Right Table (R)"] --> J

    J -- Yes --> D1[Keep row from L]
    J -- No --> D2[Discard row from L]

    D1 --> O[Final Semi Join Output]
```

## Spark Execution Strategy

Condition        | Join Strategy
-----------------|--------------------
Join keys + equi | Broadcast Hash Join
Large data       | Sort-Merge Join
No join key      | Nested Loop Join

## SQL

All orders that have a valid customer.

```sql
SELECT *
FROM orders o
LEFT SEMI JOIN customers c
ON o.customer_id = c.id
```

## SQL Equivalent

```sql
SELECT *
FROM orders
WHERE customer_id IN (
  SELECT id FROM customers
)
```

⚠️ But again, IN can be less efficient and buggy with nulls — LEFT SEMI JOIN is safer and faster.

## Semi vs Anti vs Inner Join

Join Type | Left Only | Right Only | Matching | Output Columns
----------|-----------|------------|----------|--------------------------
Inner     | ❌         | ❌          | ✅        | Left + Right
Left Semi | ✅         | ❌          | ✅        | Left only
Left Anti | ✅         | ❌          | ❌        | Left only (when no match)
