# Stack

Row Generator from Static Columns.

## 🧠 What is STACK() in Spark SQL?

STACK() is used to convert multiple columns into multiple rows — typically to unpivot wide data into a tall format.

## 📌 Syntax

```sql
STACK(n, col1, col2, ..., colN)
```

Where:

n = number of rows you want to generate

The remaining arguments are split across those rows (like a row-major layout)

### Use it with LATERAL VIEW in SQL

```sql
SELECT ...
FROM your_table
LATERAL VIEW STACK(...) AS ...
```

## 🧪 Examples & Use Cases

### 🔹 1. Unpivot Fixed Columns into Rows

#### 🔄 Unpivot Subjects using STACK

```sql
CREATE OR REPLACE TEMP VIEW scores AS
SELECT * FROM VALUES
  (1, 85, 90, 78),
  (2, 88, 76, 92)
AS scores(student_id, math, science, history);
SELECT student_id, subject, score
FROM scores
LATERAL VIEW STACK(3,
  'math', math,
  'science', science,
  'history', history
) AS subject, score;
```

## 🔹 2. Generate Static Rows

You can use STACK() even without a table to generate rows manually.

```sql
SELECT *
FROM (SELECT 1) AS dummy
LATERAL VIEW STACK(3,
  'A', 100,
  'B', 200,
  'C', 300
) AS label, value;
```

## 🔹 3. Use with Constants and Variables

```sql
CREATE OR REPLACE TEMP VIEW features AS
SELECT 
  101 AS id,
  TRUE AS feature_a,
  FALSE AS feature_b,
  TRUE AS feature_c;
SELECT id, feature_name, is_enabled
FROM features
LATERAL VIEW STACK(3,
  'feature_a', feature_a,
  'feature_b', feature_b,
  'feature_c', feature_c
) AS feature_name, is_enabled;

```

## 🔹 4. Create Pivot-Like Summary with Labels

```sql
CREATE OR REPLACE TEMP VIEW summary AS
SELECT 
    'Product A' AS name,
    1200 AS sales,
    300 AS profit;
SELECT name, metric, value
FROM summary
LATERAL VIEW STACK(2,
  'sales', sales,
  'profit', profit
) AS metric, value;
```

## 🔹 5. Use Without a Table (Manual Data Creation)

```sql
SELECT *
FROM (SELECT 1) AS dummy
LATERAL VIEW STACK(2,
  'apple', 50,
  'banana', 30
) AS fruit, quantity;
```

## ✅ Summary: Why Use STACK()

Use Case| Benefit
---|---
Unpivot fixed columns| Convert wide table to tall format
Generate synthetic rows| Define rows inline for small datasets
Create readable summaries| Label fields explicitly
Combine multiple columns into rows| Perfect for structured reporting
