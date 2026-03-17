# UDF Guide

Complete reference for creating, registering, and using User-Defined Functions in Spark SQL.

---

## 📌 Python Scalar UDFs

### Registration via Decorator

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf(returnType=IntegerType())
def square(x):
    return x * x

spark.udf.register("square", square)
```

### Registration via Lambda

```python
from pyspark.sql.types import StringType

spark.udf.register("to_upper", lambda s: s.upper() if s else None, StringType())
```

### Usage in SQL

```sql
SELECT square(5) AS result;
-- Result: 25

SELECT name, to_upper(name) AS upper_name
FROM employees;
```

### Complex Return Types

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("first", StringType()),
    StructField("last", StringType())
])

@udf(returnType=schema)
def split_name(full_name):
    parts = full_name.split(" ", 1)
    return (parts[0], parts[1] if len(parts) > 1 else "")

spark.udf.register("split_name", split_name)
```

```sql
SELECT split_name('John Doe') AS name;
-- Result: {first: John, last: Doe}

SELECT split_name(full_name).first AS first_name FROM contacts;
```

### NULL Handling

```python
@udf(returnType=IntegerType())
def safe_length(s):
    return len(s) if s is not None else None

spark.udf.register("safe_length", safe_length)
```

```sql
SELECT safe_length('hello');  -- 5
SELECT safe_length(NULL);     -- NULL
```

> **Important:** Always handle `None` in Python UDFs — Spark passes NULL values as `None`.

---

## 📌 Pandas UDFs (Vectorized)

Pandas UDFs operate on **batches of rows** using Pandas Series/DataFrames, avoiding the
per-row serialization overhead of regular UDFs. They are 3–100x faster for numeric operations.

### Series → Series (Scalar)

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def double_price(prices: pd.Series) -> pd.Series:
    return prices * 2

spark.udf.register("double_price", double_price)
```

```sql
SELECT product, double_price(price) AS doubled FROM products;
```

### Series → Scalar (Grouped Aggregate)

```python
@pandas_udf("double")
def weighted_avg(values: pd.Series, weights: pd.Series) -> float:
    return (values * weights).sum() / weights.sum()

spark.udf.register("weighted_avg", weighted_avg)
```

```sql
SELECT category, weighted_avg(price, quantity) AS wavg
FROM sales
GROUP BY category;
```

### Iterator of Series (Batched)

```python
from typing import Iterator

@pandas_udf("string")
def batch_classify(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for batch in batches:
        yield batch.apply(lambda x: "high" if x > 100 else "low")

spark.udf.register("classify", batch_classify)
```

```sql
SELECT amount, classify(amount) AS category FROM transactions;
```

---

## 📌 Scala / Java UDFs

### Scala UDF Registration

```scala
spark.udf.register("cube", (x: Int) => x * x * x)
```

```sql
SELECT cube(3);
-- Result: 27
```

### Java UDF Class

```java
import org.apache.spark.sql.api.java.UDF1;

public class SquareUDF implements UDF1<Integer, Integer> {
    @Override
    public Integer call(Integer x) {
        return x * x;
    }
}
```

```sql
CREATE OR REPLACE TEMPORARY FUNCTION square AS 'com.example.SquareUDF';
SELECT square(5);
-- Result: 25
```

---

## 📌 User-Defined Aggregate Functions (UDAFs)

UDAFs compute a single result from a group of rows — like built-in `SUM` or `AVG` but with
custom logic.

### Python (Pandas Grouped Aggregate)

```python
@pandas_udf("double")
def geometric_mean(values: pd.Series) -> float:
    import numpy as np
    return np.exp(np.log(values).mean())

spark.udf.register("geometric_mean", geometric_mean)
```

```sql
SELECT category, geometric_mean(price) AS geo_mean
FROM products
GROUP BY category;
```

### Scala (Aggregator API)

```scala
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

case class Average(sum: Double, count: Long)

object CustomAvg extends Aggregator[Double, Average, Double] {
  def zero: Average = Average(0.0, 0L)
  def reduce(buf: Average, input: Double): Average =
    Average(buf.sum + input, buf.count + 1)
  def merge(b1: Average, b2: Average): Average =
    Average(b1.sum + b2.sum, b1.count + b2.count)
  def finish(buf: Average): Double = buf.sum / buf.count
  def bufferEncoder: Encoder[Average] = Encoders.product
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

spark.udf.register("custom_avg", functions.udaf(CustomAvg))
```

```sql
SELECT department, custom_avg(salary) FROM employees GROUP BY department;
```

---

## 📌 SQL CREATE FUNCTION

### Temporary Function (Session-Scoped)

```sql
CREATE OR REPLACE TEMPORARY FUNCTION my_func AS 'com.example.MyUDF';
```

### Permanent Function (Catalog-Scoped)

```sql
CREATE OR REPLACE FUNCTION my_catalog.my_schema.my_func AS 'com.example.MyUDF'
USING JAR 'hdfs:///libs/my-udfs.jar';
```

### Drop Function

```sql
DROP TEMPORARY FUNCTION IF EXISTS my_func;
DROP FUNCTION IF EXISTS my_catalog.my_schema.my_func;
```

---

## ⚠️ Performance Considerations

| Factor | Impact | Mitigation |
|--------|--------|------------|
| Serialization overhead | Python UDFs serialize/deserialize every row | Use Pandas UDFs (vectorized) |
| Catalyst opacity | Optimizer cannot push predicates through UDFs | Filter before UDF call |
| NULL handling | Python receives `None`, Scala receives `null` | Always check for NULLs |
| Type conversion | Python ↔ JVM type marshalling adds latency | Use native types, avoid complex structs |
| Parallelism | UDFs run within Spark tasks | Avoid blocking I/O in UDFs |

### Performance Hierarchy (Fastest → Slowest)

```
Built-in Functions > SQL Macros > Pandas UDFs > Scala UDFs > Python Scalar UDFs
```

---

## 🧪 Common Patterns

### Pattern 1: Lookup / Enrichment

```python
# Broadcast a lookup dict, use in UDF
country_map = {"US": "United States", "UK": "United Kingdom", "IN": "India"}
broadcast_map = spark.sparkContext.broadcast(country_map)

@udf(returnType=StringType())
def country_name(code):
    return broadcast_map.value.get(code, "Unknown")

spark.udf.register("country_name", country_name)
```

```sql
SELECT country_code, country_name(country_code) AS name FROM customers;
```

### Pattern 2: Regex Extraction

```python
import re

@udf(returnType=StringType())
def extract_domain(email):
    if email is None:
        return None
    match = re.search(r'@(.+)', email)
    return match.group(1) if match else None

spark.udf.register("extract_domain", extract_domain)
```

```sql
SELECT email, extract_domain(email) AS domain FROM users;
-- alice@gmail.com → gmail.com
```

### Pattern 3: Conditional Business Logic

```python
@udf(returnType=StringType())
def risk_tier(score):
    if score is None:
        return "Unknown"
    if score >= 800:
        return "Low"
    if score >= 650:
        return "Medium"
    return "High"

spark.udf.register("risk_tier", risk_tier)
```

```sql
SELECT customer_id, credit_score, risk_tier(credit_score) AS tier FROM accounts;
```

---

## 🧠 When to Use UDFs

| Scenario | Recommended Approach |
|----------|---------------------|
| Simple math / string ops | ❌ Use built-in functions |
| Reusable SQL expressions | ❌ Use SQL macros |
| Numeric batch operations | ✅ Pandas UDF |
| Complex business logic | ✅ Scalar UDF |
| External API calls / lookups | ✅ Scalar UDF with broadcast |
| Custom aggregation | ✅ Pandas grouped UDF or Scala UDAF |
| Row generation (1→N rows) | ✅ UDTF |

> **Tip:** Always benchmark UDF vs built-in alternatives. A chain of built-in functions
> is almost always faster than a single UDF doing the same work.
