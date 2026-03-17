# User-Defined Functions (UDFs)

User-Defined Functions extend Spark SQL with **custom logic** written in Python, Scala, or Java.
They allow you to call arbitrary code from SQL queries when built-in functions are insufficient.

## 📌 Types of UDFs

| Type | Language | Input → Output | Performance | Use Case |
|------|----------|---------------|-------------|----------|
| **Scalar UDF** | Python / Scala / Java | Row → Single value | Moderate | Per-row transformations |
| **Pandas UDF** (Vectorized) | Python (Pandas) | Batch → Batch | Fast | Bulk numeric / ML operations |
| **UDAF** | Scala / Java | Group → Single value | Fast | Custom aggregations |
| **UDTF** | Python / Scala / Java | Row → Multiple rows | Moderate | Custom row generators |

## 🔍 How UDFs Work

1. **Register** a function with `spark.udf.register()` or `CREATE FUNCTION`.
2. **Call** it in SQL like any built-in function.
3. Spark serializes each row's data, sends it to the UDF, and deserializes the result.
4. Unlike SQL macros, UDFs are **opaque to the Catalyst optimizer** — no predicate pushdown or constant folding through UDFs.

## 🧪 Quick Examples

### Python Scalar UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def greet(name):
    return f"Hello, {name}!"

spark.udf.register("greet", greet)
```

```sql
SELECT greet('Alice');
-- Result: 'Hello, Alice!'
```

### SQL-Registered UDF

```sql
CREATE OR REPLACE TEMPORARY FUNCTION square AS 'com.example.SquareUDF';
SELECT square(5);
-- Result: 25
```

### Pandas UDF (Vectorized)

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf("double")
def pandas_double(s: pd.Series) -> pd.Series:
    return s * 2

spark.udf.register("pandas_double", pandas_double)
```

```sql
SELECT pandas_double(price) FROM products;
```

## 🧠 UDFs vs Alternatives

| Feature | UDF | SQL Macro | Built-in Function |
|---------|-----|-----------|-------------------|
| Custom logic | ✅ Any language | ❌ SQL expressions only | ❌ Fixed set |
| Performance | Slower (serialization) | Fast (inline) | Fastest (native) |
| Catalyst optimization | ❌ Opaque | ✅ Fully optimized | ✅ Fully optimized |
| Scope | Session or permanent | Session only | Always available |
| Complex logic | ✅ Loops, APIs, libraries | ❌ Pure expressions | ❌ Limited |

> **Rule of thumb:** Prefer built-in functions → SQL macros → Pandas UDFs → scalar UDFs.
> Only use scalar UDFs when no other option exists.

See the [UDF Guide](udf.md) for full syntax, registration patterns, and best practices.
