---
applyTo: "src/**/*.py,examples/**/*.py"
---

# PySpark Conventions

## SparkSession

- Always create sessions with `master("local[*]")` for local examples:
  ```python
  spark = SparkSession.builder.master("local[*]").appName("my-app").getOrCreate()
  ```
- Give each script a **unique, descriptive** `appName`.
- Never hard-code cluster URLs — use config or environment variables for
  non-local deployments.

## DataFrame Creation from XML

- Store XML strings as a `StringType` column, then rename:
  ```python
  df = spark.createDataFrame(data, StringType()).withColumnRenamed("value", "data")
  ```
- For file-based XML, use `spark.read.text(paths=path, wholetext=True)` to
  load the entire file as a single row. The column is named `"value"`.

## XPath Functions (Spark SQL)

Prefer **Spark SQL** syntax over the DataFrame API for XPath operations.
Register the DataFrame as a temp view first:

```python
df.createOrReplaceTempView("xml_data")
spark.sql("SELECT xpath_string(data, 'Root/Child') AS child FROM xml_data")
```

### Available XPath Functions

| Function | Returns | Use Case |
| --- | --- | --- |
| `xpath_string(col, expr)` | `STRING` | Single text value |
| `xpath(col, expr)` | `ARRAY<STRING>` | Multiple text nodes |
| `xpath_boolean(col, expr)` | `BOOLEAN` | Conditional check |
| `xpath_int(col, expr)` | `INT` | Integer value |
| `xpath_long(col, expr)` | `LONG` | Long value |
| `xpath_float(col, expr)` | `FLOAT` | Float value |
| `xpath_double(col, expr)` | `DOUBLE` | Double value |
| `xpath_number(col, expr)` | `DOUBLE` | Numeric value |
| `xpath_short(col, expr)` | `SHORT` | Short integer |

### XPath Expression Rules

- **Strip namespace prefixes** — Spark ignores them automatically:
  ```sql
  -- XML: <ns0:Root xmlns:ns0="..."><Child>val</Child></ns0:Root>
  xpath_string(data, 'Root/Child')   -- ✅ correct
  xpath_string(data, 'ns0:Root/Child') -- ❌ wrong
  ```
- Use `[@attr=value]` predicates for indexed elements:
  ```sql
  xpath_string(data, 'Results/RST[@cxArrayIndex=1]/Score')
  ```
- Use `text()` in `xpath()` to get array of text nodes:
  ```sql
  xpath(data, 'Root/Items/Item/text()')
  ```

## SQL Style in spark.sql()

- Use **triple-quoted strings** for multi-line SQL.
- Use **CTEs** (`WITH ... AS`) for complex queries — avoid deeply nested subqueries.
- Alias all computed columns with `AS`.
- Use `CASE WHEN ... THEN ... ELSE ... END` for conditional logic.

## Common Pitfalls

- **`df.select("/xpath/expr")`** is a column reference, NOT XPath evaluation.
  Always use `spark.sql()` with `xpath_string()`.
- **`show(truncate=False)`** is for debugging only — don't leave it in
  production code.
- **Avoid `collect()` on large DataFrames** — it pulls all data to the driver.
  Use it only in tests or on small result sets.
