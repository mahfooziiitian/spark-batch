# Macro

In Databricks, `identifier()` is a macro-like helper function, typically used inside SQL cells with Python string interpolation (like f""" ... """) or templating engines (like Jinja, dbt, etc.).

In Databricks notebooks, when you're mixing Python + SQL, you might need to dynamically insert table or column names. To safely handle this, Databricks provides identifier() to:

1. Ensure correct quoting (especially for special characters, reserved keywords)
2. Avoid SQL injection
3. Maintain platform compatibility

## Databricks notebook Python cell

```python
table_name = "sales_data"

query = f"""
SELECT COUNT(*) AS total
FROM {identifier(table_name)}
"""

spark.sql(query).show()
```

## 🧱 Why use identifier()?

1. ✅ Avoid quoting errors for table/column names like order, select, etc.
2. ✅ Helps with dynamic SQL generation
3. ✅ Prevents SQL injection if user input is used
