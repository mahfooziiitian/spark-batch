# PySpark function

In PySpark, functions play a crucial role in transforming and analyzing large-scale datasets efficiently.

## 1. Types of PySpark Functions

PySpark provides various types of functions:

### 1. Built-in Functions

PySpark has built-in functions in the pyspark.sql.functions module that help manipulate DataFrames. These functions can be categorized into:

#### Column Functions

Operations on individual columns (col(), expr(), when(), etc.).
String Functions: String operations (lower(), upper(), substring(), etc.).

#### Date/Time Functions

Date manipulations (current_date(), date_add(), date_format(), etc.).

#### Aggregate Functions

Aggregations (sum(), avg(), count(), etc.).

#### Window Functions

Analytical functions (row_number(), rank(), etc.).

### UDFs (User Defined Functions)

Custom functions applied to DataFrame columns.

## Performance Considerations

1. `Use Built-in Functions First`: PySpark’s built-in functions (col(), lower(), etc.) are faster than UDFs.
2. `Prefer Pandas UDFs Over Regular UDFs`: They process data in batches instead of row-by-row.
3. `Avoid Python Loops Inside UDFs`: UDFs should work on entire columns rather than iterating over rows.
4. `Use .cache() and .persist()`: If UDF calculations are expensive, cache results for reuse.
