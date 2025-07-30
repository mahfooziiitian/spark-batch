# Csv

In Spark SQL, the from_csv function is used to parse a CSV-formatted string column into a structured column (typically a STRUCT). This is extremely useful when you have CSV data inside a column, not in a file.

## 📌 Purpose

Parses a CSV string column into a STRUCT using a defined schema.

## 🔧 Syntax

```sql
from_csv(csv_string_column, schema_string [, options])
```

1. csv_string_column: the column containing the CSV text.
2. schema_string: the schema to apply to the parsed output.
3. options (optional): parsing options (e.g. delimiter, quote).

## ✅ Example

### Parse CSV Column into Struct

```sql
SELECT from_csv('1, 0.8', 'a INT, b DOUBLE');
SELECT from_csv('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
```

```sql
CREATE OR REPLACE TEMP VIEW raw_data AS
SELECT 'Alice,30,New York' AS csv_col
UNION ALL
SELECT 'Bob,45,San Francisco';
SELECT from_csv(csv_col, 'name STRING, age INT, city STRING') AS parsed
FROM raw_data;
```

### 🔸 Access Struct Fields

```sql
SELECT
  parsed.name,
  parsed.age,
  parsed.city
FROM (
  SELECT from_csv(csv_col, 'name STRING, age INT, city STRING') AS parsed
  FROM raw_data
);
```

### 🧪 Example with Options

```sql
SELECT from_csv('Tom|25|LA', 'name STRING, age INT, city STRING', map('delimiter', '|')) AS parsed;
```

### 🧼 Handling Corrupt Rows

```sql
SELECT from_csv('bad,row,data', 'id INT, name STRING', map('mode', 'PERMISSIVE', 'columnNameOfCorruptRecord', '_corrupt_record')) AS parsed
```

## 🔍 Common from_csv Options

Option| Description
---|---
delimiter| Field delimiter (default ,)
quote |Quote character
escape |Escape character
multiLine| Whether fields can span multiple lines
mode |PERMISSIVE, DROPMALFORMED, FAILFAST
columnNameOfCorruptRecord| Capture corrupt record column

## 🆚 Difference vs read.csv

Feature| read.csv()| from_csv()
---|---|---
Reads CSV file| ✅ Yes| ❌ No
Parses CSV string| ❌ No |✅ Yes
Used in SQL |✅ Yes (USING csv) |✅ Yes (function)
Returns |DataFrame |STRUCT
