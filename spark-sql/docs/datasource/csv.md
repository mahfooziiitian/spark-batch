# CSV Data Source

Spark SQL can read and write CSV files using the built-in CSV data source.
Provide an explicit schema for best performance.

---

## 📌 Read CSV

```sql
CREATE OR REPLACE TEMP VIEW sales_csv
USING csv
OPTIONS (
  path 's3://data/sales/',
  header 'true',
  inferSchema 'false'
);
```

---

## 📌 Write CSV

```sql
CREATE TABLE sales_export
USING csv
AS SELECT * FROM sales;
```

---

## 🔍 Common Options

| Option | Description |
|--------|-------------|
| `header` | First row is header |
| `delimiter` | Column separator |
| `quote` | Quote character |
| `escape` | Escape character |
| `multiLine` | Allow multi-line rows |

---

## 🧠 When to Use

| Scenario | Recommendation |
|----------|----------------|
| Lightweight export | CSV |
| Production analytics | Prefer Parquet |
| Unknown schema | Use `inferSchema` sparingly |
