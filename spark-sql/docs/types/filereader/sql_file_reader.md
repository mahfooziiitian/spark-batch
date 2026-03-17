# SQL File Reader

Spark SQL can read files directly in SQL queries using the file format as a table name,
without needing to create a table or DataFrame first.

## 📌 Syntax

```sql
SELECT * FROM format.`path`
```

| Format | Syntax |
|--------|--------|
| Parquet | `SELECT * FROM parquet.\`/path/to/file.parquet\`` |
| CSV | `SELECT * FROM csv.\`/path/to/file.csv\`` |
| JSON | `SELECT * FROM json.\`/path/to/file.json\`` |
| ORC | `SELECT * FROM orc.\`/path/to/file.orc\`` |
| Text | `SELECT * FROM text.\`/path/to/file.txt\`` |

## 🔍 Behavior

1. The format name acts as a **virtual table** backed by the file.
2. Schema is **inferred** from the file (Parquet/ORC have embedded schema; CSV/JSON inferred).
3. Supports glob patterns: `parquet.\`/data/*.parquet\``.
4. Supports directory paths: reads all files in the directory.
5. Read-only — cannot INSERT into file-based references.

## 🧪 Practical Examples

### 🧱 1. Read Parquet Directly

```sql
SELECT * FROM parquet.`/data/sales/2024/`;
```

### 🧱 2. Read CSV with Schema

```sql
CREATE OR REPLACE TEMP VIEW sales
USING csv
OPTIONS (
  path '/data/sales.csv',
  header 'true',
  inferSchema 'true',
  delimiter ','
);

SELECT * FROM sales WHERE amount > 100;
```

### 🧱 3. Read JSON

```sql
SELECT * FROM json.`/data/events.json`;

-- With options
CREATE OR REPLACE TEMP VIEW events
USING json
OPTIONS (
  path '/data/events.json',
  multiLine 'true'
);
```

### 🧱 4. Read with Glob Pattern

```sql
SELECT * FROM parquet.`/data/logs/2024-01-*`;
```

### 🧱 5. Create Temp View from File

```sql
CREATE OR REPLACE TEMPORARY VIEW customers
USING parquet
OPTIONS (path '/data/customers.parquet');

SELECT * FROM customers WHERE country = 'US';
```

## 🧠 When to Use

| Scenario | Approach |
|----------|----------|
| Quick ad-hoc exploration | `SELECT * FROM format.\`path\`` |
| Repeated queries on same file | Create a temp view with `USING` |
| Production pipelines | Create managed/external table |
| Schema enforcement needed | Use `USING` with explicit schema |
