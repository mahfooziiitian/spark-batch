# Data Types

Spark SQL has a rich type system covering primitives, datetime types, and complex
(nested) types. Every column in a DataFrame has a data type that determines how
values are stored, compared, and processed.

## 📌 Primitive Types

| Type | SQL Syntax | Description | Example |
|------|-----------|-------------|---------|
| `BOOLEAN` | `BOOLEAN` | True / false | `TRUE` |
| `TINYINT` | `TINYINT` / `BYTE` | 8-bit signed integer | `127` |
| `SMALLINT` | `SMALLINT` / `SHORT` | 16-bit signed integer | `32767` |
| `INT` | `INT` / `INTEGER` | 32-bit signed integer | `2147483647` |
| `BIGINT` | `BIGINT` / `LONG` | 64-bit signed integer | `9223372036854775807` |
| `FLOAT` | `FLOAT` / `REAL` | 32-bit floating point | `3.14` |
| `DOUBLE` | `DOUBLE` | 64-bit floating point | `3.14159265` |
| `DECIMAL` | `DECIMAL(p, s)` | Arbitrary precision | `DECIMAL(10, 2)` |
| `STRING` | `STRING` | Variable-length text | `'hello'` |
| `BINARY` | `BINARY` | Byte array | `X'48656C6C6F'` |

## 📌 DateTime Types

| Type | SQL Syntax | Description | Example |
|------|-----------|-------------|---------|
| `DATE` | `DATE` | Calendar date (no time) | `DATE '2024-01-15'` |
| `TIMESTAMP` | `TIMESTAMP` | Date + time (session timezone) | `TIMESTAMP '2024-01-15 10:30:00'` |
| `TIMESTAMP_NTZ` | `TIMESTAMP_NTZ` | Date + time (no timezone) | `TIMESTAMP_NTZ '2024-01-15 10:30:00'` |
| `INTERVAL` | `INTERVAL` | Duration | `INTERVAL '1' DAY` |

See [DateTime](datetime/index.md) for detailed datetime functions and formatting.

## 📌 Complex Types

| Type | SQL Syntax | Description |
|------|-----------|-------------|
| `ARRAY` | `ARRAY<element_type>` | Ordered collection of elements |
| `MAP` | `MAP<key_type, value_type>` | Key-value pairs |
| `STRUCT` | `STRUCT<field: type, ...>` | Named fields (like a row) |

See [Arrays](complextype/arrays/array_data_type.md), [Lists](complextype/lists/list_data_type.md), and [Structs](complextype/structs/struct_data_type.md) for detailed usage.

## 🧪 Type Inspection

```sql
-- Check column types
DESCRIBE TABLE my_table;

-- Cast between types
SELECT CAST('123' AS INT);
SELECT CAST(1234567890 AS TIMESTAMP);

-- Type-safe casting (returns NULL on failure instead of error)
SELECT TRY_CAST('abc' AS INT);
-- Result: NULL
```

## 🧠 Type Precedence (Implicit Casting)

When mixing types in expressions, Spark follows this promotion order:

```
TINYINT → SMALLINT → INT → BIGINT → DECIMAL → FLOAT → DOUBLE
STRING → (parsed to target type)
```

> **Tip:** Use `CAST()` or `TRY_CAST()` for explicit conversions. Implicit casting can
> produce unexpected results with precision loss (e.g., `DOUBLE` → `FLOAT`).
