# :material-file-code: Structure Functions

Structure functions parse, generate, and manipulate semi-structured data formats
such as **JSON**, **CSV**, and **XML** within Spark SQL. They bridge the gap between
raw string data and Spark's typed column system.

### :material-sitemap: Overview

```mermaid
graph LR
    A[Semi-structured String Column] --> B["from_json / from_csv / from_xml"]
    B --> C[Typed Struct Column]
    C --> D[Field Access]
```

## 📌 Functions by Format

### JSON

| Function | Direction | Description |
|----------|-----------|-------------|
| `FROM_JSON(str, schema)` | String → Struct | Parse JSON string into a struct/array |
| `TO_JSON(expr)` | Struct → String | Convert struct/map/array to JSON string |
| `GET_JSON_OBJECT(json, path)` | Extract | Extract value using JSONPath |
| `JSON_TUPLE(json, keys…)` | Extract | Extract multiple keys at once |
| `JSON_ARRAY_LENGTH(json)` | Inspect | Count elements in a JSON array |
| `JSON_OBJECT_KEYS(json)` | Inspect | Get all keys of a JSON object |
| `SCHEMA_OF_JSON(json)` | Inspect | Infer schema DDL from JSON sample |

### CSV

| Function | Direction | Description |
|----------|-----------|-------------|
| `FROM_CSV(str, schema, options)` | String → Struct | Parse CSV string into a struct |
| `TO_CSV(expr)` | Struct → String | Convert struct to CSV string |
| `SCHEMA_OF_CSV(csv)` | Inspect | Infer schema DDL from CSV sample |

### XML

| Function | Direction | Description |
|----------|-----------|-------------|
| `XPATH(xml, xpath)` | Extract | Extract string array via XPath |
| `XPATH_STRING(xml, xpath)` | Extract | Extract single string value |
| `XPATH_BOOLEAN(xml, xpath)` | Extract | Evaluate XPath as boolean |
| `XPATH_INT(xml, xpath)` | Extract | Extract integer value |
| `XPATH_LONG(xml, xpath)` | Extract | Extract long value |
| `XPATH_DOUBLE(xml, xpath)` | Extract | Extract double value |
| `XPATH_FLOAT(xml, xpath)` | Extract | Extract float value |
| `XPATH_SHORT(xml, xpath)` | Extract | Extract short value |

## 🧪 Quick Examples

```sql
-- Parse JSON into a struct
SELECT FROM_JSON('{"name":"Alice","age":30}', 'name STRING, age INT') AS parsed;

-- Convert struct to JSON
SELECT TO_JSON(NAMED_STRUCT('product', 'laptop', 'price', 999.99)) AS json_str;

-- Extract from JSON using path
SELECT GET_JSON_OBJECT('{"store":{"book":"Spark"}}', '$.store.book') AS title;

-- Parse CSV
SELECT FROM_CSV('Alice,30,Engineering', 'name STRING, age INT, dept STRING') AS parsed;

-- Convert struct to CSV
SELECT TO_CSV(NAMED_STRUCT('name', 'Alice', 'age', 30)) AS csv_str;

-- XPath extraction
SELECT XPATH_STRING('<root><name>Spark</name></root>', '/root/name') AS val;
```

## 🧠 When to Use

| Scenario | Function |
|----------|----------|
| Ingest JSON columns from external sources | `FROM_JSON` |
| Serialize structs for output/export | `TO_JSON`, `TO_CSV` |
| Quick field extraction from JSON strings | `GET_JSON_OBJECT`, `JSON_TUPLE` |
| Inspect JSON structure | `JSON_OBJECT_KEYS`, `JSON_ARRAY_LENGTH` |
| Process XML payloads | `XPATH_*` family |
| Discover schema of raw data | `SCHEMA_OF_JSON`, `SCHEMA_OF_CSV` |
