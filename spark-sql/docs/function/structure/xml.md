# XML Functions

Spark SQL provides the `XPATH` family of functions to **extract values from XML strings** using
XPath expressions — useful for processing XML payloads stored in string columns.

## 📌 Available Functions

| Function | Return Type | Description |
|----------|------------|-------------|
| `XPATH(xml, xpath)` | `ARRAY<STRING>` | All matching node values as string array |
| `XPATH_STRING(xml, xpath)` | `STRING` | Text content of first matching node |
| `XPATH_BOOLEAN(xml, xpath)` | `BOOLEAN` | XPath evaluated as boolean |
| `XPATH_INT(xml, xpath)` | `INT` | Integer value (0 if no match) |
| `XPATH_LONG(xml, xpath)` | `BIGINT` | Long value (0 if no match) |
| `XPATH_SHORT(xml, xpath)` | `SMALLINT` | Short value (0 if no match) |
| `XPATH_FLOAT(xml, xpath)` | `FLOAT` | Float value (NaN if non-numeric match) |
| `XPATH_DOUBLE(xml, xpath)` | `DOUBLE` | Double value (NaN if non-numeric match) |
| `XPATH_NUMBER(xml, xpath)` | `DOUBLE` | Alias for `XPATH_DOUBLE` |

## 🔍 Behavior

1. All functions take two arguments: an XML string and an XPath expression.
2. Numeric functions return `0` if no match is found, or `NaN` for non-numeric matches.
3. `XPATH_STRING` returns the text content of the **first** matching node.
4. `XPATH` returns **all** matching values as a string array.
5. `XPATH_BOOLEAN` returns `TRUE` if the XPath matches any node, or if the expression evaluates to true.
6. All functions return `NULL` if the XML input is `NULL`.

## 🧪 Practical Examples

### 🧱 1. Extract All Matching Values

```sql
SELECT XPATH(
  '<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>',
  'a/b/text()'
) AS values;
-- Result: ['b1', 'b2', 'b3']
```

### 🧱 2. Extract First String Value

```sql
SELECT XPATH_STRING('<a><b>hello</b><c>world</c></a>', 'a/c') AS val;
-- Result: world
```

### 🧱 3. Check Node Existence

```sql
SELECT XPATH_BOOLEAN('<a><b>1</b></a>', 'a/b') AS exists;
-- Result: true

SELECT XPATH_BOOLEAN('<a><b>1</b></a>', 'a/x') AS exists;
-- Result: false
```

### 🧱 4. Sum Numeric Nodes

```sql
SELECT XPATH_DOUBLE('<a><b>1</b><b>2</b><b>3</b></a>', 'sum(a/b)') AS total;
-- Result: 6.0

SELECT XPATH_INT('<prices><p>10</p><p>20</p></prices>', 'sum(prices/p)') AS total;
-- Result: 30
```

### 🧱 5. Count Matching Nodes

```sql
SELECT XPATH_INT('<a><b>1</b><b>2</b><b>3</b></a>', 'count(a/b)') AS cnt;
-- Result: 3
```

### 🧱 6. Conditional XPath

```sql
SELECT XPATH_STRING(
  '<employees><emp><name>Alice</name><dept>Engineering</dept></emp><emp><name>Bob</name><dept>Sales</dept></emp></employees>',
  '//emp[dept="Engineering"]/name'
) AS eng_employee;
-- Result: Alice
```

### 🧱 7. Process XML Column

```sql
CREATE OR REPLACE TEMP VIEW xml_data AS
SELECT * FROM VALUES
  ('<order><item>book</item><qty>2</qty><price>15.99</price></order>'),
  ('<order><item>pen</item><qty>10</qty><price>1.50</price></order>')
AS xml_data(payload);

SELECT
  XPATH_STRING(payload, 'order/item') AS item,
  XPATH_INT(payload, 'order/qty') AS quantity,
  XPATH_DOUBLE(payload, 'order/price') AS price
FROM xml_data;
-- (book, 2, 15.99), (pen, 10, 1.50)
```

### 🧱 8. Extract Attributes

```sql
SELECT XPATH_STRING(
  '<book lang="en"><title>Spark SQL</title></book>',
  'book/@lang'
) AS language;
-- Result: en
```

## 📋 Numeric Function Comparison

| Function | Return Type | No Match | Non-Numeric Match |
|----------|------------|----------|-------------------|
| `XPATH_INT` | INT | 0 | 0 |
| `XPATH_LONG` | BIGINT | 0 | 0 |
| `XPATH_SHORT` | SMALLINT | 0 | 0 |
| `XPATH_FLOAT` | FLOAT | 0.0 | NaN |
| `XPATH_DOUBLE` | DOUBLE | 0.0 | NaN |
| `XPATH_NUMBER` | DOUBLE | 0.0 | NaN |

## 🧠 When to Use

| Scenario | Function |
|----------|----------|
| Extract all matching values as array | `XPATH` |
| Get single text value | `XPATH_STRING` |
| Check if node exists | `XPATH_BOOLEAN` |
| Extract numeric value with XPath math | `XPATH_INT`, `XPATH_DOUBLE`, etc. |
| Count matching nodes | `XPATH_INT(xml, 'count(...)')` |
| Filter by attribute or child value | XPath predicates (`//node[@attr="val"]`) |
| Process XML column row-by-row | Combine `XPATH_*` in SELECT |

> **Tip:** For complex XML processing, consider parsing XML files with the `com.databricks.spark.xml`
> reader. Use `XPATH_*` functions for lightweight extraction from XML strings in columns.
