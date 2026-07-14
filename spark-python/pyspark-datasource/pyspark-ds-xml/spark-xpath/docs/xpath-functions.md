# XPath Functions Reference

PySpark (Spark SQL) provides several built-in XPath functions that operate on
**XML strings stored in DataFrame columns**. All functions are available in both
Spark SQL and the PySpark DataFrame API.

---

## Function Summary

| Function | Return Type | Description |
|---|---|---|
| [`xpath_string`](#xpath_string) | `STRING` | First matching text value |
| [`xpath`](#xpath) | `ARRAY<STRING>` | All matching text nodes as array |
| [`xpath_boolean`](#xpath_boolean) | `BOOLEAN` | Evaluates XPath predicate to true/false |
| [`xpath_int`](#xpath_int) | `INT` | Integer value from matched node |
| [`xpath_long`](#xpath_long) | `LONG` | Long integer from matched node |
| [`xpath_short`](#xpath_short) | `SHORT` | Short integer from matched node |
| [`xpath_float`](#xpath_float) | `FLOAT` | Float from matched node |
| [`xpath_double`](#xpath_double) | `DOUBLE` | Double from matched node |
| [`xpath_number`](#xpath_number) | `DOUBLE` | Alias for `xpath_double` |

---

## xpath_string

Returns the **first** text value matching the XPath expression.

```sql
xpath_string(xml_column, 'Root/Parent/Child')
```

!!! info "Return type: `STRING`"
    Returns an empty string `""` (not `null`) when no match is found.

=== "Spark SQL"

    ```sql
    SELECT
        xpath_string(data, 'Msg/Header/tag1') AS tag1,
        xpath_string(data, 'Msg/Header/tag2') AS tag2,
        xpath_string(data, 'Msg/Header/tag3') AS tag3
    FROM xml_df
    ```

=== "PySpark DataFrame API"

    ```python
    from pyspark.sql.functions import expr

    df.select(
        expr("xpath_string(data, 'Msg/Header/tag1')").alias("tag1"),
        expr("xpath_string(data, 'Msg/Header/tag2')").alias("tag2"),
    ).show()
    ```

??? example "Using wildcards"
    The wildcard `*` matches the **first** child element:

    ```sql
    -- Returns the text of the first child under Header
    SELECT xpath_string(data, 'Msg/Header/*') FROM xml_df
    ```

    | data | result |
    |---|---|
    | `<Msg><Header><tag1>first</tag1><tag2>second</tag2></Header></Msg>` | `first` |

---

## xpath

Returns an **array of strings** for all matching text nodes. Use this when an
element repeats and you need all values.

```sql
xpath(xml_column, 'Root/Items/Item/text()')
```

!!! info "Return type: `ARRAY<STRING>`"
    Always returns an array — empty `[]` when no match is found.

=== "Spark SQL"

    ```sql
    SELECT xpath(x, 'a/b/text()') AS values
    FROM xml_data
    ```

=== "PySpark DataFrame API"

    ```python
    from pyspark.sql.functions import xpath, lit

    df.select(
        xpath(df.x, lit('a/b/text()')).alias('values')
    ).show()
    ```

??? success "Expected output"
    Given XML `<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>`:

    ```
    +----------+
    |    values|
    +----------+
    |[b1,b2,b3]|
    +----------+
    ```

!!! tip "Extracting individual elements from the array"
    Use `element_at()` or array indexing to pick a specific item:

    ```sql
    SELECT element_at(xpath(x, 'a/b/text()'), 1) AS first_b FROM xml_data
    ```

---

## xpath_boolean

Evaluates an XPath expression and returns **true / false**. Useful for
conditional logic with `CASE` or `WHERE` clauses.

```sql
xpath_boolean(xml_column, 'Root/Items/Item[ @id > 5 ]')
```

!!! info "Return type: `BOOLEAN`"
    Returns `true` if the XPath predicate matches at least one node.

=== "Simple predicate"

    ```sql
    SELECT xpath_boolean(data, 'root/item[score >= 5]') AS high_score
    FROM items
    ```

=== "Complex predicate (range)"

    ```sql
    SELECT xpath_boolean(data,
        'RespNewCreditEvaluation/InboundResponse/BureauResult'
        '/EQU/RiskModel[@cxArrayIndex=1][ ModelID >= 2 and ModelID <= 4 ]'
    ) AS has_risk_model
    FROM xml_data
    ```

=== "Use in CASE expression"

    ```sql
    SELECT
        CASE
            WHEN xpath_boolean(data, 'root/item[score >= 5]')
            THEN 'HIGH'
            ELSE 'LOW'
        END AS category
    FROM items
    ```

---

## Numeric Functions

### xpath_int

Returns an **integer** value from the matched XPath node.

```sql
xpath_int(xml_column, 'Root/Count')
```

!!! info "Return type: `INT`"
    Returns `0` when no match is found or the value is not a valid integer.

### xpath_long

Returns a **long integer** from the matched XPath node.

```sql
xpath_long(xml_column, 'Root/BigCount')
```

### xpath_short

Returns a **short integer** from the matched XPath node.

```sql
xpath_short(xml_column, 'Root/SmallNumber')
```

### xpath_float

Returns a **float** from the matched XPath node.

```sql
xpath_float(xml_column, 'Root/Rate')
```

### xpath_double

Returns a **double** from the matched XPath node.

```sql
xpath_double(xml_column, 'Root/Amount')
```

### xpath_number

**Alias** for `xpath_double` — returns a `DOUBLE`.

```sql
xpath_number(xml_column, 'Root/Amount')
```

??? example "Numeric extraction example"
    ```sql
    SELECT
        xpath_int(data, 'order/quantity')      AS qty,
        xpath_double(data, 'order/price')      AS price,
        xpath_int(data, 'order/quantity') *
            xpath_double(data, 'order/price')  AS total
    FROM orders
    ```

---

## Common Patterns

### Namespace Handling

Spark XPath **strips the default namespace prefix** automatically. Reference
elements by their local name only:

```sql
-- XML: <ns0:Root xmlns:ns0="http://example.com"><Child>val</Child></ns0:Root>
xpath_string(data, 'Root/Child')       -- ✅ works
xpath_string(data, 'ns0:Root/Child')   -- ❌ does NOT work
```

!!! warning "Always use local names"
    Do **not** include namespace prefixes like `ns0:` in your XPath expressions.
    Spark handles namespace stripping internally.

### Attribute Selectors

Use `[@attribute=value]` to select specific elements by attribute:

```sql
-- Select the RiskModel where cxArrayIndex="1"
xpath_string(data,
    'RespNewCreditEvaluation/InboundResponse/BureauResult'
    '/EQU/RiskModel[@cxArrayIndex=1]/Score'
)
```

### XPath Predicates

Filter elements using conditions inside square brackets:

```sql
-- Elements where score > 100
xpath_string(data, 'Root/Items/Item[Score > 100]/Name')

-- Elements where attribute matches AND value condition holds
xpath_string(data, 'Root/Item[@type="premium"][Price < 50]/Name')
```

### Combining with CASE Expressions

```sql
SELECT *,
    CASE
        WHEN xpath_boolean(data, '...condition_a...') THEN xpath_string(data, '...value_a...')
        WHEN xpath_boolean(data, '...condition_b...') THEN xpath_string(data, '...value_b...')
        ELSE xpath_string(data, '...default_value...')
    END AS result
FROM xml_data
```

### Combining with String Comparison

```sql
-- Compare extracted value with a known string
xpath_string(data, 'Root/Bureau/RiskModel/ScoreID') == 'TC' AS is_tc_model
```

---

## Performance Tips

!!! tip "Keep XPath expressions simple"
    - Prefer direct paths (`Root/Child/Value`) over deep wildcards (`Root//Value`).
    - The `//` descendant-or-self axis scans the entire subtree and is slower.

!!! tip "Extract once, reuse with CTE"
    If you extract many fields from the same XML, use a **CTE** to avoid
    re-parsing:

    ```sql
    WITH extracted AS (
        SELECT
            xpath_string(data, 'Root/Field1') AS field1,
            xpath_string(data, 'Root/Field2') AS field2,
            xpath_boolean(data, 'Root/Flag')  AS flag
        FROM xml_data
    )
    SELECT *, CASE WHEN flag THEN field1 ELSE field2 END AS result
    FROM extracted
    ```

!!! tip "Filter early"
    Use `WHERE` clauses with `xpath_boolean` to filter rows **before** extracting
    expensive fields:

    ```sql
    SELECT xpath_string(data, 'Root/Detail/LargeField') AS detail
    FROM xml_data
    WHERE xpath_boolean(data, 'Root[Status="Active"]')
    ```
