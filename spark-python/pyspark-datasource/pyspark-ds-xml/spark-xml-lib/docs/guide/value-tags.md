# Value Tags

Access the text content of elements that have both text and attributes.

---

## The Problem

Given this XML:

```xml
<books>
  <book>
    <title>XML Guide</title>
    <price currency="USD">44.95</price>
  </book>
</books>
```

The `<price>` element has **both** a text value (`44.95`) and an attribute (`currency`). spark-xml represents these using:

- `price._VALUE` — the element text content
- `price.attr_currency` — the attribute (with `attributePrefix`)

---

## Reading Value Tags

```python
books_df = (
    spark.read.format("com.databricks.spark.xml")
    .option("rootTag", "books")
    .option("rowTag", "book")
    .option("attributePrefix", "attr_")
    .load(xml_file)
)

books_df.printSchema()
```

Schema:

```
root
 |-- title: string
 |-- price: struct
 |    |-- _VALUE: double
 |    |-- attr_currency: string
```

---

## Selecting Value and Attribute

```python
books_df.select(
    "title",
    "price._VALUE",
    "price.attr_currency",
).show()
```

```
+----------+------+-------------+
|     title|_VALUE|attr_currency|
+----------+------+-------------+
| XML Guide| 44.95|          USD|
+----------+------+-------------+
```

> **Source:** `src/spark_xml/value_tag/element_value_tag.py`

!!! tip
    The `_VALUE` tag name is fixed by spark-xml and cannot be changed. The attribute prefix is configurable via `attributePrefix`.
