# Attributes

Map XML attributes to DataFrame columns using a configurable prefix.

---

## The Problem

Given this XML:

```xml
<root>
  <person id="1" role="admin">
    <name>Alice</name>
  </person>
</root>
```

By default, attributes become columns prefixed with `_` (e.g., `_id`, `_role`).

---

## Custom Attribute Prefix

Use `attributePrefix` to set a custom prefix:

```python
df = (
    spark.read.format("com.databricks.spark.xml")
    .option("rootTag", "root")
    .option("rowTag", "person")
    .option("attributePrefix", "attr_")
    .load(xml_file)
)
df.printSchema()
df.show()
```

Result schema:

```
root
 |-- attr_id: string
 |-- attr_role: string
 |-- name: string
```

> **Source:** `src/spark_xml/attribute/attribute_prefix.py`

---

## Exclude Attributes

Skip attributes entirely with `excludeAttribute`:

```python
df = (
    spark.read.format("xml")
    .option("rowTag", "person")
    .option("excludeAttribute", "true")
    .load(xml_file)
)
# Only element content columns: name
```

---

## Writing Attributes

When writing, columns with the attribute prefix are written as XML attributes:

```python
# DataFrame with attr_ columns writes them as attributes
df.write.format("xml") \
    .option("rootTag", "root") \
    .option("rowTag", "person") \
    .option("attributePrefix", "attr_") \
    .save(output_path)
```

Output:

```xml
<root>
  <person id="1" role="admin">
    <name>Alice</name>
  </person>
</root>
```
