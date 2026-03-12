# Namespaces

Handle XML namespace prefixes during reads.

---

## The Problem

Given namespaced XML:

```xml
<root xmlns:bk="http://example.com/books">
  <bk:book>
    <bk:title>XML Guide</bk:title>
    <bk:author>Alice</bk:author>
  </bk:book>
</root>
```

The `rowTag` must include the prefix: `"bk:book"`.

---

## Preserve Namespaces

Column names retain the namespace prefix:

```python
df = (
    spark.read.format("xml")
    .option("rowTag", "bk:book")
    .option("ignoreNamespace", "false")
    .load(xml_file)
)
df.show()
# Columns: bk:title, bk:author
```

> **Source:** `src/spark_xml/namespace/namespace_xml.py`

---

## Ignore Namespaces

Strip prefixes for cleaner column names:

```python
df = (
    spark.read.format("xml")
    .option("rowTag", "bk:book")
    .option("ignoreNamespace", "true")
    .load(xml_file)
)
df.show()
# Columns: title, author
```

> **Source:** `src/spark_xml/namespace/ignore_namespace_xml.py`

!!! tip
    Use `ignoreNamespace: "true"` unless you need to distinguish identically-named elements across different namespaces.
