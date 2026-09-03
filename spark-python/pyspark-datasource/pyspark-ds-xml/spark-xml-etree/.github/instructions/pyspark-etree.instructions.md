---
applyTo: "src/**/*.py,examples/**/*.py"
---

# PySpark + ElementTree Patterns

## SparkSession

Every standalone script creates a SparkSession using the `SPARK_MASTER` env var
with a `local[*]` fallback:

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("xml-etree-descriptive-name")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

Always call `spark.stop()` at the end of standalone scripts.

## ElementTree Import

Always alias as `ET`:

```python
import xml.etree.ElementTree as ET
```

## UDF Patterns

### String UDF — Extract a Single Value

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def extract_title(payload: str) -> Optional[str]:
    doc = ET.fromstring(payload)
    titles = [e.text for e in doc.findall("TITLE") if isinstance(e, ET.Element)]
    return next(iter(titles), None)

extract_title_udf = udf(extract_title, StringType())
df.select(extract_title_udf(F.col("xml")).alias("title"))
```

### Struct UDF — Extract Multiple Fields

Return a dict matching the StructType schema:

```python
from pyspark.sql.types import StructType, StructField, StringType

CD_INFO_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("artist", StringType(), True),
])

def extract_cd_info(payload: str) -> Dict[str, Optional[str]]:
    doc = ET.fromstring(payload)
    return {
        "title": doc.findtext("TITLE"),
        "artist": doc.findtext("ARTIST"),
    }

extract_udf = udf(extract_cd_info, CD_INFO_SCHEMA)
df.withColumn("info", extract_udf("xml")).select("info.title", "info.artist")
```

### Array UDF + Explode — One-to-Many

```python
from pyspark.sql.types import ArrayType, IntegerType

def extract_record_ids(xml: str) -> List[int]:
    doc = ET.fromstring(xml)
    return [int(r.attrib["id"]) for r in doc.findall("records/record")]

extract_udf = udf(extract_record_ids, ArrayType(IntegerType()))
df.withColumn("ids", extract_udf(F.col("data"))).withColumn("id", F.explode("ids"))
```

### Array-of-Struct UDF — Nested Flattening

Return a list of tuples to flatten hierarchical XML into denormalized rows:

```python
LINE_ITEM_SCHEMA = ArrayType(StructType([
    StructField("order_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("qty", IntegerType(), False),
    StructField("price", DoubleType(), False),
]))

def flatten_order(payload: str) -> List[Tuple]:
    order = ET.fromstring(payload)
    return [
        (order.attrib["id"], item.attrib["sku"],
         int(item.attrib["qty"]), float(item.attrib["price"]))
        for item in order.findall("items/item")
    ]

flatten_udf = udf(flatten_order, LINE_ITEM_SCHEMA)
df.withColumn("items", flatten_udf("xml")).select(F.explode("items").alias("item")).select("item.*")
```

### Decorator UDF

Use `@udf(returnType=...)` for simple UDFs that don't need a separate registration step:

```python
@udf(returnType=ArrayType(StringType()))
def extract_attributes(xml: str) -> List[str]:
    doc = ET.fromstring(xml)
    return [doc.attrib["a"], doc.attrib["b"]]
```

## XML Namespace Handling

Define a namespace map and pass it to `find()` / `findall()`:

```python
NS = {
    "bk": "http://example.com/books",
    "rv": "http://example.com/reviews",
}

def extract_book(payload: str) -> Dict[str, Optional[str]]:
    doc = ET.fromstring(payload)
    return {
        "isbn": doc.attrib.get("isbn"),
        "title": doc.findtext("bk:title", namespaces=NS),
    }
```

Register namespaces before `ET.tostring()` to preserve prefixes:

```python
for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)
```

## Error Handling

For production XML parsing, catch `ET.ParseError` and return error details
instead of crashing the Spark job:

```python
def safe_parse(payload: Optional[str]) -> Dict[str, Optional[str]]:
    if not payload:
        return {**empty_result, "parse_error": "empty or null input"}
    try:
        doc = ET.fromstring(payload)
    except ET.ParseError as e:
        return {**empty_result, "parse_error": str(e)}
    return {... parsed fields ..., "parse_error": None}
```

Then separate clean from error rows:

```python
clean = parsed.filter(F.col("parse_error").isNull())
errors = parsed.filter(F.col("parse_error").isNotNull())
```

## Building XML from DataFrames

Use `ET.Element`, `ET.SubElement`, and `ET.tostring` inside a UDF:

```python
def row_to_xml(emp_id: int, name: str, dept: str) -> Optional[str]:
    emp = ET.Element("employee", id=str(emp_id))
    ET.SubElement(emp, "name").text = name
    ET.SubElement(emp, "department").text = dept
    return ET.tostring(emp, encoding="unicode")
```

Assemble fragments into a full document on the driver:

```python
def wrap_in_root(fragments: list[str], root_tag: str = "employees") -> str:
    root = ET.Element(root_tag)
    for frag in fragments:
        root.append(ET.fromstring(frag))
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)
```

## Sample Data

Embed XML sample data as module-level constants rather than fetching from URLs:

```python
SAMPLE_XML = """\
<CATALOG>
  <CD>
    <TITLE>Empire Burlesque</TITLE>
    <ARTIST>Bob Dylan</ARTIST>
  </CD>
</CATALOG>
"""
```

## DataFrame Function Style

Use `from pyspark.sql import functions as F` and method chaining:

```python
result = (
    df
    .withColumn("info", extract_udf("xml"))
    .select("info.title", "info.artist", "info.country")
    .filter(F.col("info.country") == "USA")
    .orderBy(F.desc("info.title"))
)
```
