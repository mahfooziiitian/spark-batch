# API Reference

Utility modules for XML/XSD generation, validation, and Spark session management.

---

## Spark Session Utility

::: spark_xml.util.session.spark_session_util

### `get_spark_session()`

Create a SparkSession pre-configured with the spark-xml JAR.

```python
from spark_xml.util.session.spark_session_util import get_spark_session

spark = get_spark_session(
    app_name="my-app",
    scala_version="2.12",
    spark_xml_version="0.18.0",
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `app_name` | `str` | *(required)* | Spark application name |
| `scala_version` | `str` | `"2.12"` | Scala version for spark-xml JAR |
| `spark_xml_version` | `str` | `"0.17.0"` | spark-xml library version |

**Returns:** `SparkSession`

> **Source:** `src/spark_xml/util/session/spark_session_util.py`

---

## XML Validation

### `validate_xml()`

Validate an XML file against an XSD schema using `xmlschema`.

```python
from spark_xml.util.validation.validate_xml_xsd import validate_xml

validate_xml("orders.xsd", "orders.xml")
# ✅ XML is valid against the XSD!
```

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `xsd_file` | `str` | Path to the XSD schema file |
| `xml_file` | `str` | Path to the XML file to validate |

**Prints:** `✅` on success, `❌` with error details on failure.

> **Source:** `src/spark_xml/util/validation/validate_xml_xsd.py`

---

## CSV-Mapping XML Generator

### `generate_xml_data.py`

Generate XML data from a pipe-delimited CSV mapping file using Faker.

```bash
python generate_xml_data.py -m mapping.csv -t person -n 100 --pretty --seed 42
```

**CLI Options:**

| Option | Default | Description |
|---|---|---|
| `--mapping`, `-m` | `mapping.csv` | Pipe-delimited CSV mapping file |
| `--tag`, `-t` | `person` | Row element tag name |
| `--root`, `-r` | `root` | Root wrapper element name |
| `--count`, `-n` | `1000` | Number of elements to generate |
| `--output`, `-o` | `DATA_HOME/.../tag.xml` | Output file path |
| `--pretty` | `false` | Pretty-print XML output |
| `--seed`, `-s` | *(random)* | Random seed for reproducibility |

**Mapping CSV format** (pipe-delimited):

```
tag|attribute|attribute_type|attribute_allowed_values|data_type|allowed_values
p_id|id|int||string|
name||||string|
gender||||string|male,female,other
```

**Supported data types:**

`string`, `int`, `float`, `decimal`, `boolean`, `date`, `datetime`, `name`, `first_name`, `last_name`, `email`, `phone`, `address`, `city`, `country`, `zipcode`, `uuid`, `sentence`, `paragraph`, `url`, `company`, `job`

**Python API:**

```python
from spark_xml.util.data.generate_xml_data import load_mapping, build_xml, pretty_print_xml

mapping = load_mapping("mapping.csv")
tree = build_xml(mapping, main_tag="person", count=50, root_tag="people")
print(pretty_print_xml(tree))
```

> **Source:** `src/spark_xml/util/data/generate_xml_data.py`

---

## XSD-Based XML Generator

### `generate_xml_from_xsd.py`

Read an XSD schema and generate realistic sample XML data.

```bash
python generate_xml_from_xsd.py books.xsd --count 5 --output books.xml
```

**CLI Options:**

| Option | Default | Description |
|---|---|---|
| `xsd_file` | *(required)* | Path to XSD schema file |
| `--output`, `-o` | `<name>_generated.xml` | Output XML file |
| `--count`, `-n` | `3` | Number of elements |
| `--root-tag`, `-r` | auto-derived | Root wrapper element |

**Supported XSD types:**

- Simple: `xs:string`, `xs:int`, `xs:integer`, `xs:long`, `xs:decimal`, `xs:float`, `xs:double`, `xs:boolean`, `xs:date`, `xs:dateTime`, `xs:time`, `xs:anyURI`
- Complex types with nested sequences
- Attributes (required/optional)
- `xs:enumeration` restrictions
- `minOccurs` / `maxOccurs`

**Python API:**

```python
from spark_xml.util.data.generate_xml_from_xsd import generate_xml_from_xsd, pretty_print_xml

tree = generate_xml_from_xsd("books.xsd", count=10, root_tag="library")
print(pretty_print_xml(tree))
```

> **Source:** `src/spark_xml/util/data/generate_xml_from_xsd.py`

---

## XSD Generation

### From XML via `xmltoxsd`

```python
from xmltoxsd import XSDGenerator

generator = XSDGenerator()
xsd = generator.generate_xsd("sample.xml")
print(xsd)
```

> **Source:** `src/spark_xml/util/xsd/xml_to_xsd.py`

### From XML via Trang (Java)

```bash
java -jar trang.jar input.xml output.xsd
```

The project includes a helper that downloads Trang via Maven and runs the conversion:

> **Source:** `src/spark_xml/util/xsd/xml_to_xsd_trang.py`

---

## DataFrame Reconciliation

### `ReconComparator`

Compare two DataFrames and produce reconciliation buckets:

```python
from spark_xml.comapredf import ReconComparator

comparator = ReconComparator(
    spark=spark,
    srcdf=source_df,
    tgtdf=target_df,
    key_columns_dict="id",
)
comparator.recon_compare()
```

**Buckets:**

| Bucket | Description |
|---|---|
| Bucket 1 | Keys in source not found in target |
| Bucket 2 | Keys matched, values identical |
| Bucket 3 | Keys matched, values differ (with column-level diff) |
| Bucket 4 | Keys in target not found in source |

> **Source:** `src/spark_xml/comapredf.py`
