# Utility Reference

Functions, schemas, and constants exported by each module. All functions
are designed to be used both as standalone Python functions and inside
PySpark UDFs.

## xmls_data_processing

:material-file-code: `examples/xmls_data_processing.py`

| Name | Type | Description |
|------|------|-------------|
| `SAMPLE_XML` | `str` | Embedded CD catalog XML (5 CDs) |
| `extract_title(payload)` | `str → Optional[str]` | Extract `<TITLE>` text from a CD element |

```python
from spark_etree.xmls_data_processing import extract_title

extract_title('<CD><TITLE>Eros</TITLE></CD>')  # → "Eros"
extract_title('<CD><ARTIST>Test</ARTIST></CD>') # → None
```

---

## xmls_data_processing_multiple_column

:material-file-code: `examples/xmls_data_processing_multiple_column.py`

| Name | Type | Description |
|------|------|-------------|
| `SAMPLE_XML` | `str` | Embedded CD catalog XML (5 CDs) |
| `CD_INFO_SCHEMA` | `StructType` | Schema: title, artist, country, year (all `StringType`) |
| `select_text(xml_doc, xpath)` | `Element, str → Optional[str]` | Return text of first match, or `None` |
| `extract_cd_info(payload)` | `str → Dict[str, Optional[str]]` | Extract all CD fields as a dict |

```python
from spark_etree.xmls_data_processing_multiple_column import extract_cd_info

extract_cd_info('<CD><TITLE>X</TITLE><ARTIST>Y</ARTIST><COUNTRY>US</COUNTRY><YEAR>2000</YEAR></CD>')
# → {"title": "X", "artist": "Y", "country": "US", "year": "2000"}
```

---

## xmls_data_processing_multiple_column2

:material-file-code: `examples/xmls_data_processing_multiple_column2.py`

| Name | Type | Description |
|------|------|-------------|
| `SAMPLE_DATA` | `list[dict]` | Embedded test data (3 rows with XML attributes + nested records) |
| `extract_attributes(xml)` | `str → List[str]` | UDF-decorated; returns `[a, b]` attributes from root element |
| `extract_record_ids(xml)` | `str → List[int]` | Extract all `record/@id` values as integers |

```python
from spark_etree.xmls_data_processing_multiple_column2 import extract_record_ids

extract_record_ids('<test a="1" b="2"><records><record id="10" /><record id="20" /></records></test>')
# → [10, 20]
```

---

## xmls_namespace_handling

:material-file-code: `examples/xmls_namespace_handling.py`

| Name | Type | Description |
|------|------|-------------|
| `NS` | `dict[str, str]` | Namespace map: `{"bk": "...", "rv": "..."}` |
| `SAMPLE_XML` | `str` | Embedded library XML with 3 namespaced books |
| `BOOK_SCHEMA` | `StructType` | Schema: isbn, title, author, year |
| `extract_book(payload)` | `str → Dict[str, Optional[str]]` | Extract book metadata using namespace map |
| `extract_review_ratings(payload)` | `str → List[int]` | Extract all `rv:rating` values as integers |

```python
from spark_etree.xmls_namespace_handling import extract_review_ratings

# Returns [5, 4] for a book with two reviews rated 5 and 4
```

---

## xmls_nested_flattening

:material-file-code: `examples/xmls_nested_flattening.py`

| Name | Type | Description |
|------|------|-------------|
| `SAMPLE_ORDERS_XML` | `str` | Embedded orders XML (4 orders, 8 line items) |
| `LINE_ITEM_SCHEMA` | `ArrayType(StructType)` | Schema for exploded line items |
| `flatten_order(payload)` | `str → List[Tuple]` | Denormalize one order into line-item tuples |

```python
from spark_etree.xmls_nested_flattening import flatten_order

rows = flatten_order('<order id="1" date="2025-01-01"><customer name="A" region="N" />'
                     '<items><item sku="X" qty="2" price="10.0" /></items></order>')
# → [("1", "2025-01-01", "A", "N", "X", 2, 10.0)]
```

---

## xmls_error_handling

:material-file-code: `examples/xmls_error_handling.py`

| Name | Type | Description |
|------|------|-------------|
| `SAMPLE_DATA` | `list[dict]` | Mix of valid, malformed, null, and empty XML rows |
| `PRODUCT_SCHEMA` | `StructType` | Schema: sku, name, price, category, parse_error |
| `safe_parse_product(payload)` | `Optional[str] → Dict` | Parse product XML; returns error message on failure |

```python
from spark_etree.xmls_error_handling import safe_parse_product

safe_parse_product("not xml")   # → {"sku": None, ..., "parse_error": "syntax error: ..."}
safe_parse_product(None)        # → {"sku": None, ..., "parse_error": "empty or null input"}
safe_parse_product('<product sku="P1"><name>W</name></product>')
                                # → {"sku": "P1", "name": "W", ..., "parse_error": None}
```

---

## xmls_build_from_dataframe

:material-file-code: `examples/xmls_build_from_dataframe.py`

| Name | Type | Description |
|------|------|-------------|
| `SAMPLE_DATA` | `list[dict]` | Employee records (5 rows) |
| `row_to_xml(emp_id, name, dept, salary)` | `int, str, str, int → Optional[str]` | Build an `<employee>` XML element string |
| `wrap_in_root(fragments, root_tag)` | `list[str], str → str` | Combine fragments under a root element with XML declaration |

```python
from spark_etree.xmls_build_from_dataframe import row_to_xml, wrap_in_root

xml = row_to_xml(101, "Alice", "Engineering", 95000)
# → '<employee id="101"><name>Alice</name><department>Engineering</department><salary>95000</salary></employee>'

doc = wrap_in_root([xml])
# → '<?xml version=\'1.0\' ...?>\n<employees>\n  <employee ...>...</employee>\n</employees>'
```
