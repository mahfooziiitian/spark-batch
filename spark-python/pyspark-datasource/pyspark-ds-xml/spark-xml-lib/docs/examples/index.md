# Examples

Runnable example scripts demonstrating end-to-end spark-xml features.

Each script under `src/spark_xml/` is self-contained and executable:

```bash
uv run python src/spark_xml/<module>/<script>.py
```

---

## By Feature

### Reading XML

| Script | Description |
|---|---|
| `reader/xml_reader.py` | Basic XML file reading |
| `reader/date_time/default_date_time_reader.py` | ISO 8601 date/timestamp parsing |
| `reader/date_time/custom_date_time_reader_before.py` | Custom `dateFormat` with corrupt record handling |
| `reader/date_time/custom_date_time_reader_after.py` | Post-read date conversion with `to_date()` |
| `collection/read_api_xml_collection.py` | Read XML from HTTP API via temp file |
| `collection/read_xml_string_text.py` | Read XML string with FAILFAST mode |
| `collection/read_xml_string_pandas.py` | Read XML via Pandas bridge |

### Writing XML

| Script | Description |
|---|---|
| `writer/xml_writer.py` | Basic XML writing |
| `compression/spark_xml_gzip_write.py` | Write-only gzip example |

### Compression

| Script | Codec |
|---|---|
| `compression/spark_xml_gzip.py` | gzip round-trip |
| `compression/spark_xml_bz2.py` | bzip2 round-trip |
| `compression/spark_xml_deflate.py` | deflate round-trip |
| `compression/spark_xml_snappy.py` | snappy round-trip |

### Encoding

| Script | Encoding |
|---|---|
| `encoding/spark_xml_encoding_utf8.py` | UTF-8 read/write |
| `encoding/spark_xml_encoding_utf16.py` | UTF-16 read/write |
| `encoding/spark_xml_encoding_iso.py` | ISO-8859-1 read/write |

### Schema

| Script | Description |
|---|---|
| `schema/xml_schema_validator_schema.py` | Explicit `StructType` for orders.xml |
| `schema/json/read_schema_in_json.py` | Load schema from JSON file |
| `schema/xsd/validate/pyt/xml_schema_validator.py` | XSD validation via spark-xml |
| `schema/xsd/validate/sql/xml_schema_validator_sql.py` | XSD validation via SQL |
| `schema/xsd/xml_schema_validator_corrupt.py` | XSD + FAILFAST for corrupt data |
| `schema/xsd/generate/xsd_to_xml.py` | Generate XML from XSD (xmlschema) |
| `schema/xsd/generate/xsd_to_xml_2.py` | Generate XML from XSD (lxml) |

### Nested XML

| Script | Description |
|---|---|
| `nested/nested_xml.py` | Iterative struct/array flattening |
| `nested/nested_xml_2.py` | Alternative flattening approach |
| `nested/array_of_struct.py` | Explode array of structs |
| `nested/array_of_struct_array_of_map.py` | Complex nested structures |
| `nested/parsing_xml_column.py` | JVM bridge `from_xml()` for XML columns |

### Namespaces & Attributes

| Script | Description |
|---|---|
| `namespace/namespace_xml.py` | Preserve namespace prefixes |
| `namespace/ignore_namespace_xml.py` | Ignore namespace prefixes |
| `attribute/attribute_prefix.py` | Custom attribute prefix mapping |
| `value_tag/element_value_tag.py` | `_VALUE` access for mixed-content elements |

### SQL Interface

| Script | Description |
|---|---|
| `sql/spark-databrick-xml-sql.py` | CREATE TABLE USING xml |
| `sql/spark-databrick-xml.py` | SQL-based XML reading |

### Utilities

| Script | Description |
|---|---|
| `util/data/generate_xml_data.py` | Generate XML from CSV mapping + Faker |
| `util/data/generate_xml_from_xsd.py` | Generate XML from XSD schema |
| `util/validation/validate_xml_xsd.py` | Validate XML against XSD |
| `util/xsd/xml_to_xsd.py` | Generate XSD from XML (xmltoxsd) |
| `util/xsd/xml_to_xsd_trang.py` | Generate XSD from XML (Trang JAR) |

### Other

| Script | Description |
|---|---|
| `instruction/spark_xml_instruction.py` | XML with processing instructions |
| `stack_overlfow/stack_overflow_array.py` | posexplode recipe |
| `stack_overlfow/xml_column/spark_xml_column.py` | UDF-based XML column parsing |
| `comapredf.py` | DataFrame reconciliation comparator |

---

## Quick Run

```bash
# Compression round-trip
uv run python src/spark_xml/compression/spark_xml_gzip.py

# Namespace handling
uv run python src/spark_xml/namespace/ignore_namespace_xml.py

# Generate test data from XSD
uv run python src/spark_xml/util/data/generate_xml_from_xsd.py \
    src/spark_xml/util/data/books.xsd --count 10 --pretty
```
