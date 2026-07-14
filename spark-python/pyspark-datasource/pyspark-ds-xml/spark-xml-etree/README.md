# spark-xml-etree

PySpark examples that parse XML strings inside DataFrames using Python's
built-in `xml.etree.ElementTree` through Spark UDFs.

## Examples

| Script | Demonstrates |
|--------|-------------|
| `xmls_data_processing.py` | Single-field extraction with a string UDF |
| `xmls_data_processing_multiple_column.py` | Multi-field extraction with a struct-returning UDF |
| `xmls_data_processing_multiple_column2.py` | Attribute extraction, nested records, and `explode()` |
| `xmls_namespace_handling.py` | Namespace-prefixed XML parsing with namespace maps |
| `xmls_nested_flattening.py` | Flattening deeply nested XML (orders → line items) into denormalized rows |
| `xmls_error_handling.py` | Robust parsing of malformed, incomplete, and missing XML |
| `xmls_build_from_dataframe.py` | Building XML strings from DataFrame rows and round-tripping back |

## Run

```bash
uv run python src/spark_etree/xmls_data_processing.py
uv run python src/spark_etree/xmls_data_processing_multiple_column.py
uv run python src/spark_etree/xmls_data_processing_multiple_column2.py
uv run python src/spark_etree/xmls_namespace_handling.py
uv run python src/spark_etree/xmls_nested_flattening.py
uv run python src/spark_etree/xmls_error_handling.py
uv run python src/spark_etree/xmls_build_from_dataframe.py
```