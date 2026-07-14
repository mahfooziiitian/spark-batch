# Spark XML

## XML to XSD generator

### trang

location: <https://github.com/relaxng/jing-trang>

```bash
java -jar ~/data/jars/trang.jar ~/data/file_data/xml/person.xml ~/data/file_data/xml/person.xsd
```

### xmltoxsd

```python
from xmltoxsd import XSDGenerator

generator = XSDGenerator()
xsd_schema = generator.generate_xsd(sample_xml_file)
print(xsd_schema)
```

## refereces

1. <https://github.com/databricks/spark-xml>
1.
