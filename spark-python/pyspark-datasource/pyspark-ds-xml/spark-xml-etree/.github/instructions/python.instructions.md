---
applyTo: "**/*.py"
---

# Python Code Conventions

## Language Version

- Python ≥ 3.11 (see `.python-version`).
- Use modern syntax: `list[str]` over `List[str]`, `X | None` over `Optional[X]`
  (except in PySpark UDFs where `typing.Optional` is clearer).

## Style

- Follow PEP 8.
- Maximum line length: 120 characters.
- Use double quotes for strings.
- Use f-strings for interpolation.
- snake_case for functions, variables, and modules; PascalCase for classes.
- No trailing whitespace; files end with a single newline.

## Imports

Order imports in three groups separated by blank lines:

1. Standard library (`os`, `xml.etree.ElementTree`, `typing`)
2. Third-party (`pyspark`)
3. Local (`spark_etree.*`)

```python
import os
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
```

Never use wildcard imports:

```python
# ✅ Good
from pyspark.sql import functions as F

# ❌ Bad
from pyspark.sql.functions import *
```

## Type Hints

Add type hints to all function signatures:

```python
def extract_title(payload: str) -> Optional[str]:
    ...

def flatten_order(payload: str) -> List[Tuple]:
    ...
```

## Docstrings

Use Google-style docstrings. Keep them concise — a single imperative line
is sufficient for small helpers:

```python
def extract_title(payload: str) -> Optional[str]:
    """Extract the TITLE text from a single CD XML element."""
    ...
```

For complex functions, add Args / Returns / Raises:

```python
def flatten_order(payload: str) -> List[Tuple]:
    """Denormalize one <order> element into a list of line-item tuples.

    Args:
        payload: XML string containing a single <order> element.

    Returns:
        List of tuples (order_id, date, customer, region, sku, qty, price).

    Raises:
        ET.ParseError: If payload is not valid XML.
    """
    ...
```

## Module-Level Docstrings

Every source file starts with a one-line docstring describing its purpose:

```python
"""Parse XML elements with ElementTree and extract a single field via UDF."""
```

## Constants

Module-level constants use UPPER_SNAKE_CASE:

```python
SAMPLE_XML = """\
<CATALOG>
  ...
</CATALOG>
"""

CD_INFO_SCHEMA = StructType([...])
```

## Entry Points

Standalone scripts use `if __name__ == "__main__":` guard.
Test files additionally include:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
