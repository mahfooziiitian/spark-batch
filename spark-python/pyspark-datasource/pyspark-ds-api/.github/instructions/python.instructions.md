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

1. Standard library (`os`, `sys`, `json`, `ssl`, `typing`, `pathlib`, `base64`)
2. Third-party (`pyspark`, `requests`, `httpx`, `authlib`, `jwt`, `cryptography`, `fastapi`)
3. Local (project modules)

```python
import os
import json
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

import requests
from requests.auth import HTTPBasicAuth
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
def make_request(url: str, headers: dict[str, str], auth: HTTPBasicAuth | None = None) -> requests.Response:
    ...

def read_key_value(data: dict, key_path: str) -> str | int | list | None:
    ...
```

## Docstrings

Use Google-style docstrings. Keep them concise — a single imperative line
is sufficient for small helpers:

```python
def get_auth_headers(auth_config: dict) -> tuple[dict, HTTPBasicAuth | None]:
    """Build authentication headers and auth object from config."""
    ...
```

For complex functions, add Args / Returns / Raises:

```python
def fetch_paginated_data(
    url: str,
    strategy: str,
    result_key: str,
    **kwargs,
) -> list[dict]:
    """Fetch all pages from a paginated REST API.

    Args:
        url: Base API endpoint URL.
        strategy: Pagination strategy (cursor, offset, page).
        result_key: Dot-notation path to data array in response.
        **kwargs: Strategy-specific parameters (limit, cursor_key, etc.).

    Returns:
        Flattened list of all records across all pages.

    Raises:
        requests.HTTPError: If any page request fails after retries.
    """
    ...
```

## Module-Level Docstrings

Every source file starts with a one-line docstring describing its purpose:

```python
"""REST API client with authentication, pagination, and retry support."""
```

## Constants

Module-level constants use UPPER_SNAKE_CASE:

```python
DEFAULT_MAX_RETRIES = 3
DEFAULT_TIMEOUT = 30
SUPPORTED_AUTH_TYPES = ["basic", "bearer", "apikey", "oauth2", "mtls"]
```

## Entry Points

Standalone scripts use `if __name__ == "__main__":` guard.
Test files additionally include:

```python
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
```
