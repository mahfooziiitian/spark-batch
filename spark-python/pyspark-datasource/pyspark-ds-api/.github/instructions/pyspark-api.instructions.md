---
applyTo: "src/**/*.py"
---

# PySpark REST API Ingestion Patterns

## SparkSession

Every standalone script creates a SparkSession using the `SPARK_MASTER` env var
with a `local[*]` fallback. Set `JAVA_HOME` from `JAVA_HOME_11` and
`PYSPARK_PYTHON` to the current interpreter:

```python
import os
import sys

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("REST_API_Ingestion")
    .master(os.environ.get("SPARK_MASTER", "local[*]"))
    .getOrCreate()
)
```

Always call `spark.stop()` at the end of standalone scripts.

## APIClient Class

The core `APIClient` in `rest_api.py` encapsulates request building, auth,
pagination, retry, and response handling:

```python
class APIClient:
    def __init__(self, url: str, options: dict):
        self.url = url
        self.options = options
        self._build_components()      # headers, auth, body, query params, certs

    def make_request(self, extra_params: dict | None = None) -> requests.Response:
        """Execute HTTP request with retry logic."""
        ...
```

### Key conventions

- Constructor receives `url` and flat `options` dict extracted from YAML config.
- `_build_components()` parses auth, headers, body, query params, and certs.
- `make_request()` returns a raw `requests.Response`; caller handles parsing.
- Response format handling (`json`, `xml`, `csv`) is done after `make_request()`.

## Authentication Patterns

Auth type is determined by the `options.authentication.type` field in config.
The `_get_auth_headers()` method returns a `(headers_dict, auth_obj)` tuple.

### Basic Auth

```python
from requests.auth import HTTPBasicAuth

auth = HTTPBasicAuth(username, password)
# headers = {}, auth_obj = auth
```

### Bearer / JWT

```python
headers = {"Authorization": f"Bearer {token}"}
# auth_obj = None
```

### JWT Assertion (RS256)

For OAuth2 assertion flow, generate a JWT signed with a private key:

```python
import jwt
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.primitives.hashes import SHA1

# Load x509 cert for x5t thumbprint
cert = load_pem_x509_certificate(cert_pem)
fingerprint = cert.fingerprint(SHA1())
x5t = base64.urlsafe_b64encode(fingerprint).decode().rstrip("=")

# Build assertion
payload = {"iss": client_id, "sub": client_id, "aud": token_url, "exp": now + 600}
assertion = jwt.encode(payload, private_key, algorithm="RS256", headers={"x5t": x5t, "kid": kid})
```

### OAuth2 Flows

All OAuth2 flows POST to a token endpoint and extract `access_token`:

| Flow | Config `type` | Body format |
|------|---------------|-------------|
| Client credentials (JSON) | `oauth2_client_credentials_json` | JSON body |
| Client credentials (form) | `oauth2_client_credentials_form` | Form-encoded body |
| Client credentials (basic) | `oauth2_client_credentials_basic` | Basic auth + form body |
| Password (form) | `oauth2_password_form` | Form with username/password |
| Password (JSON) | `oauth2_password_json` | JSON with username/password |
| Assertion | `oauth2_assertion` | JWT assertion + form body |

```python
# Common pattern for all OAuth2 flows
token_response = requests.post(token_url, json=body)
token_response.raise_for_status()
access_token = token_response.json()["access_token"]
headers = {"Authorization": f"Bearer {access_token}"}
```

### API Key

```python
# Header-based
headers = {key_name: api_key_value}

# Query parameter-based
query_params[key_name] = api_key_value
```

### mTLS (Mutual TLS)

```python
cert = (cert_file_path, key_file_path)
response = requests.get(url, cert=cert, verify=ca_cert_path)
```

### Certificate Utilities

Generate and validate X.509 certificates for mTLS:

```python
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.primitives.hashes import SHA1

cert = load_pem_x509_certificate(pem_data)
fingerprint = cert.fingerprint(SHA1())
x5t = base64.urlsafe_b64encode(fingerprint).decode().rstrip("=")
```

## Pagination Strategies

Pagination uses an OOP hierarchy with a `PaginationFactory`:

```python
class Paginator:             # abstract base
class OffsetPaginator        # limit/offset increment
class OffsetPageTokenPaginator  # opaque page token from response
class PageNumberPaginator    # page number increment, total_pages termination
class CursorPaginator        # cursor from response, null = done
class LinkHeaderPaginator    # parse Link header for rel="next"
```

Factory dispatch:

```python
paginator = PaginationFactory.get_paginator(client, strategy="cursor", **kwargs)
all_data = paginator.fetch_all()
```

### Cursor-Based Pagination

```python
cursor = None
while True:
    params = {"cursor": cursor, "limit": limit}
    response = client.make_request(extra_params=params)
    data = response.json()
    records = data.get(result_key, [])
    if not records:
        break
    all_data.extend(records)
    cursor = data.get(next_cursor_key)
    if not cursor:
        break
```

### Offset-Based Pagination

```python
offset = 0
while True:
    params = {offset_key: offset, limit_key: limit}
    response = client.make_request(extra_params=params)
    records = response.json().get(result_key, [])
    if not records:
        break
    all_data.extend(records)
    offset += limit
```

### Page-Number Pagination

```python
page = 1
while True:
    params = {page_number_key: page, page_size_key: page_size}
    response = client.make_request(extra_params=params)
    data = response.json()
    records = read_key_value(data, result_key)
    all_data.extend(records)
    total_pages = read_key_value(data, f"{metadata_prefix}.{total_pages_key}")
    if page >= total_pages:
        break
    page += 1
```

### Dot-Path JSON Accessor

`read_key_value()` navigates nested JSON using dot-notation:

```python
def read_key_value(data: dict, key_path: str) -> any:
    """Traverse nested dict using dot-separated key path."""
    keys = key_path.split(".")
    for key in keys:
        data = data[key]
    return data

# Usage: read_key_value(response_json, "meta.pagination.total_pages")
```

## Response Format Handling

Parse API responses based on `responseFormat` config:

```python
response_format = options.get("responseFormat", "json")

if response_format == "json":
    data = response.json()
elif response_format == "xml":
    # Parse XML to dict
    ...
elif response_format == "csv":
    # Parse CSV string to list of dicts
    ...
```

JSON is the default format. Extract data arrays using `result_key` dot-notation.

## Parallel Ingestion Patterns

### ThreadPoolExecutor + Spark DataFrame

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as executor:
    pages = list(executor.map(fetch_page, page_urls))

all_records = [record for page in pages for record in page]
rdd = spark.sparkContext.parallelize(all_records)
json_rdd = rdd.map(json.dumps)
df = spark.read.schema(schema).json(json_rdd)
```

### Spark-Native Parallelism

```python
page_numbers = list(range(1, total_pages + 1))
rdd = spark.sparkContext.parallelize(page_numbers, numSlices=10)
data_rdd = rdd.flatMap(lambda page: fetch_page_data(url, page))
json_rdd = data_rdd.map(json.dumps)
df = spark.read.schema(schema).json(json_rdd)
```

### Creating DataFrames from API Data

Prefer `spark.read.schema().json(rdd)` for typed DataFrames:

```python
# ✅ Preferred — schema-aware
records = [json.dumps(r) for r in all_records]
rdd = spark.sparkContext.parallelize(records)
df = spark.read.schema(schema).json(rdd)

# Also valid — inferred schema
df = spark.createDataFrame(all_records)
```

## YAML Configuration Structure

ETL configs follow a nested YAML structure under `extracts.extract.source.params`:

```yaml
extracts:
  extract:
    source:
      params:
        location: "https://api.example.com/data"
        options:
          method: "GET"
          authentication:
            type: "basic"
            username: "user"
            password: "pass"
          pagination:
            strategy: "cursor"
            limit: 100
            result_key: "data"
            cursor_key: "cursor"
            next_cursor_key: "next_cursor"
          headers:
            Accept: "application/json"
          queryParams:
            status: "active"
          body:
            type: "json"
            content:
              filter: "active"
          responseFormat: "json"
          retries:
            max_attempts: 3
            timeout: 30
          filepath: "output.json"
```

Load configs with `yaml.safe_load`:

```python
from pathlib import Path
import yaml

config_path = Path(__file__).parents[0] / "etl_config.yaml"
with open(file=config_path, mode="r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

params = config["extracts"]["extract"]["source"]["params"]
url = params["location"]
options = params["options"]
```

## Error Handling

### Retry Pattern

```python
max_attempts = options.get("retries", {}).get("max_attempts", 3)
timeout = options.get("retries", {}).get("timeout", 30)

for attempt in range(max_attempts):
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        if attempt == max_attempts - 1:
            raise
        print(f"Attempt {attempt + 1} failed: {e}")
```

### Certificate Error Handling

```python
import socket
import ssl

try:
    validate_certificate(host, port, ca_cert)
except (socket.error, ssl.SSLError) as e:
    print(f"Certificate validation failed: {e}")
```

## Schema Inference and Persistence

Read and write Spark schemas as JSON files:

```python
from pyspark.sql.types import StructType

# Read schema from file
with open("schema.json", "r") as f:
    schema = StructType.fromJson(json.load(f))

# Write schema to file
with open("schema_output.json", "w") as f:
    f.write(df.schema.json())
```

## Streaming API Source

Streaming sources implement a continuous data fetch pattern for Spark Structured
Streaming, polling the API at configurable intervals.

## Mock API Servers (FastAPI)

Each authentication and pagination strategy includes a companion FastAPI server
for local testing:

```python
from fastapi import FastAPI, Depends
from faker import Faker
import uvicorn

app = FastAPI()
fake = Faker()

@app.get("/api/data")
def get_data():
    return [{"id": i, "name": fake.name()} for i in range(10)]

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

These servers use `Faker` for test data, `SQLModel` + SQLite for stateful
pagination, and `Pydantic` models for response schemas.

## File Output

The `FileWriter` class handles writing API responses to files:

```python
class FileWriter:
    @staticmethod
    def write_json_response_to_file(filepath, data):
        """Write list of dicts as JSONL (one JSON object per line)."""
        with open(filepath, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")
```

## DataFrame Function Style

Use `from pyspark.sql import functions as F` and method chaining:

```python
result = (
    df
    .filter(F.col("status") == "active")
    .select("id", "name", "email")
    .orderBy(F.desc("id"))
)
```
