# Copilot Instructions — pyspark-ds-api

This project is a comprehensive REST API ingestion framework for Apache Spark.
It fetches data from REST APIs — handling authentication, pagination, and
response parsing — and loads the results into PySpark DataFrames. Every example
is self-contained and runnable locally — no cluster required.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark-api.instructions.md` | `src/**/*.py` | REST API + Spark ingestion patterns |
| `instructions/testing.instructions.md` | `tests/**/*.py` | pytest conventions, SparkSession fixture, assertions |
| `instructions/project-config.instructions.md` | `pyproject.toml`, `uv.lock`, `.python-version` | Package manager and project metadata |

## Project Overview

The framework supports end-to-end REST API data ingestion into Spark:

- **Authentication** — Basic, Bearer/JWT, API Key (header/query), mTLS,
  OAuth2 (client credentials, password, assertion, authorization code, device code)
- **Pagination** — cursor-based, offset-based (simple + page token), page-number
- **Response Formats** — JSON (default), XML, CSV parsing into DataFrames
- **Parallel Ingestion** — ThreadPoolExecutor for page fetching + Spark RDD
  partitions for distributed processing
- **Streaming** — Streaming API data source
- **Configuration** — YAML config files with nested auth, pagination, header,
  query param, body, retry, and response format settings
- **Certificate Generation** — mTLS client certificate and CA generation utilities

## Technology Stack

| Component | Version / Tool |
|-----------|---------------|
| Python | ≥ 3.11 |
| PySpark | ≤ 3.5.5 |
| Package manager | uv |
| HTTP clients | requests, httpx |
| Auth libraries | authlib, pyjwt, cryptography |
| Mock API servers | FastAPI + uvicorn + SQLModel |
| Test data | Faker, pandas |
| Testing | pytest + pytest-sugar |
| Linting | black, flake8, isort |

## Project Structure

```
pyspark-ds-api/
├── .github/
│   ├── copilot-instructions.md          ← you are here
│   └── instructions/
│       ├── python.instructions.md
│       ├── pyspark-api.instructions.md
│       ├── testing.instructions.md
│       └── project-config.instructions.md
├── src/
│   ├── rest_api.py                      # Core APIClient, Paginator hierarchy, FileWriter
│   ├── authentication/                  # Auth strategies + mock FastAPI servers
│   │   ├── auth_util.py                     # get_auth_headers() dispatcher
│   │   ├── basic/                           # HTTP Basic auth
│   │   ├── jwt/                             # Bearer/JWT with RS256 assertions
│   │   ├── oauth2/                          # OAuth2 flows (client_creds, password, assertion)
│   │   ├── api_key/                         # API Key (header / query param)
│   │   ├── mtls/                            # Mutual TLS (client certificates)
│   │   └── certificates/                    # Certificate generation utilities
│   ├── paginated/                       # Pagination strategies + mock servers
│   │   ├── cursor/                          # Cursor-based pagination
│   │   ├── offset/                          # Offset-based (simple + page token)
│   │   └── page_number/                     # Page-number pagination
│   ├── ingestion/                       # PySpark REST API ingestion patterns
│   │   ├── parallel_ingestion.py            # ThreadPoolExecutor + executor.map
│   │   ├── parallel_ingestion_page.py       # ThreadPoolExecutor + submit/result
│   │   ├── parallel_with_spark_partitions.py # RDD parallelize + flatMap
│   │   └── pyspark_rest_optimized.py        # Sequential fetch + RDD → DataFrame
│   ├── streaming_source/                # Streaming API data source
│   ├── generate/                        # Test data generation (CSV, JSON)
│   ├── config/                          # YAML config loading
│   ├── schema/                          # JSON schema inference + persistence
│   ├── headers/                         # API request headers
│   ├── query_param/                     # Query parameter handling
│   └── util/                            # Shared utilities
│       ├── api_client.py                    # make_request, fetch_paginated_data
│       ├── request_builder.py               # build_request_components
│       ├── config_loader.py                 # YAML config loader
│       ├── data_processor.py                # create_dataframe_json
│       ├── certificate_util.py              # X.509 cert utilities
│       └── main.py                          # CLI entry point
├── tests/
│   └── test_rest_api.py                 # Unit tests
├── *.json                               # ETL config files (root level)
├── *.yaml                               # ETL config files (in submodules)
├── pyproject.toml
├── uv.lock
├── .python-version
└── README.md
```

## Quick Reference

```bash
uv sync                                    # install dependencies
uv run pytest                              # run all tests
uv run pytest tests/ -v --tb=short         # verbose test output
uv run python src/rest_api.py              # run the core API client
uv run python src/ingestion/parallel_ingestion.py  # run parallel ingestion
```

## Things to Avoid

- Do **not** use `from pyspark.sql.functions import *` — always `import functions as F`.
- Do **not** leave `spark.stop()` out of standalone scripts.
- Do **not** use `print(df.schema)` — use `df.printSchema()` instead.
- Do **not** use `len(df.collect())` — use `df.count()` for row counts.
- Do **not** hardcode API credentials — load them from config files or environment variables.
- Do **not** skip `response.raise_for_status()` after HTTP calls.
- Do **not** use bare `except:` — catch specific exceptions (`requests.HTTPError`, `ssl.SSLError`).
- Do **not** mix authentication strategies in a single request — use one auth type per config.
- Do **not** use RDD APIs when a DataFrame equivalent exists — prefer `spark.read.schema().json()` over manual RDD transformations.
- Do **not** use pip directly — always use `uv` for dependency management.
