# Copilot Instructions — pyspark-ds-api

This project is a comprehensive REST API ingestion framework for Apache Spark.
It fetches data from REST APIs — handling authentication, pagination, and
response parsing — and loads the results into PySpark DataFrames. Every example
is self-contained and runnable locally — no cluster required.

The codebase is split into two top-level areas with different rules:

- **`src/`** — the library. Reusable, importable modules only: `APIClient`,
  `Paginator` classes, `auth_util`, `data_processor`, the incremental
  ingestion engine, etc. No mock servers, no standalone demo scripts, no
  scenario-specific YAML/JSON configs.
- **`examples/`** — usage/demo code. One runnable scenario per
  auth/pagination/incremental strategy, each with its own mock FastAPI
  server, ETL/runner script, and YAML/JSON config, importing library code
  from `src/`.

When adding a new feature: put the reusable logic in `src/`, and add (or
extend) a runnable demonstration of it under `examples/`.

## Modular Instruction Files

| File | Scope (`applyTo`) | Purpose |
|------|--------------------|---------|
| `instructions/python.instructions.md` | `**/*.py` | Python style, imports, type hints, docstrings |
| `instructions/pyspark-api.instructions.md` | `src/**/*.py`, `examples/**/*.py` | REST API + Spark ingestion patterns |
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
- **Incremental Ingestion** — YAML-driven watermark parameters injected into
  each API call, with run state (last watermark + full run history) tracked
  in a **database control table**, not a file — see the "Incremental
  Ingestion" section below
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
| Incremental state store | SQLModel (SQLAlchemy) — any DB URL: sqlite / postgresql / mysql |
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
├── src/                                  # LIBRARY ONLY — reusable, importable modules
│   ├── rest_api.py                      # Core APIClient, Paginator hierarchy, FileWriter
│   ├── authentication/
│   │   └── auth_util.py                     # get_auth_headers() dispatcher
│   ├── incremental/                     # Incremental (delta) ingestion + DB state store
│   │   ├── models.py                        # SQLModel: IngestionWatermark, IngestionRunHistory
│   │   ├── state_store.py                   # IncrementalStateStore (control-table CRUD)
│   │   ├── watermark.py                     # parse/format/lookback/compute_next_watermark
│   │   └── incremental_runner.py             # run_incremental_ingestion() orchestrator
│   ├── schema/
│   │   └── json_schema.py                   # read_json_schema, generate_schema_from_df
│   └── util/                            # Shared utilities
│       ├── api_client.py                    # make_request, fetch_paginated_data
│       ├── request_builder.py               # build_request_components
│       ├── config_loader.py                 # YAML config loader
│       ├── data_processor.py                # fetch_records, create_dataframe_json
│       └── certificate_util.py              # X.509 cert utilities
├── examples/                             # USAGE/DEMO — runnable scenarios, mock servers, configs
│   ├── authentication/                  # Auth strategies + mock FastAPI servers
│   │   ├── etl_extract.py                   # Standalone extract demo
│   │   ├── basic/                           # HTTP Basic auth
│   │   ├── jwt/                             # Bearer/JWT with RS256 assertions
│   │   ├── oauth2/                          # OAuth2 flows (client_creds, password, assertion,
│   │   │                                    #   authorization_code, device_code)
│   │   ├── api_key/                         # API Key (header / query param)
│   │   ├── mtls/                            # Mutual TLS (client certificates)
│   │   └── certificates/                    # Certificate generation demo + validation script
│   ├── paginated/                       # Pagination strategies + mock servers
│   │   ├── cursor/                          # Cursor-based pagination
│   │   ├── offset/                          # Offset-based (simple + page token)
│   │   └── page_number/                     # Page-number pagination
│   ├── ingestion/                       # PySpark REST API ingestion patterns
│   │   ├── parallel_ingestion.py            # ThreadPoolExecutor + executor.map
│   │   ├── parallel_ingestion_page.py       # ThreadPoolExecutor + submit/result
│   │   ├── parallel_with_spark_partitions.py # RDD parallelize + flatMap
│   │   └── pyspark_rest_optimized.py        # Sequential fetch + RDD → DataFrame
│   ├── incremental/                     # Incremental ingestion demo (uses src/incremental)
│   │   ├── mock_incremental_server.py        # FastAPI mock filtering by updated_since
│   │   ├── incremental_api_source.yaml       # Example config with `incremental` block
│   │   ├── run_incremental_example.py        # Runnable two-run end-to-end example
│   │   └── README.md                         # Design, control-table schema, config reference
│   ├── streaming_source/                # Streaming API data source (stub)
│   ├── generate/                        # Test data generation (CSV, JSON) (stubs)
│   ├── config/                          # YAML config loading demo (ds.yaml, yaml_config_api.py)
│   ├── schema/                          # JSON schema demo (demo_json_schema.py)
│   ├── headers/                         # API request headers (stub)
│   ├── query_param/                     # Query parameter handling (stub)
│   └── util/                            # Utility demos
│       ├── main.py                          # CLI entry point demo
│       └── demo_certificate_util.py         # Cert-fetch demo (uses src/util/certificate_util.py)
├── tests/
│   ├── conftest.py                      # Shared SparkSession fixture
│   ├── test_rest_api.py                 # Unit tests
│   ├── test_incremental_state.py        # Watermark + state-store unit tests
│   └── test_incremental_runner.py       # Incremental runner integration tests
├── pyproject.toml
├── uv.lock
├── .python-version
└── README.md
```

## Incremental Ingestion

For sources that support fetching only new/changed records (vs. a full
re-read every run), enable the `incremental` block in the source's YAML
config and drive the run through `run_incremental_ingestion()` instead of
`read_api()`:

```python
from incremental.incremental_runner import run_incremental_ingestion
from incremental.state_store import IncrementalStateStore

store = IncrementalStateStore("sqlite:///incremental_state.db")  # or postgresql+psycopg2://...
df = run_incremental_ingestion(spark, config, source_name="events_api", state_store=store)
```

Key rules for this pattern:

- **Run state lives in a database, never a file** — the `ingestion_watermark`
  (current pointer) and `ingestion_run_history` (audit log) tables, defined
  in `src/incremental/models.py` as `SQLModel` classes. See
  `examples/incremental/README.md` for the full schema and rationale.
- **The watermark only advances after a successful run.** A failed run is
  recorded in `ingestion_run_history` with `status="failed"`, but
  `ingestion_watermark` is left untouched so the next run retries the same
  window automatically — never advance the watermark before you know the
  fetch succeeded.
- **Always pass a `state_store`** with a real DB URL in production code
  (tests/examples may use `sqlite:///:memory:` or a `tmp_path` sqlite file).
- **Empty result sets are normal**, not errors — `create_dataframe_json()`
  returns a valid zero-row DataFrame when nothing new was fetched instead of
  raising on `spark.createDataFrame([])`.
- New YAML config keys live under `options.incremental` — see
  `examples/incremental/incremental_api_source.yaml` for the annotated
  reference and `pyspark-api.instructions.md` for full field documentation.

## Quick Reference

```bash
uv sync                                    # install dependencies
uv run pytest                              # run all tests
uv run pytest tests/ -v --tb=short         # verbose test output
PYTHONPATH=src uv run python examples/ingestion/parallel_ingestion.py       # run parallel ingestion demo
PYTHONPATH=src uv run python examples/incremental/mock_incremental_server.py &   # start mock API
PYTHONPATH=src uv run python examples/incremental/run_incremental_example.py     # run incremental example
```

> Note: scripts under `examples/` import library code by top-level package
> name (e.g. `from util.config_loader import ...`, `from incremental.state_store import ...`),
> so always run them with `PYTHONPATH=src` set. Scripts under `src/` never
> execute at import time — there are no runnable demos left in `src/`.

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
- Do **not** track incremental run state in a local file/variable — use
  `IncrementalStateStore` (a real DB table) so state survives across job
  runs, restarts, and different executors.
- Do **not** advance `ingestion_watermark` before the fetch completes
  successfully — always compute and persist the new watermark inside the
  success path, after the data has been retrieved.
- Do **not** add mock servers, `if __name__ == "__main__"` demo scripts, or
  scenario-specific YAML/JSON configs under `src/` — those belong in
  `examples/`. `src/` is library code only, imported by both `examples/`
  and `tests/`.

