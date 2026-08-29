# Examples

Runnable demos of the PySpark 4 Python Data Source API, built on the
`custom_ds` library in `src/`. Each script is self-contained and runs
locally with `local[*]`.

| Folder | Demonstrates |
|--------|--------------|
| `01_batch_read/` | Registering and reading a partitioned batch `DataSource` |
| `02_batch_write/` | Writing a DataFrame via a custom `DataSourceWriter` sink |
| `03_streaming/` | Streaming reads with `SimpleDataSourceStreamReader` |
| `04_sql/` | Querying a custom data source through Spark SQL temp views |
| `05_restapi_read/` | Batch reading from a REST API endpoint (HTTP GET → DataFrame) |
| `06_restapi_write/` | Batch writing to a REST API endpoint (DataFrame → HTTP POST) |
| `07_restapi_stream/` | Streaming reads from a REST API with offset tracking |
| `08_restapi_sql/` | SQL analytics over REST API data via temp views |
| `09_restapi_partitioned/` | Parallel reads: URL-based and page-based partitioning |
| `10_community_sources/` | Using `pyspark-data-sources` community connectors (Fake, GitHub) |
| `11_uc_http_auth/` | Unity Catalog HTTP connection — secure credential injection |
| `12_oauth2/` | OAuth2 authentication (client credentials, bearer token) |
| `13_databricks_connect/` | Run REST API examples on remote clusters via Databricks Connect |
| `14_restapi_weather/` | Live OpenWeatherMap API — read, multi-city, SQL, streaming |
| `15_restapi_databricks/` | Databricks REST API — list jobs, clusters, and job runs |
| `mock_server/` | FastAPI mock server used by examples 05–09, 12 |

## Run (simple in-memory sources)

```bash
uv run python examples/01_batch_read/read_simple_source.py
uv run python examples/02_batch_write/write_simple_sink.py
uv run python examples/03_streaming/stream_simple_source.py
uv run python examples/04_sql/query_custom_source_with_sql.py
```

## Run (REST API examples)

Start the mock server in one terminal, then run examples:

```bash
# Terminal 1 — start mock API
uv run python examples/mock_server/server.py

# Terminal 2 — run REST API examples
uv run python examples/05_restapi_read/read_restapi_source.py
uv run python examples/06_restapi_write/write_restapi_sink.py
uv run python examples/07_restapi_stream/stream_restapi_source.py
uv run python examples/08_restapi_sql/query_restapi_with_sql.py
uv run python examples/09_restapi_partitioned/partitioned_restapi_read.py
```

## Run (community data sources — `pyspark-data-sources` package)

These examples use the [`pyspark-data-sources`](https://github.com/allisonwang-db/pyspark-data-sources)
community library. Install with `uv sync` (already in dev deps).

```bash
# Fake data + GitHub PRs
uv run python examples/10_community_sources/fake_and_github.py

# Streaming fake data
uv run python examples/10_community_sources/streaming_fake_data.py

# SQL queries over synthetic data
uv run python examples/10_community_sources/sql_with_fake_data.py

# Mixed pipeline (community + custom) — requires mock server running
uv run python examples/10_community_sources/mixed_pipeline.py
```

## Run (Databricks Connect — remote cluster execution)

Execute REST API examples from your local IDE on a remote Databricks cluster.
Requires `databricks-connect` (`uv sync --extra databricks`) and a configured cluster.

```bash
# Set connection (or use DATABRICKS_PROFILE)
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_CLUSTER_ID="<cluster-id>"
export DATABRICKS_TOKEN="<pat-token>"
export REST_API_URL="http://<api-host>/api/users"

# Batch read
uv run python examples/13_databricks_connect/dbconnect_batch_read.py

# Batch write
uv run python examples/13_databricks_connect/dbconnect_batch_write.py

# SQL queries
uv run python examples/13_databricks_connect/dbconnect_sql.py
```

## Run (OpenWeatherMap — live API examples)

These examples hit the real OpenWeatherMap API. Get a free key at
[openweathermap.org/api](https://openweathermap.org/api).

```bash
export OPENWEATHER_API_KEY=<your-api-key>

# Single city weather read
uv run python examples/14_restapi_weather/weather_read.py

# Multiple cities — parallel URL-based partitioning
uv run python examples/14_restapi_weather/weather_multi_city.py

# SQL analytics over weather data
uv run python examples/14_restapi_weather/weather_sql.py

# Streaming weather updates (polls every 30s for 2 minutes)
uv run python examples/14_restapi_weather/weather_stream.py
```

## Run (Databricks REST API — workspace metadata)

Query your Databricks workspace metadata (jobs, clusters, runs) using the
REST API data source with bearer token auth.

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...

# List all jobs with creator and format analytics
uv run python examples/15_restapi_databricks/list_jobs.py

# List clusters with state and version breakdown
uv run python examples/15_restapi_databricks/list_clusters.py

# Recent job runs with success rate analytics
uv run python examples/15_restapi_databricks/list_job_runs.py
```
