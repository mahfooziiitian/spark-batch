# Community Data Sources

The [`pyspark-data-sources`](https://github.com/allisonwang-db/pyspark-data-sources) package
provides ready-to-use Python Data Source connectors built by the community. These work
alongside our custom connectors — all use the same `pyspark.sql.datasource` API.

## Installation

```bash
uv add pyspark-data-sources
```

## Available Sources

| Source | Description | Auth Required |
|--------|-------------|---------------|
| `FakeDataSource` | Synthetic data via Faker | No |
| `GithubDataSource` | GitHub pull requests | Optional (token) |
| `JsonPlaceholderDataSource` | JSONPlaceholder REST API | No |
| `OpenSkyDataSource` | Real-time aircraft tracking | Optional |
| `StockDataSource` | Stock market data | API key |
| `WeatherDataSource` | Weather forecasts | API key |

## FakeDataSource — Synthetic Test Data

Generate realistic test data using [Faker](https://faker.readthedocs.io/) providers:

```python
from pyspark_datasources import FakeDataSource

spark.dataSource.register(FakeDataSource)

# Default schema: name, date, zipcode, state
df = spark.read.format("fake").option("numRows", 100).load()

# Custom schema — field names map to Faker provider methods
df = (
    spark.read.format("fake")
    .schema("name string, email string, company string, city string")
    .option("numRows", 50)
    .load()
)
df.show()
```

!!! tip "Streaming Mode"
    FakeDataSource also supports streaming reads — great for testing streaming pipelines:
    ```python
    df = (
        spark.readStream.format("fake")
        .schema("name string, email string")
        .option("rowsPerMicrobatch", 10)
        .load()
    )
    ```

## GithubDataSource — Pull Requests

Read pull request data from any public GitHub repository:

```python
from pyspark_datasources import GithubDataSource

spark.dataSource.register(GithubDataSource)

# Public repo — no token needed
df = spark.read.format("github").load("apache/spark")
df.select("id", "title", "author", "created_at").show(5)

# Private repo — use a personal access token
df = (
    spark.read.format("github")
    .option("token", "ghp_your_token")
    .load("owner/private-repo")
)
```

## Mixed Pipelines

Combine community and custom sources in one session:

```python
from custom_ds import RestApiDataSource
from pyspark_datasources import FakeDataSource

spark.dataSource.register(FakeDataSource)
spark.dataSource.register(RestApiDataSource)

# Generate test data
df_fake = spark.read.format("fake").schema("name string").option("numRows", 10).load()

# Read from REST API
df_api = spark.read.format("restapi").option("url", "http://api/users").load()

# Union and analyze
df_fake.unionByName(df_api).createOrReplaceTempView("all_users")
spark.sql("SELECT * FROM all_users").show()
```

## Running the Examples

```bash
# Fake data + GitHub
uv run python examples/10_community_sources/fake_and_github.py

# Streaming synthetic data
uv run python examples/10_community_sources/streaming_fake_data.py

# SQL queries over fake data
uv run python examples/10_community_sources/sql_with_fake_data.py

# Mixed pipeline (needs mock server)
uv run python examples/mock_server/server.py &
uv run python examples/10_community_sources/mixed_pipeline.py
```
