# YAML config loading demo

A minimal, unauthenticated example showing the config-loading pattern shared
by every script in `examples/`: read a YAML extraction config with
`rest_ds.util.config_loader.load_config`, then hand it to
`rest_ds.rest_api.read_api`. For auth-specific or pagination-specific
demos, see `examples/authentication/` and `examples/paginated/` instead.

```text
[YAML CONFIG]
      |
   (load_config: read + expand ${DATA_HOME} + parse)
      |
   (read_api: fetch data from API)
      |
   (convert to Spark DataFrame)
      |
   (write result to filepath under ${DATA_HOME}/rest_api_ds)
```

## Run

```bash
# 1. Start the shared mock API (serves GET /items?page=N)
PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py &

# 2. Run the demo
PYTHONPATH=src uv run python examples/config/yaml_config_api.py
```

Output lands at `${DATA_HOME:-/tmp}/rest_api_ds/config_demo_ds.json`.
