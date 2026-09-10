# dbx_mcp — Databricks Copilot MCP Server

A local [Model Context Protocol](https://modelcontextprotocol.io/) server that exposes
Unity Catalog metadata, `system.*` observability queries, and catalog grant management
as tools for GitHub Copilot Chat (Agent Mode) in VS Code. Authenticates to Databricks
with a Personal Access Token (PAT) or a named CLI config profile — either stays local
to this server and is never seen by the Copilot model.

Full architecture, setup, and usage guide: [`docs/mcp-server.md`](../../docs/mcp-server.md)
(or via `uv run task docs_serve` → **MCP Server**).

## Quick start

```bash
# From repo root
uv sync
cp config/.env.example .env       # then fill in DATABRICKS_HOST / TOKEN / WAREHOUSE_ID (or WAREHOUSE_NAME)
                                    # (or set DATABRICKS_CONFIG_PROFILE to use ~/.databrickscfg)
uv run dbx-copilot-mcp             # sanity-check the server starts
```

VS Code picks up the server automatically via `.mcp.json` (command: `uv run dbx-copilot-mcp`).

`query_system_table` needs a SQL warehouse to run against. Set either `DATABRICKS_WAREHOUSE_ID`
directly, or `DATABRICKS_WAREHOUSE_NAME` — the server resolves the name to an ID on demand via
the SQL Warehouses API (`tools/warehouses.py`), so only one of the two is required.

## Environments (`DAB_TARGET`)

The server resolves `dev`/`staging`/`prod` the same way `make dab-*` does: `config/settings.py`
loads `.env` (base) then layers `.env.$(DAB_TARGET)` on top (see `dashboard.util.cli_env.load_env_layers`),
defaulting to `dev` when `DAB_TARGET` is unset. `settings.dab_target` exposes the resolved value.

```bash
DAB_TARGET=dev     uv run dbx-copilot-mcp
DAB_TARGET=staging uv run dbx-copilot-mcp
DAB_TARGET=prod    uv run dbx-copilot-mcp
```

From your editor, `.mcp.json` forwards `DAB_TARGET` (`${env:DAB_TARGET}`) — export it in the
shell/session your editor launches from to control which target the server picks up (GUI-launched
editors don't otherwise inherit a terminal's exported vars). Leave it unset to default to `dev`.

## Authentication

Two mutually exclusive options, resolved in `clients/databricks_client.py`:

| Method | Env var(s) | Notes |
|--------|------------|-------|
| Config profile | `DATABRICKS_CONFIG_PROFILE` | Named profile from `~/.databrickscfg` (e.g. via `databricks configure --profile <name>`). Takes priority when set. |
| Host + PAT | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | Used when `DATABRICKS_CONFIG_PROFILE` is not set. |


## Layout

```text
server.py           # FastMCP app — registers @mcp.tool() wrappers
config/settings.py  # pydantic-settings, reads .env
clients/            # WorkspaceClient factory (the only place PAT auth happens)
tools/               # One module per capability: catalogs, schemas, tables,
                     # warehouses (list + name→id resolution),
                     # system_tables (read-only SQL), grants
```

## Tools

| Tool | Description |
|------|--------------|
| `list_catalogs` | List Unity Catalog catalogs |
| `list_schemas` | List schemas in a catalog |
| `list_tables` | List tables in a schema |
| `list_warehouses` | List SQL warehouses with their ID, name, and state |
| `get_warehouse_id` | Resolve a SQL warehouse display name to its ID |
| `query_system_table` | Read-only SQL against `system.*` / `information_schema.*` (SELECT/SHOW/DESCRIBE/WITH/EXPLAIN only) |
| `show_grants` | Show privilege assignments on a catalog |
| `grant_catalog_privileges` | Grant specific privileges to a principal |
| `revoke_catalog_privileges` | Revoke specific privileges from a principal |

## Testing

```bash
uv run task test_mcp
```

Tests live in `tests/test_tools.py` and mock `WorkspaceClient` via `pytest-mock` — no
tool test should hit a real Databricks workspace.
