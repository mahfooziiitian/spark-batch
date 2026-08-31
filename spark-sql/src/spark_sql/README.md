# dbx_uc — Databricks Copilot MCP Server

A local [Model Context Protocol](https://modelcontextprotocol.io/) server that exposes
Unity Catalog metadata, `system.*` observability queries, and catalog grant management
as tools for GitHub Copilot Chat (Agent Mode) in VS Code. Authenticates to Databricks
with a Personal Access Token (PAT), which stays local to this server and is never seen
by the Copilot model.

Full architecture, setup, and usage guide: [`docs/mcp-server.md`](../../docs/mcp-server.md)
(or via `uv run task docs_serve` → **MCP Server**).

## Quick start

```bash
# From repo root
uv sync
cp .env.example .env               # then fill in DATABRICKS_HOST / TOKEN / WAREHOUSE_ID
uv run dbx-copilot-mcp             # sanity-check the server starts
```

VS Code picks up the server automatically via `.vscode/mcp.json` (command:
`uv run dbx-copilot-mcp`).

## Layout

```text
server.py           # FastMCP app — registers @mcp.tool() wrappers
config/settings.py  # pydantic-settings, reads .env
clients/            # WorkspaceClient factory (the only place PAT auth happens)
tools/               # One module per capability: catalogs, schemas, tables,
                     # system_tables (read-only SQL), grants
```

## Tools

| Tool | Description |
|------|--------------|
| `list_catalogs` | List Unity Catalog catalogs |
| `list_schemas` | List schemas in a catalog |
| `list_tables` | List tables in a schema |
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
