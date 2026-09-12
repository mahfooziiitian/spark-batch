"""FastMCP stdio server exposing Unity Catalog / SQL warehouse tools.

Wraps each ``spark_sql.dbx_mcp.tools.*`` implementation function as an
``@mcp.tool()`` for consumption by an MCP host (Copilot CLI/IDE). See
``.mcp.json`` for the stdio launch configuration and ``dbx-copilot-mcp`` entry
point wiring.
"""

import logging

from mcp.server.fastmcp import FastMCP

from spark_sql.dbx_mcp.tools.catalogs import list_catalogs as list_catalogs_impl
from spark_sql.dbx_mcp.tools.grants import grant_catalog_privileges as grant_catalog_privileges_impl
from spark_sql.dbx_mcp.tools.grants import revoke_catalog_privileges as revoke_catalog_privileges_impl
from spark_sql.dbx_mcp.tools.grants import show_grants as show_grants_impl
from spark_sql.dbx_mcp.tools.schemas import list_schemas as list_schemas_impl
from spark_sql.dbx_mcp.tools.system_tables import query_system_table as query_system_table_impl
from spark_sql.dbx_mcp.tools.tables import list_tables as list_tables_impl
from spark_sql.dbx_mcp.tools.warehouses import get_warehouse_id as get_warehouse_id_impl
from spark_sql.dbx_mcp.tools.warehouses import list_warehouses as list_warehouses_impl

# NOTE: this server communicates over stdio (see .mcp.json) — logging must never write to
# stdout, or it would corrupt the MCP JSON-RPC stream. ``basicConfig`` defaults to stderr,
# which is safe; do not pass ``stream=sys.stdout`` here or anywhere else in this package.
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

mcp = FastMCP(
    "Databricks Copilot MCP",
)


@mcp.tool()
def list_catalogs() -> list[dict]:
    """
    List Unity Catalog catalogs available to the configured
    Databricks PAT.
    """

    return list_catalogs_impl()


@mcp.tool()
def list_schemas(
    catalog_name: str,
) -> list[dict]:
    """
    List schemas in a Unity Catalog catalog.
    """

    return list_schemas_impl(
        catalog_name=catalog_name,
    )


@mcp.tool()
def list_tables(
    catalog_name: str,
    schema_name: str,
) -> list[dict]:
    """
    List tables in a Unity Catalog schema.
    """

    return list_tables_impl(
        catalog_name=catalog_name,
        schema_name=schema_name,
    )


@mcp.tool()
def list_warehouses() -> list[dict]:
    """
    List SQL warehouses visible to the configured Databricks PAT, with
    their ID, name, and current state.
    """

    return list_warehouses_impl()


@mcp.tool()
def get_warehouse_id(
    warehouse_name: str,
) -> str | None:
    """
    Resolve a SQL warehouse display name to its ID. Returns None if no
    warehouse with that name exists in the workspace.
    """

    return get_warehouse_id_impl(
        warehouse_name=warehouse_name,
    )


@mcp.tool()
def query_system_table(
    sql: str,
    row_limit: int = 500,
) -> list[dict]:
    """
    Run a read-only SQL query against Databricks system tables or
    information_schema (audit, billing/usage, lineage, volumes, routines).
    Only SELECT/SHOW/DESCRIBE/WITH/EXPLAIN statements are permitted.
    """

    return query_system_table_impl(
        sql=sql,
        row_limit=row_limit,
    )


@mcp.tool()
def show_grants(
    catalog_name: str,
) -> list[dict]:
    """
    Show current privilege assignments on a Unity Catalog catalog.
    """

    return show_grants_impl(
        catalog_name=catalog_name,
    )


@mcp.tool()
def grant_catalog_privileges(
    catalog_name: str,
    principal: str,
    privileges: list[str],
) -> str:
    """
    Grant privileges on a catalog to a principal. Follow least-privilege:
    pass only the specific privileges needed rather than ALL_PRIVILEGES.
    """

    return grant_catalog_privileges_impl(
        catalog_name=catalog_name,
        principal=principal,
        privileges=privileges,
    )


@mcp.tool()
def revoke_catalog_privileges(
    catalog_name: str,
    principal: str,
    privileges: list[str],
) -> str:
    """
    Revoke privileges on a catalog from a principal.
    """

    return revoke_catalog_privileges_impl(
        catalog_name=catalog_name,
        principal=principal,
        privileges=privileges,
    )


def main() -> None:
    """Start the MCP server over stdio (blocks until the host disconnects)."""
    logger.info("Starting Databricks Copilot MCP server (stdio transport)")
    mcp.run()


if __name__ == "__main__":
    main()
