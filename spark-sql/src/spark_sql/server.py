from mcp.server.fastmcp.server import FastMCP

from spark_sql.tools.catalogs import list_catalogs as list_catalogs_impl
from spark_sql.tools.grants import (
    grant_catalog_privileges as grant_catalog_privileges_impl,
)
from spark_sql.tools.grants import (
    revoke_catalog_privileges as revoke_catalog_privileges_impl,
)
from spark_sql.tools.grants import show_grants as show_grants_impl
from spark_sql.tools.schemas import list_schemas as list_schemas_impl
from spark_sql.tools.system_tables import query_system_table as query_system_table_impl
from spark_sql.tools.tables import list_tables as list_tables_impl

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


def main():
    mcp.run()


if __name__ == "__main__":
    main()
