"""Databricks Copilot MCP server package.

Exposes Unity Catalog and SQL warehouse read/administration operations (list
catalogs/schemas/tables/warehouses, grant/revoke/show catalog privileges, query
system tables) as MCP tools over stdio, for use by the Copilot CLI/IDE. See
:mod:`spark_sql.dbx_mcp.server` for the tool definitions and entry point.
"""
