"""Implementation functions backing each ``dbx_mcp`` MCP tool.

Each module here (``catalogs``, ``schemas``, ``tables``, ``warehouses``,
``grants``, ``system_tables``) implements one group of Unity Catalog / SQL
warehouse operations, wrapped as an ``@mcp.tool()`` in
:mod:`spark_sql.dbx_mcp.server`. Kept separate from the server module so the
business logic is independently testable without an MCP host.
"""
