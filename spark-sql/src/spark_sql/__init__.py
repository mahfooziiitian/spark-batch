"""``spark_sql`` — Python support package for the ``sql/`` examples in this repository.

Contains the SQL test helpers (:mod:`spark_sql._helpers`) that execute and
validate the ``sql/`` examples, the backend-agnostic runner/examiner/validator
library (:mod:`spark_sql.runner`), the ``dbx_mcp`` Databricks MCP server
(:mod:`spark_sql.dbx_mcp`, entry point ``dbx-copilot-mcp``), and the
``util``/``model`` CLI tooling used by dashboard deployment scripts.

This package has no import-time side effects.
"""
