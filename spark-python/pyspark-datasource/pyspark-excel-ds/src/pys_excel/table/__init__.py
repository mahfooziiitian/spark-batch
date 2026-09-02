"""Excel ↔ Spark table bridge — the core "read Excel, write into table" workflow."""

from pys_excel.table._table import excel_to_table, table_to_excel, upsert_table_from_excel

__all__ = ["excel_to_table", "table_to_excel", "upsert_table_from_excel"]
