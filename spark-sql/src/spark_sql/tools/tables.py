from spark_sql.clients.databricks_client import get_workspace_client


def list_tables(
    catalog_name: str,
    schema_name: str,
) -> list[dict]:

    workspace_client = get_workspace_client()

    return [
        {
            "name": table.name,
            "full_name": table.full_name,
            "table_type": (table.table_type.value if table.table_type else None),
        }
        for table in workspace_client.tables.list(
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
    ]
