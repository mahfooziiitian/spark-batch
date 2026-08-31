from spark_sql.clients.databricks_client import get_workspace_client


def list_schemas(
    catalog_name: str,
) -> list[dict]:

    workspace_client = get_workspace_client()

    return [
        {
            "name": schema.name,
            "catalog_name": schema.catalog_name,
            "comment": schema.comment,
        }
        for schema in workspace_client.schemas.list(
            catalog_name=catalog_name,
        )
    ]
