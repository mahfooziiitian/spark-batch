from spark_sql.clients.databricks_client import get_workspace_client


def list_catalogs() -> list[dict]:
    workspace_client = get_workspace_client()

    return [
        {
            "name": catalog.name,
            "comment": catalog.comment,
        }
        for catalog in workspace_client.catalogs.list()
    ]
