from databricks.sdk import WorkspaceClient

from spark_sql.config.settings import settings


def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient(
        host=settings.databricks_host,
        token=settings.databricks_token,
    )
