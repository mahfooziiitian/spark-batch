from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType

from spark_sql.clients.databricks_client import get_workspace_client


def show_grants(
    catalog_name: str,
) -> list[dict]:
    """
    Show current privilege assignments on a Unity Catalog catalog.
    """

    workspace_client = get_workspace_client()

    permissions = workspace_client.grants.get(
        securable_type=SecurableType.CATALOG.value,
        full_name=catalog_name,
    )

    return [
        {
            "principal": assignment.principal,
            "privileges": [p.value for p in (assignment.privileges or [])],
        }
        for assignment in (permissions.privilege_assignments or [])
    ]


def grant_catalog_privileges(
    catalog_name: str,
    principal: str,
    privileges: list[str],
) -> str:
    """
    Grant privileges on a catalog to a principal.

    Follow least-privilege: pass only the specific privileges needed
    (e.g. ["USE_CATALOG", "USE_SCHEMA"]) rather than ALL_PRIVILEGES.
    """

    workspace_client = get_workspace_client()

    privilege_objects = [Privilege(privilege) for privilege in privileges]

    workspace_client.grants.update(
        securable_type=SecurableType.CATALOG.value,
        full_name=catalog_name,
        changes=[
            PermissionsChange(
                add=privilege_objects,
                principal=principal,
            )
        ],
    )

    return f"Granted {privileges} on catalog '{catalog_name}' to '{principal}'"


def revoke_catalog_privileges(
    catalog_name: str,
    principal: str,
    privileges: list[str],
) -> str:
    """
    Revoke privileges on a catalog from a principal.
    """

    workspace_client = get_workspace_client()

    privilege_objects = [Privilege(privilege) for privilege in privileges]

    workspace_client.grants.update(
        securable_type=SecurableType.CATALOG.value,
        full_name=catalog_name,
        changes=[
            PermissionsChange(
                remove=privilege_objects,
                principal=principal,
            )
        ],
    )

    return f"Revoked {privileges} on catalog '{catalog_name}' from '{principal}'"
