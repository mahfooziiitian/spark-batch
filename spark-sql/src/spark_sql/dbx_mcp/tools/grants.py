import logging

from databricks.sdk.service.catalog import (
    PermissionsChange,
    Privilege,
    SecurableType,
)

from spark_sql.dbx_mcp.clients.databricks_client import (
    get_workspace_client,
)

logger = logging.getLogger(__name__)


def show_grants(
    catalog_name: str,
) -> list[dict]:
    """
    Show current privilege assignments on a Unity Catalog catalog.
    """

    logger.info("Showing grants on catalog=%s", catalog_name)
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

    logger.info("Granting %s on catalog=%s to principal=%s", privileges, catalog_name, principal)
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

    logger.info("Granted %s on catalog=%s to principal=%s", privileges, catalog_name, principal)
    return f"Granted {privileges} on catalog '{catalog_name}' to '{principal}'"


def revoke_catalog_privileges(
    catalog_name: str,
    principal: str,
    privileges: list[str],
) -> str:
    """
    Revoke privileges on a catalog from a principal.
    """

    logger.info("Revoking %s on catalog=%s from principal=%s", privileges, catalog_name, principal)
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

    logger.info("Revoked %s on catalog=%s from principal=%s", privileges, catalog_name, principal)
    return f"Revoked {privileges} on catalog '{catalog_name}' from '{principal}'"
