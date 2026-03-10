"""Auth and Databricks client utilities for the MCP server."""

import contextvars
import os

from databricks.sdk import WorkspaceClient

# Stores request headers per async context (set by middleware)
header_store: contextvars.ContextVar[dict] = contextvars.ContextVar("header_store", default={})


def get_workspace_client() -> WorkspaceClient:
    """Get a WorkspaceClient using the app's service principal (default auth)."""
    return WorkspaceClient()


def get_user_workspace_client() -> WorkspaceClient:
    """Get a WorkspaceClient authenticated as the end user (OBO).

    On Databricks Apps, the platform injects the user's OAuth token
    via the x-forwarded-access-token header.
    Falls back to default SDK auth for local development.
    """
    is_databricks_app = "DATABRICKS_APP_NAME" in os.environ

    if not is_databricks_app:
        return WorkspaceClient()

    headers = header_store.get({})
    token = headers.get("x-forwarded-access-token")

    if not token:
        raise ValueError(
            "Authentication token not found in request headers (x-forwarded-access-token). "
            "Ensure you are accessing this server through a Databricks App."
        )

    return WorkspaceClient(token=token, auth_type="pat")
