"""Auth and Databricks client utilities for the MCP server.

Supports three authentication scenarios:
1. User via Databricks AI Playground (x-forwarded-access-token header)
2. Service Principal via direct HTTP (Authorization: Bearer header)
3. Local development (default SDK auth / profile)
"""

import contextvars
import logging
import os

from databricks.sdk import WorkspaceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Stores request headers per async context (set by middleware)
header_store: contextvars.ContextVar[dict] = contextvars.ContextVar("header_store", default={})


def get_workspace_client() -> WorkspaceClient:
    """Get a WorkspaceClient using the app's own service principal.

    Use this for accessing shared resources (KB tables) that the app
    should access regardless of caller identity.
    """
    logger.info("=" * 60)
    logger.info("AUTH: Creating WorkspaceClient with APP's Service Principal")
    logger.info("=" * 60)
    return WorkspaceClient()


def get_caller_workspace_client(headers: dict | None = None) -> WorkspaceClient:
    """Get a WorkspaceClient authenticated as the caller.

    Supports three scenarios:
    1. Databricks AI Playground: Uses x-forwarded-access-token header
    2. M2M Service Principal: Uses Authorization Bearer token
    3. Local development: Falls back to default SDK auth

    Args:
        headers: Request headers dict. If None, falls back to header_store ContextVar.
                 Pass explicitly when ContextVar may not be available (e.g., in FastMCP tools).

    Use this for actions that should run as the caller (file access, job creation).
    """
    is_databricks_app = "DATABRICKS_APP_NAME" in os.environ
    app_name = os.environ.get("DATABRICKS_APP_NAME", "unknown")

    logger.info("=" * 60)
    logger.info("AUTH: get_caller_workspace_client() called")
    logger.info(f"AUTH: Running as Databricks App: {is_databricks_app}")
    logger.info(f"AUTH: App name: {app_name}")
    logger.info(f"AUTH: Headers provided explicitly: {headers is not None}")
    logger.info("=" * 60)

    if not is_databricks_app:
        logger.info("AUTH: [LOCAL DEV] Not running as Databricks App")
        logger.info("AUTH: [LOCAL DEV] Using default SDK auth (CLI profile or env vars)")
        return WorkspaceClient()

    # Use provided headers or fall back to ContextVar (for backward compat)
    if headers is None:
        headers = header_store.get({})
        logger.info("AUTH: Using headers from ContextVar (header_store)")
    else:
        logger.info("AUTH: Using explicitly provided headers")

    # Log all headers (mask sensitive values)
    logger.info("AUTH: Request headers received:")
    for key, value in headers.items():
        if 'token' in key.lower() or 'authorization' in key.lower():
            masked_value = f"{value[:20]}...{value[-10:]}" if len(str(value)) > 30 else "***"
            logger.info(f"AUTH:   {key}: {masked_value}")
        else:
            logger.info(f"AUTH:   {key}: {value}")

    # Priority 1: Databricks UI injects user token via this header (OBO)
    token = headers.get("x-forwarded-access-token")
    if token:
        logger.info("-" * 60)
        logger.info("AUTH: [OBO - ON BEHALF OF USER]")
        logger.info("AUTH: Found 'x-forwarded-access-token' header")
        logger.info("AUTH: This is a USER accessing via AI Playground")
        logger.info("AUTH: MCP will act with USER's identity and permissions")
        logger.info(f"AUTH: Token preview: {token[:20]}...{token[-10:]}")
        logger.info("-" * 60)
        return WorkspaceClient(token=token, auth_type="pat")

    # Priority 2: M2M caller sends Bearer token in Authorization header
    auth_header = headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:]  # Strip "Bearer " prefix
        logger.info("-" * 60)
        logger.info("AUTH: [M2M - MACHINE TO MACHINE]")
        logger.info("AUTH: Found 'Authorization: Bearer' header")
        logger.info("AUTH: This is another APP/SERVICE calling this MCP")
        logger.info("AUTH: MCP will act with CALLER APP's Service Principal")
        logger.info(f"AUTH: Token preview: {token[:20]}...{token[-10:]}")
        logger.info("-" * 60)
        return WorkspaceClient(token=token, auth_type="pat")

    # Priority 3: No caller token found — we are in production (local dev returned at line 62)
    logger.info("-" * 60)
    logger.error("AUTH: [DENIED] No caller token found in production!")
    logger.error("AUTH: - No 'x-forwarded-access-token' (not from AI Playground)")
    logger.error("AUTH: - No 'Authorization: Bearer' (not M2M call)")
    logger.error("AUTH: Refusing to fall back to app's Service Principal in production.")
    logger.info("-" * 60)
    raise PermissionError(
        "Authentication required: no caller token found. "
        "Provide x-forwarded-access-token (OBO) or Authorization Bearer header."
    )


# Backwards compatibility alias
get_user_workspace_client = get_caller_workspace_client
