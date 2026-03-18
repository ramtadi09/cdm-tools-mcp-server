"""File access layer — handles both local paths and Databricks Volume paths.

Uses the caller's identity (OBO or M2M SP) for Volume access so that
file permissions are enforced based on who is making the request.
"""
from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_temp_dir: str | None = None


def _is_volume_path(path: str) -> bool:
    return path.startswith("/Volumes/")


def _on_databricks_app() -> bool:
    return bool(os.environ.get("DATABRICKS_APP_NAME"))


def _get_temp_dir() -> str:
    global _temp_dir
    if _temp_dir is None:
        _temp_dir = tempfile.mkdtemp(prefix="cdm_mcp_")
        logger.info(f"FILE_ACCESS: Created temp directory: {_temp_dir}")
    return _temp_dir


def resolve_file(file_path: str, headers: dict | None = None) -> Path:
    """Resolve a file path, downloading from Volume if needed.

    Args:
        file_path: Local path or /Volumes/ path to resolve.
        headers: Request headers for authentication. Pass explicitly when
                 ContextVar may not be available (e.g., in FastMCP tools).
    """
    logger.info("=" * 60)
    logger.info(f"FILE_ACCESS: resolve_file called")
    logger.info(f"FILE_ACCESS: Path: {file_path}")
    logger.info(f"FILE_ACCESS: Headers provided: {headers is not None}")
    logger.info("=" * 60)

    local = Path(file_path)

    if local.exists():
        logger.info(f"FILE_ACCESS: File exists locally: {local}")
        return local

    if _is_volume_path(file_path):
        logger.info(f"FILE_ACCESS: Detected Unity Catalog Volume path")
        if _on_databricks_app():
            logger.info(f"FILE_ACCESS: Running as Databricks App - will download from Volume")
            return _download_from_volume(file_path, headers=headers)
        else:
            logger.warning(f"FILE_ACCESS: Not running as Databricks App - cannot access Volume")

    logger.error(f"FILE_ACCESS: File not found: {file_path}")
    raise FileNotFoundError(f"File not found: {file_path}")


def _download_from_volume(volume_path: str, headers: dict | None = None) -> Path:
    """Download a file from a UC Volume to a local temp directory.

    Uses caller's identity (OBO) so Volume permissions are enforced.
    Requires the 'files.files' OAuth scope.

    Args:
        volume_path: The /Volumes/... path to download.
        headers: Request headers for authentication. Pass explicitly when
                 ContextVar may not be available (e.g., in FastMCP tools).
    """
    logger.info("-" * 60)
    logger.info(f"FILE_ACCESS: Downloading from Volume")
    logger.info(f"FILE_ACCESS: Volume path: {volume_path}")
    logger.info(f"FILE_ACCESS: Headers provided: {headers is not None}")
    logger.info("-" * 60)

    from server.utils import get_caller_workspace_client

    logger.info("FILE_ACCESS: Getting WorkspaceClient with CALLER's identity...")
    w = get_caller_workspace_client(headers=headers)

    filename = Path(volume_path).name
    local_path = Path(_get_temp_dir()) / filename

    if local_path.exists():
        logger.info(f"FILE_ACCESS: Using cached file: {local_path}")
        return local_path

    logger.info(f"FILE_ACCESS: Calling Files API: w.files.download('{volume_path}')")
    logger.info(f"FILE_ACCESS: This requires 'files.files' OAuth scope")

    try:
        resp = w.files.download(volume_path)
        content = resp.contents.read()
        local_path.write_bytes(content)
        logger.info(f"FILE_ACCESS: SUCCESS - Downloaded {len(content)} bytes")
        logger.info(f"FILE_ACCESS: Saved to: {local_path}")
    except Exception as e:
        logger.error(f"FILE_ACCESS: FAILED - {type(e).__name__}: {e}")
        logger.error(f"FILE_ACCESS: This usually means:")
        logger.error(f"FILE_ACCESS:   1. Missing 'files.files' OAuth scope, or")
        logger.error(f"FILE_ACCESS:   2. Caller doesn't have READ_VOLUME permission")
        raise

    return local_path
