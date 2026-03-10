"""File access layer — handles both local paths and Databricks Volume paths."""
from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

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
    return _temp_dir


def resolve_file(file_path: str) -> Path:
    """Resolve a file path, downloading from Volume if needed."""
    local = Path(file_path)

    if local.exists():
        return local

    if _is_volume_path(file_path) and _on_databricks_app():
        return _download_from_volume(file_path)

    raise FileNotFoundError(f"File not found: {file_path}")


def _download_from_volume(volume_path: str) -> Path:
    """Download a file from a UC Volume to a local temp directory."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    filename = Path(volume_path).name
    local_path = Path(_get_temp_dir()) / filename

    if local_path.exists():
        logger.info("Using cached file: %s", local_path)
        return local_path

    logger.info("Downloading from Volume: %s -> %s", volume_path, local_path)
    resp = w.files.download(volume_path)
    local_path.write_bytes(resp.contents.read())
    logger.info("Downloaded %d bytes", local_path.stat().st_size)
    return local_path
