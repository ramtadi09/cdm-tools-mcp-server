"""Environment-driven configuration for CDM tools."""
from __future__ import annotations

import os
from pathlib import Path


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# Project root directory (where the cdm_tools package lives)
_PROJECT_ROOT = Path(__file__).parent.parent


# Unity Catalog paths - defaults to cortex_dev_catalog.0000_ram
CATALOG = _env("CDM_CATALOG", "cortex_dev_catalog")
SCHEMA = _env("CDM_SCHEMA", "0000_ram")
VOLUME_NAME = _env("CDM_VOLUME_NAME", "cdm_mcp_kb")
VOLUME_PATH = _env("CDM_VOLUME_PATH", f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}")
KB_PREFIX = _env("CDM_KB_PREFIX", f"{CATALOG}.{SCHEMA}")

# Delta table names
CDM_DEFINITIONS_TABLE = _env("CDM_DEFINITIONS_TABLE", f"{KB_PREFIX}.cdm_definitions")
ERP_SCHEMAS_TABLE = _env("CDM_ERP_SCHEMAS_TABLE", f"{KB_PREFIX}.erp_schemas")
MAPPING_HISTORY_TABLE = _env("CDM_MAPPING_HISTORY_TABLE", f"{KB_PREFIX}.mapping_history")

# Databricks config
DATABRICKS_CONFIG_PROFILE = _env("DATABRICKS_CONFIG_PROFILE", "dev")

# Workspace notebook path prefix
NOTEBOOK_PATH_PREFIX = _env("CDM_NOTEBOOK_PATH_PREFIX", "")

# Knowledge base local paths - use absolute paths based on project root
KB_LOCAL_DIR = _env("CDM_KB_LOCAL_DIR", str(_PROJECT_ROOT / "knowledge_base"))
NOTEBOOK_TEMPLATE_DIR = _env("CDM_NOTEBOOK_TEMPLATE_DIR", str(_PROJECT_ROOT / "knowledge_base" / "templates"))
