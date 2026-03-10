"""Environment-driven configuration for CDM tools."""
from __future__ import annotations

import os


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


# Unity Catalog paths
CATALOG = _env("CDM_CATALOG", "fins_genai")
SCHEMA = _env("CDM_SCHEMA", "agents")
VOLUME_PATH = _env("CDM_VOLUME_PATH", f"/Volumes/{CATALOG}/{SCHEMA}/cdm_agent")
KB_PREFIX = _env("CDM_KB_PREFIX", f"{CATALOG}.{SCHEMA}")

# Delta table names
CDM_DEFINITIONS_TABLE = _env("CDM_DEFINITIONS_TABLE", f"{KB_PREFIX}.cdm_definitions")
ERP_SCHEMAS_TABLE = _env("CDM_ERP_SCHEMAS_TABLE", f"{KB_PREFIX}.erp_schemas")
MAPPING_HISTORY_TABLE = _env("CDM_MAPPING_HISTORY_TABLE", f"{KB_PREFIX}.mapping_history")

# Databricks config
DATABRICKS_CONFIG_PROFILE = _env("DATABRICKS_CONFIG_PROFILE", "azure_fieldeng")

# Workspace notebook path prefix
NOTEBOOK_PATH_PREFIX = _env("CDM_NOTEBOOK_PATH_PREFIX", "")

# Knowledge base local paths
KB_LOCAL_DIR = _env("CDM_KB_LOCAL_DIR", "knowledge_base")
NOTEBOOK_TEMPLATE_DIR = _env("CDM_NOTEBOOK_TEMPLATE_DIR", f"{KB_LOCAL_DIR}/templates")
