"""Knowledge base query functions using Databricks SQL statement execution.

Uses the app's Service Principal for KB table access since these are
shared resources that all callers should have access to.
"""
from __future__ import annotations

import json
import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from cdm_tools import config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_client() -> WorkspaceClient:
    """Get WorkspaceClient using caller's identity (OBO) for KB access.

    Changed from app's SP to OBO because user may not be able to grant
    USE CATALOG permission to the app's SP.
    """
    logger.info("KB_QUERIES: Getting WorkspaceClient for KB access")

    if os.environ.get("DATABRICKS_APP_NAME"):
        logger.info("KB_QUERIES: Running as Databricks App - using caller's identity (OBO)")
        from server.utils import get_caller_workspace_client
        return get_caller_workspace_client()

    profile = config.DATABRICKS_CONFIG_PROFILE
    logger.info(f"KB_QUERIES: Local dev - using profile: {profile}")
    return WorkspaceClient(profile=profile)


def _execute_sql(sql: str, parameters: list[StatementParameterListItem] | None = None) -> list[dict]:
    """Execute SQL via Databricks SDK statement execution and return rows as dicts."""
    logger.info("=" * 60)
    logger.info("KB_QUERIES: Executing SQL query")
    logger.info(f"KB_QUERIES: SQL: {sql[:100]}..." if len(sql) > 100 else f"KB_QUERIES: SQL: {sql}")
    if parameters:
        logger.info(f"KB_QUERIES: Parameters: {[(p.name, p.value) for p in parameters]}")
    logger.info("=" * 60)

    w = _get_client()

    warehouse_id = os.environ.get("CDM_WAREHOUSE_ID")
    if not warehouse_id:
        logger.info("KB_QUERIES: CDM_WAREHOUSE_ID not set, finding available warehouse...")
        warehouses = list(w.warehouses.list())
        if not warehouses:
            logger.error("KB_QUERIES: No SQL warehouses available!")
            raise RuntimeError("No SQL warehouses available")
        warehouse_id = warehouses[0].id
        logger.info(f"KB_QUERIES: Using warehouse: {warehouse_id}")
    else:
        logger.info(f"KB_QUERIES: Using configured warehouse: {warehouse_id}")

    logger.info("KB_QUERIES: Executing statement...")

    try:
        response = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            parameters=parameters,
            wait_timeout="30s",
        )
    except Exception as e:
        logger.error(f"KB_QUERIES: SQL execution failed: {type(e).__name__}: {e}")
        raise

    if response.status.state != StatementState.SUCCEEDED:
        logger.error(f"KB_QUERIES: SQL failed: {response.status.error}")
        raise RuntimeError(f"SQL failed: {response.status.error}")

    if not response.result or not response.result.data_array:
        logger.info("KB_QUERIES: Query returned 0 rows")
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    rows = [dict(zip(columns, row)) for row in response.result.data_array]
    logger.info(f"KB_QUERIES: Query returned {len(rows)} rows")

    return rows


def get_cdm_spec(cdm_name: str) -> dict:
    """Get CDM field specifications for a given CDM data model."""
    logger.info("-" * 60)
    logger.info(f"KB_QUERIES: get_cdm_spec(cdm_name='{cdm_name}')")
    logger.info(f"KB_QUERIES: Table: {config.CDM_DEFINITIONS_TABLE}")
    logger.info("-" * 60)

    rows = _execute_sql(
        f"SELECT fields_json FROM {config.CDM_DEFINITIONS_TABLE} WHERE cdm_name = :cdm_name",
        parameters=[StatementParameterListItem(name="cdm_name", value=cdm_name)],
    )
    if not rows:
        logger.warning(f"KB_QUERIES: No CDM spec found for '{cdm_name}'")
        return {}

    result = json.loads(rows[0]["fields_json"])
    logger.info(f"KB_QUERIES: Found {len(result)} fields for '{cdm_name}'")
    return result


def get_erp_schema(erp_system: str) -> dict:
    """Get ERP schema info including columns, patterns, and DC indicator specs."""
    logger.info("-" * 60)
    logger.info(f"KB_QUERIES: get_erp_schema(erp_system='{erp_system}')")
    logger.info(f"KB_QUERIES: Table: {config.ERP_SCHEMAS_TABLE}")
    logger.info("-" * 60)

    rows = _execute_sql(
        f"SELECT * FROM {config.ERP_SCHEMAS_TABLE} WHERE erp_system = :erp_system",
        parameters=[StatementParameterListItem(name="erp_system", value=erp_system)],
    )
    if not rows:
        logger.warning(f"KB_QUERIES: No ERP schema found for '{erp_system}'")
        return {}

    row = rows[0]
    logger.info(f"KB_QUERIES: Found ERP schema for '{erp_system}'")
    return {
        "erp_system": row["erp_system"],
        "known_columns": json.loads(row["known_columns_json"]),
        "file_patterns": json.loads(row["file_patterns_json"]),
        "multi_file_specs": json.loads(row["multi_file_specs_json"]),
        "dc_indicator_patterns": json.loads(row["dc_indicator_patterns_json"]),
        "debit_credit_patterns": json.loads(row["debit_credit_patterns_json"]),
    }


def get_all_erp_columns() -> dict[str, list[str]]:
    """Get all ERP systems and their known columns."""
    logger.info("-" * 60)
    logger.info(f"KB_QUERIES: get_all_erp_columns()")
    logger.info(f"KB_QUERIES: Table: {config.ERP_SCHEMAS_TABLE}")
    logger.info("-" * 60)

    rows = _execute_sql(
        f"SELECT erp_system, known_columns_json FROM {config.ERP_SCHEMAS_TABLE}"
    )

    result = {row["erp_system"]: json.loads(row["known_columns_json"]) for row in rows}
    logger.info(f"KB_QUERIES: Found {len(result)} ERP systems")
    return result


def find_similar_mappings(erp_system: str, cdm_name: str) -> list[dict]:
    """Find past mapping configs for similar ERP system and CDM model."""
    logger.info("-" * 60)
    logger.info(f"KB_QUERIES: find_similar_mappings(erp_system='{erp_system}', cdm_name='{cdm_name}')")
    logger.info(f"KB_QUERIES: Table: {config.MAPPING_HISTORY_TABLE}")
    logger.info("-" * 60)

    erp_base = erp_system.split("(")[0].strip().split(" ")[0]
    erp_pattern = f"%{erp_base}%"
    logger.info(f"KB_QUERIES: ERP pattern: {erp_pattern}")

    rows = _execute_sql(
        f"SELECT pipeline_id, erp_system, config_json FROM {config.MAPPING_HISTORY_TABLE} "
        f"WHERE data_model = :cdm_name AND erp_system LIKE :erp_pattern",
        parameters=[
            StatementParameterListItem(name="cdm_name", value=cdm_name),
            StatementParameterListItem(name="erp_pattern", value=erp_pattern),
        ],
    )

    results = []
    for row in rows:
        config_data = json.loads(row["config_json"])
        results.append({
            "pipeline_id": row["pipeline_id"],
            "erp_system": row["erp_system"],
            "config": config_data,
        })

    logger.info(f"KB_QUERIES: Found {len(results)} similar mappings")
    return results
