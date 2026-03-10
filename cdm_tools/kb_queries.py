"""Knowledge base query functions using Databricks SQL statement execution."""
from __future__ import annotations

import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from cdm_tools import config


def _get_client() -> WorkspaceClient:
    if os.environ.get("DATABRICKS_APP_NAME"):
        return WorkspaceClient()
    return WorkspaceClient(profile=config.DATABRICKS_CONFIG_PROFILE)


def _execute_sql(sql: str, parameters: list[StatementParameterListItem] | None = None) -> list[dict]:
    """Execute SQL via Databricks SDK statement execution and return rows as dicts."""
    w = _get_client()
    warehouse_id = os.environ.get("CDM_WAREHOUSE_ID")
    if not warehouse_id:
        warehouses = list(w.warehouses.list())
        if not warehouses:
            raise RuntimeError("No SQL warehouses available")
        warehouse_id = warehouses[0].id

    response = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        parameters=parameters,
        wait_timeout="30s",
    )
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL failed: {response.status.error}")

    if not response.result or not response.result.data_array:
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in response.result.data_array]


def get_cdm_spec(cdm_name: str) -> dict:
    """Get CDM field specifications for a given CDM data model."""
    rows = _execute_sql(
        f"SELECT fields_json FROM {config.CDM_DEFINITIONS_TABLE} WHERE cdm_name = :cdm_name",
        parameters=[StatementParameterListItem(name="cdm_name", value=cdm_name)],
    )
    if not rows:
        return {}
    return json.loads(rows[0]["fields_json"])


def get_erp_schema(erp_system: str) -> dict:
    """Get ERP schema info including columns, patterns, and DC indicator specs."""
    rows = _execute_sql(
        f"SELECT * FROM {config.ERP_SCHEMAS_TABLE} WHERE erp_system = :erp_system",
        parameters=[StatementParameterListItem(name="erp_system", value=erp_system)],
    )
    if not rows:
        return {}
    row = rows[0]
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
    rows = _execute_sql(
        f"SELECT erp_system, known_columns_json FROM {config.ERP_SCHEMAS_TABLE}"
    )
    return {row["erp_system"]: json.loads(row["known_columns_json"]) for row in rows}


def find_similar_mappings(erp_system: str, cdm_name: str) -> list[dict]:
    """Find past mapping configs for similar ERP system and CDM model."""
    erp_base = erp_system.split("(")[0].strip().split(" ")[0]
    erp_pattern = f"%{erp_base}%"
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
    return results
