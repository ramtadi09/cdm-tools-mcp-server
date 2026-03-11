"""Databricks Job creation for transform execution.

# TODO: Currently returns instructions only because Databricks Apps user_api_scopes
# does not support 'jobs' scope for OBO. To enable actual job creation:
# 1. Use the app's Service Principal instead of OBO for this specific operation
# 2. Grant the app's SP permission to create jobs in the workspace
# 3. Update this code to use get_workspace_client() instead of get_caller_workspace_client()
# See: https://docs.databricks.com/dev-tools/databricks-apps/auth
"""
from __future__ import annotations

import json
import logging
import os

from cdm_tools import config
from cdm_tools.models import JobSetupResult

logger = logging.getLogger(__name__)


def setup_transform_job(
    notebook_path: str,
    cluster_id: str,
    job_name: str,
    transform_config: dict,
) -> JobSetupResult:
    """Generate instructions for creating a Databricks Job to run a transform notebook.

    Note: Actual job creation is disabled because Databricks Apps user_api_scopes
    does not include 'jobs' scope. This function returns instructions instead.

    TODO: Enable actual job creation using app's Service Principal once
    the SP is granted job creation permissions.
    """
    logger.info("=" * 70)
    logger.info("TOOL CALL: setup_transform_job")
    logger.info(f"  notebook_path: {notebook_path}")
    logger.info(f"  cluster_id: {cluster_id}")
    logger.info(f"  job_name: {job_name}")
    logger.info("=" * 70)

    # Build the job creation payload for manual use
    config_json_escaped = json.dumps(transform_config)

    # Determine cluster configuration
    if cluster_id.lower() == "serverless":
        cluster_config = '"serverless": {"enabled": true}'
        cluster_note = "Uses serverless compute"
    else:
        cluster_config = f'"existing_cluster_id": "{cluster_id}"'
        cluster_note = f"Uses existing cluster: {cluster_id}"

    # Build instructions with the full API payload
    api_payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "transform",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "transform_config_json": config_json_escaped
                    }
                }
            }
        ]
    }

    # Add cluster config based on type
    if cluster_id.lower() == "serverless":
        api_payload["tasks"][0]["environment_key"] = "Default"
    else:
        api_payload["tasks"][0]["existing_cluster_id"] = cluster_id

    instructions = f"""
## Job Creation Instructions

The MCP server cannot create jobs directly (OBO 'jobs' scope not available).
Please create the job manually using one of these methods:

### Option 1: Databricks UI
1. Go to **Workflows** → **Jobs** → **Create Job**
2. Name: `{job_name}`
3. Task type: **Notebook**
4. Notebook path: `{notebook_path}`
5. Cluster: {cluster_note}
6. Add parameter: `transform_config_json` with the config below

### Option 2: Databricks CLI
```bash
databricks jobs create --json '{json.dumps(api_payload, indent=2)}'
```

### Option 3: REST API
```
POST /api/2.1/jobs/create
{json.dumps(api_payload, indent=2)}
```

### Transform Config (for parameter):
```json
{json.dumps(transform_config, indent=2)}
```
"""

    logger.info("JOB_SETUP: Returning instructions (OBO jobs scope not available)")

    return JobSetupResult(
        job_id=None,
        job_url="",
        notebook_path=notebook_path,
        status="instructions",
        message=instructions,
    )
