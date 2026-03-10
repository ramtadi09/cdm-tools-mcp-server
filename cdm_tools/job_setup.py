"""Databricks Job creation for transform execution."""
from __future__ import annotations

import json
import os

from cdm_tools import config
from cdm_tools.models import JobSetupResult


def setup_transform_job(
    notebook_path: str,
    cluster_id: str,
    job_name: str,
    transform_config: dict,
) -> JobSetupResult:
    """Create a Databricks Job to run a transform notebook. Does NOT run the job."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.jobs import Task, NotebookTask

    if not cluster_id or cluster_id.lower() in ("auto", "none", ""):
        return JobSetupResult(
            job_id=None, job_url="", notebook_path=notebook_path,
            status="skipped",
            message="No cluster_id provided. Job setup skipped.",
        )

    if os.environ.get("DATABRICKS_APP_NAME"):
        w = WorkspaceClient()
    else:
        w = WorkspaceClient(profile=config.DATABRICKS_CONFIG_PROFILE)

    notebook_task = NotebookTask(
        notebook_path=notebook_path,
        base_parameters={"transform_config_json": json.dumps(transform_config)},
    )

    task = Task(
        task_key="transform",
        notebook_task=notebook_task,
        existing_cluster_id=cluster_id,
    )

    job = w.jobs.create(name=job_name, tasks=[task])

    host = w.config.host.rstrip("/")
    job_url = f"{host}/jobs/{job.job_id}"

    return JobSetupResult(
        job_id=job.job_id, job_url=job_url, notebook_path=notebook_path,
        status="created",
        message=f"Job '{job_name}' created. Run it manually or via API.",
    )
