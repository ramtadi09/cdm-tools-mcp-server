"""Register all 9 CDM tools with the MCP server.

Each tool wraps the corresponding function from cdm_tools,
exposing it over MCP with proper type hints and docstrings.
"""

import json
import logging
from pathlib import Path

import pandas as pd
from fastmcp import FastMCP

from cdm_tools.file_access import resolve_file
from cdm_tools.ingestion.format_detector import detect_format
from cdm_tools.ingestion.loader import load_file
from cdm_tools.ingestion.profiler import profile_columns
from cdm_tools.classification.classifier import classify_files
from cdm_tools.models import (
    ColumnProfileModel,
    FileInfo,
    JoinSpecModel,
    SchemaReport,
    TransformConfig,
    TransformPreview,
)
from server.utils import header_store

# Try to use FastMCP's built-in header injection; fall back to ContextVar
try:
    from fastmcp.server.dependencies import get_http_headers as _fmcp_get_headers
except ImportError:
    _fmcp_get_headers = None


def _get_request_headers() -> dict:
    """Get the current request's HTTP headers for OBO auth forwarding."""
    if _fmcp_get_headers is not None:
        try:
            return _fmcp_get_headers() or {}
        except Exception as e:
            logging.getLogger(__name__).debug(f"get_http_headers() failed, falling back to header_store: {e}")
    return header_store.get({})


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def register_tools(mcp: FastMCP) -> None:
    """Register all 9 CDM tools on the given FastMCP server."""

    logger.info("TOOLS: Registering 9 CDM tools...")

    # ── Schema Analyzer Tools ──

    @mcp.tool()
    def analyze_files(file_paths: list[str]) -> str:
        """Analyze uploaded data files: detect format, load, profile columns, classify as fact/dimension.

        Args:
            file_paths: List of file paths to analyze (local or /Volumes/ paths).

        Returns:
            JSON string of SchemaReport with file info, column profiles, and classification.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: analyze_files")
        logger.info(f"TOOL CALL: file_paths = {file_paths}")
        logger.info("=" * 70)

        headers = _get_request_headers()

        files_info = []
        all_profiles = {}
        loaded_dfs = {}

        for fp in file_paths:
            logger.info(f"TOOL: Processing file: {fp}")
            try:
                path = resolve_file(fp, headers=headers)
            except FileNotFoundError:
                logger.warning("TOOL: File not found, skipping: %s", fp)
                continue

            logger.info(f"TOOL: Detecting format for {path.name}...")
            fmt = detect_format(path)
            logger.info(f"TOOL: Format: {fmt.file_type}, delimiter: {fmt.delimiter}")

            logger.info(f"TOOL: Loading file...")
            df = load_file(path, fmt)
            logger.info(f"TOOL: Loaded {len(df)} rows, {len(df.columns)} columns")

            logger.info(f"TOOL: Profiling columns...")
            profiles = profile_columns(df)

            files_info.append(FileInfo(
                file_path=fp, file_type=fmt.file_type, delimiter=fmt.delimiter,
                encoding=fmt.encoding, header_row=fmt.header_row,
                report_format=fmt.report_format,
                row_count=len(df), column_count=len(df.columns),
            ))

            profile_models = [
                ColumnProfileModel(
                    name=p.name, inferred_type=p.inferred_type,
                    total_count=p.total_count, null_count=p.null_count,
                    unique_count=p.unique_count, sample_values=p.sample_values,
                )
                for p in profiles.values()
            ]
            all_profiles[path.name] = profile_models
            loaded_dfs[path.name] = df

        logger.info(f"TOOL: Classifying {len(loaded_dfs)} files as fact/dimension...")
        classification = classify_files(loaded_dfs) if loaded_dfs else None

        report = SchemaReport(
            files=files_info,
            profiles=all_profiles,
            fact_table=classification.fact_table if classification else None,
            dimension_tables=classification.dimension_tables if classification else [],
            joins=[
                JoinSpecModel(
                    fact_table=j.fact_table, dimension_table=j.dimension_table,
                    join_columns=j.join_columns, join_type=j.join_type,
                )
                for j in (classification.joins if classification else [])
            ],
        )
        logger.info(f"TOOL: analyze_files completed successfully")
        return report.model_dump_json()

    @mcp.tool()
    def lookup_erp_columns() -> str:
        """Look up known column names for all ERP systems from the knowledge base.

        Returns:
            JSON dict mapping ERP system name to list of known columns.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: lookup_erp_columns")
        logger.info("=" * 70)

        headers = _get_request_headers()

        from cdm_tools.kb_queries import get_all_erp_columns
        result = get_all_erp_columns(headers=headers)
        logger.info(f"TOOL: lookup_erp_columns completed, found {len(result)} ERP systems")
        return json.dumps(result)

    # ── Mapping Tools ──

    @mcp.tool()
    def lookup_cdm_fields(cdm_name: str) -> str:
        """Look up CDM field specifications for a data model.

        Args:
            cdm_name: CDM data model name (e.g., "general_ledger_detail").

        Returns:
            JSON dict of field name -> {type, description} for the CDM model.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: lookup_cdm_fields")
        logger.info(f"TOOL CALL: cdm_name = '{cdm_name}'")
        logger.info("=" * 70)

        headers = _get_request_headers()

        from cdm_tools.kb_queries import get_cdm_spec
        result = get_cdm_spec(cdm_name, headers=headers)
        logger.info(f"TOOL: lookup_cdm_fields completed, found {len(result)} fields")
        return json.dumps(result)

    @mcp.tool()
    def find_past_mappings(erp_system: str, cdm_name: str) -> str:
        """Find past transformation configs for similar ERP system and CDM model.

        Args:
            erp_system: ERP system name (e.g., "Oracle", "SAP").
            cdm_name: CDM data model name.

        Returns:
            JSON list of past mapping configs.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: find_past_mappings")
        logger.info(f"TOOL CALL: erp_system = '{erp_system}', cdm_name = '{cdm_name}'")
        logger.info("=" * 70)

        headers = _get_request_headers()

        from cdm_tools.kb_queries import find_similar_mappings
        result = find_similar_mappings(erp_system, cdm_name, headers=headers)
        logger.info(f"TOOL: find_past_mappings completed, found {len(result)} mappings")
        return json.dumps(result)

    # ── Transform Tools ──

    @mcp.tool()
    def preview_transform(config_json: str, file_paths: list[str]) -> str:
        """Preview transformation results using pandas (no Spark needed).

        Args:
            config_json: JSON string of TransformConfig.
            file_paths: List of file paths to transform.

        Returns:
            JSON string of TransformPreview with sample rows and warnings.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: preview_transform")
        logger.info(f"TOOL CALL: file_paths = {file_paths}")
        logger.info("=" * 70)

        headers = _get_request_headers()

        from cdm_tools.transform_preview import apply_preview

        config_data = json.loads(config_json)
        config = (
            TransformConfig.from_raw_config(config_data)
            if "transformation_variables" in config_data
            else TransformConfig.model_validate(config_data)
        )

        dfs = {}
        for fp in file_paths:
            try:
                path = resolve_file(fp, headers=headers)
            except FileNotFoundError:
                logger.warning("TOOL: File not found for preview, skipping: %s", fp)
                continue
            fmt = detect_format(path)
            dfs[path.name] = load_file(path, fmt)

        preview_df, warnings = apply_preview(dfs, config)

        preview = TransformPreview(
            row_count=len(preview_df),
            column_count=len(preview_df.columns),
            columns=list(preview_df.columns),
            sample_rows=preview_df.head(10).to_dict(orient="records"),
            warnings=warnings,
        )
        logger.info(f"TOOL: preview_transform completed, {len(preview_df)} rows")
        return preview.model_dump_json()

    @mcp.tool()
    def lookup_pipeline_notebook(cdm_name: str, erp_system: str) -> str:
        """Search pipeline_notebooks.json for a matching transform notebook.

        Args:
            cdm_name: CDM data model name.
            erp_system: ERP system name.

        Returns:
            JSON with matching notebook info or empty list.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: lookup_pipeline_notebook")
        logger.info(f"TOOL CALL: cdm_name = '{cdm_name}', erp_system = '{erp_system}'")
        logger.info("=" * 70)

        from cdm_tools import config as cfg

        registry_path = Path(cfg.KB_LOCAL_DIR) / "pipeline_notebooks.json"
        if not registry_path.exists():
            logger.warning(f"TOOL: pipeline_notebooks.json not found at {registry_path}")
            return json.dumps([])

        notebooks = json.loads(registry_path.read_text())
        prefix = cfg.NOTEBOOK_PATH_PREFIX.rstrip("/")
        matches = []
        for nb in notebooks:
            if nb["cdm_name"] == cdm_name and erp_system.lower() in nb["erp_system"].lower():
                if prefix and not nb["notebook_path"].startswith("/"):
                    nb = {**nb, "notebook_path": f"{prefix}/{nb['notebook_path']}"}
                matches.append(nb)

        logger.info(f"TOOL: lookup_pipeline_notebook completed, found {len(matches)} matches")
        return json.dumps(matches)

    @mcp.tool()
    def setup_databricks_job(
        notebook_path: str, cluster_id: str, job_name: str, config_json: str,
    ) -> str:
        """Create a Databricks Job to run a transform notebook (does NOT execute it).

        Args:
            notebook_path: Workspace path to the transform notebook.
            cluster_id: Cluster ID to run on.
            job_name: Human-readable job name.
            config_json: JSON string of the transform config to pass as parameters.

        Returns:
            JSON string of JobSetupResult with job_id and URL.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: setup_databricks_job")
        logger.info(f"TOOL CALL: notebook_path = '{notebook_path}'")
        logger.info(f"TOOL CALL: cluster_id = '{cluster_id}'")
        logger.info(f"TOOL CALL: job_name = '{job_name}'")
        logger.info("=" * 70)

        from cdm_tools.job_setup import setup_transform_job

        config_data = json.loads(config_json)
        result = setup_transform_job(notebook_path, cluster_id, job_name, config_data)
        logger.info(f"TOOL: setup_databricks_job completed, status: {result.status}")
        return result.model_dump_json()

    @mcp.tool()
    def generate_transform_notebook(
        config_json: str,
        erp_system: str,
        notebook_title: str,
        user_description: str = "",
    ) -> str:
        """Generate a transform notebook from template when no existing notebook is found.

        Args:
            config_json: JSON string of TransformConfig.
            erp_system: ERP system name.
            notebook_title: Descriptive title for the notebook.
            user_description: Optional user-provided description with domain context.

        Returns:
            JSON string of NotebookGenerationResult with full notebook code.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: generate_transform_notebook")
        logger.info(f"TOOL CALL: erp_system = '{erp_system}'")
        logger.info(f"TOOL CALL: notebook_title = '{notebook_title}'")
        logger.info("=" * 70)

        headers = _get_request_headers()

        from cdm_tools.notebook_generator import generate_notebook

        config_data = json.loads(config_json)
        tc = (
            TransformConfig.from_raw_config(config_data)
            if "transformation_variables" in config_data
            else TransformConfig.model_validate(config_data)
        )
        result = generate_notebook(tc, erp_system, notebook_title, user_description, headers=headers)
        logger.info(f"TOOL: generate_transform_notebook completed")
        return result.model_dump_json()

    # ── Validation Tools ──

    @mcp.tool()
    def validate_data(
        preview_rows_json: str,
        cdm_name: str,
        date_columns: list[str] | None = None,
        debit_col: str = "",
        credit_col: str = "",
    ) -> str:
        """Run 5 rule-based validation checks on preview data.

        Args:
            preview_rows_json: JSON string of list of row dicts (from preview).
            cdm_name: CDM model name to look up required fields.
            date_columns: Date column names to validate.
            debit_col: Debit column name for balance check.
            credit_col: Credit column name for balance check.

        Returns:
            JSON string of ValidationReport.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: validate_data")
        logger.info(f"TOOL CALL: cdm_name = '{cdm_name}'")
        logger.info(f"TOOL CALL: date_columns = {date_columns}")
        logger.info("=" * 70)

        headers = _get_request_headers()

        from cdm_tools.validation_checks import run_all_checks

        rows = json.loads(preview_rows_json)
        df = pd.DataFrame(rows)
        logger.info(f"TOOL: Validating {len(df)} rows")

        cdm_fields = {}
        required_fields = []
        try:
            from cdm_tools.kb_queries import get_cdm_spec
            cdm_fields = get_cdm_spec(cdm_name, headers=headers)
            required_fields = list(cdm_fields.keys())
        except Exception as e:
            logger.warning(f"TOOL: Could not load CDM spec: {e}")

        report = run_all_checks(
            df=df,
            required_fields=required_fields,
            field_specs=cdm_fields,
            date_columns=date_columns or [],
            debit_col=debit_col,
            credit_col=credit_col,
        )
        logger.info(f"TOOL: validate_data completed")
        return report.model_dump_json()

    logger.info("TOOLS: All 9 CDM tools registered successfully")
