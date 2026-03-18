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
    _logger = logging.getLogger(__name__)

    # Try FastMCP dependency injection first
    if _fmcp_get_headers is not None:
        try:
            hdrs = _fmcp_get_headers() or {}
            if hdrs:
                _logger.info(f"HEADERS: Got {len(hdrs)} headers from FastMCP get_http_headers()")
                _log_auth_headers(hdrs, _logger, "FastMCP")
                return hdrs
            _logger.info("HEADERS: FastMCP get_http_headers() returned empty, trying header_store")
        except Exception as e:
            _logger.warning(f"HEADERS: FastMCP get_http_headers() failed: {e}, trying header_store")

    # Fall back to ContextVar set by middleware
    hdrs = header_store.get({})
    if hdrs:
        _logger.info(f"HEADERS: Got {len(hdrs)} headers from header_store ContextVar")
        _log_auth_headers(hdrs, _logger, "header_store")
    else:
        _logger.warning("HEADERS: header_store ContextVar is EMPTY — no auth headers available!")
    return hdrs


def _log_auth_headers(headers: dict, _logger, source: str):
    """Log which auth headers are present (values masked)."""
    obo = headers.get("x-forwarded-access-token")
    auth = headers.get("authorization", "")
    if obo:
        _logger.info(f"HEADERS[{source}]: x-forwarded-access-token PRESENT (len={len(obo)})")
    else:
        _logger.warning(f"HEADERS[{source}]: x-forwarded-access-token MISSING")
    if auth:
        _logger.info(f"HEADERS[{source}]: authorization PRESENT ({auth[:15]}...)")
    else:
        _logger.info(f"HEADERS[{source}]: authorization not present")


logger = logging.getLogger(__name__)


def register_tools(mcp: FastMCP) -> None:
    """Register all 9 CDM tools on the given FastMCP server."""

    logger.info("TOOLS: Registering 9 CDM tools...")

    # ── Schema Analyzer Tools ──

    @mcp.tool()
    def analyze_files(file_paths: list[str]) -> str:
        """[PHASE 1 - STEP 1] Start the CDM mapping workflow here. Analyzes ERP data files:
        detects format (CSV/Excel/fixed-width), profiles all columns (types, nulls, samples),
        and classifies files as fact or dimension tables.

        Use this FIRST before any other CDM tool. The erp_system, column names, and file_paths
        from this output are required by: lookup_erp_columns (Phase 1), find_past_mappings
        (Phase 2), preview_transform (Phase 3), and lookup_pipeline_notebook (Phase 3).

        Args:
            file_paths: List of file paths to analyze (local or /Volumes/ paths).

        Returns:
            JSON string of SchemaReport with file info, column profiles, and fact/dimension classification.
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

        if not loaded_dfs:
            error_msg = f"No files could be loaded from the provided paths: {file_paths}. Check that the paths exist and are accessible."
            logger.error(f"TOOL: analyze_files failed — {error_msg}")
            return json.dumps({"error": error_msg})

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
        """[PHASE 1 - STEP 2] Get known column patterns for all ERP systems from the knowledge base.
        Call this immediately after analyze_files to compare the file's actual columns against
        known ERP patterns — this identifies the source ERP system (SAP, Oracle, MS Dynamics, etc.).

        No arguments needed. Call after analyze_files, before lookup_cdm_fields.

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
        """[PHASE 2 - STEP 1] Get the target CDM field specifications for a data model.
        Call this at the start of Phase 2 (after user confirms Phase 1 findings) to know
        what fields the CDM model requires before proposing column mappings.

        Call alongside find_past_mappings. Output used to build the mapping table and
        later to construct TransformConfig in Phase 3.

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
        """[PHASE 2 - STEP 2] Find past transformation configs for this ERP + CDM combination.
        Call alongside lookup_cdm_fields in Phase 2. Past mappings serve as a starting point
        for the column mapping proposal — use them to increase confidence scores and reuse
        proven column mappings.

        Requires erp_system identified in Phase 1 (from analyze_files + lookup_erp_columns).

        Args:
            erp_system: ERP system name identified in Phase 1 (e.g., "Oracle", "SAP").
            cdm_name: CDM data model name (e.g., "general_ledger_detail").

        Returns:
            JSON list of past mapping configs with source->CDM column mappings.
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
        """[PHASE 3 - STEP 1] Test the transformation config against actual files using pandas.
        Call this after user confirms the Phase 2 column mappings. Build config_json from
        the confirmed mappings (required_columns, date_columns, amount_columns, etc.).

        The sample_rows in the output are required by validate_data in Phase 4.
        Call lookup_pipeline_notebook immediately after this.

        Args:
            config_json: JSON string of TransformConfig built from Phase 2 confirmed mappings.
            file_paths: Same file paths used in analyze_files (Phase 1).

        Returns:
            JSON string of TransformPreview with sample_rows (feed to validate_data) and warnings.
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

        if not dfs:
            error_msg = f"No files could be loaded from the provided paths: {file_paths}. Check that the paths exist and are accessible."
            logger.error(f"TOOL: preview_transform failed — {error_msg}")
            return json.dumps({"error": error_msg})

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
        """[PHASE 3 - STEP 2] Search for an existing transform notebook for this ERP + CDM pair.
        Call immediately after preview_transform in Phase 3.

        If a notebook IS found: present the notebook_path to the user.
        If NO notebook is found (empty list): ask the user for additional context,
        then call generate_transform_notebook instead.

        Args:
            cdm_name: CDM data model name (same as used in Phase 2).
            erp_system: ERP system name identified in Phase 1.

        Returns:
            JSON with matching notebook info (notebook_path, etc.) or empty list if not found.
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
        """[OPTIONAL - Final Step] Create a Databricks Job to schedule the transform notebook.
        Only call this if the user explicitly wants to schedule or run the transformation.
        Does NOT execute the job — only creates it. Ask the user for cluster_id before calling.

        Requires notebook_path from lookup_pipeline_notebook or generate_transform_notebook (Phase 3).

        Args:
            notebook_path: Workspace path from lookup_pipeline_notebook or generate_transform_notebook.
            cluster_id: Ask the user to provide their cluster ID.
            job_name: Suggest a descriptive name e.g. "CDM_{erp_system}_{cdm_name}_transform".
            config_json: The TransformConfig JSON string from Phase 3.

        Returns:
            JSON string of JobSetupResult with job_id and job URL.
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
        """[PHASE 3 - STEP 3, conditional] Generate a new transform notebook from template.
        Only call this when lookup_pipeline_notebook returns an empty list (no existing notebook).
        Ask the user for any additional domain context before calling.

        The notebook_path in the output is required by setup_databricks_job (optional final step).

        Args:
            config_json: JSON string of TransformConfig from Phase 3.
            erp_system: ERP system name identified in Phase 1.
            notebook_title: Descriptive title, e.g. "SAP_GL_to_CDM_GeneralLedger_transform".
            user_description: Optional domain context from the user (business rules, special handling).

        Returns:
            JSON string of NotebookGenerationResult with notebook_path and full notebook code.
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
        """[PHASE 4 - FINAL STEP] Run 5 quality checks on the transformed data.
        Call this after user confirms Phase 3 preview results. Use the sample_rows
        from preview_transform output as preview_rows_json.

        Runs: completeness, type consistency, null ratios, date range, debit/credit balance.
        This is the last required phase — summarize results and flag any issues.

        Args:
            preview_rows_json: JSON string of sample_rows from preview_transform output.
            cdm_name: CDM model name (same as used throughout the workflow).
            date_columns: Date column names identified during the workflow.
            debit_col: Debit column name if present (for balance check).
            credit_col: Credit column name if present (for balance check).

        Returns:
            JSON string of ValidationReport with pass/fail for all 5 checks.
        """
        logger.info("=" * 70)
        logger.info("TOOL CALL: validate_data")
        logger.info(f"TOOL CALL: cdm_name = '{cdm_name}'")
        logger.info(f"TOOL CALL: date_columns = {date_columns}")
        logger.info("=" * 70)

        headers = _get_request_headers()

        from cdm_tools.validation_checks import run_all_checks

        try:
            rows = json.loads(preview_rows_json)
        except json.JSONDecodeError as e:
            error_msg = f"preview_rows_json is not valid JSON: {e}. Pass the sample_rows field directly from preview_transform output."
            logger.error(f"TOOL: validate_data failed — {error_msg}")
            return json.dumps({"error": error_msg})

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
