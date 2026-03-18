"""Template-based notebook generation for transform pipelines."""
from __future__ import annotations

import json
from pathlib import Path

from cdm_tools import config
from cdm_tools.models import NotebookGenerationResult, TransformConfig


def load_template(cdm_name: str) -> str:
    """Read the template notebook for a given CDM data model."""
    # Use .template extension to prevent Databricks from stripping it during deployment
    template_path = Path(config.NOTEBOOK_TEMPLATE_DIR) / f"transform_{cdm_name}.template"
    if not template_path.exists():
        raise FileNotFoundError(f"Template not found: {template_path}")
    return template_path.read_text()


def serialize_transform_config(tc: TransformConfig) -> str:
    """Convert TransformConfig back to CortexPy raw dict format as Python code."""
    dc = tc.debit_credit
    dci = tc.dc_indicator

    raw = {
        "erp_system": tc.erp_system,
        "data_model": tc.data_model,
        "read_in_variables": tc.read_in_variables,
        "transformation_variables": {
            "required_columns": tc.required_columns,
            "extra_table_columns": tc.extra_table_columns,
            "join_columns": tc.join_columns,
            "date_columns": tc.date_columns,
            "amount_columns": tc.amount_columns,
            "effective_date": tc.effective_date,
            "posted_date": tc.posted_date,
            "debit_credit": {
                "amount_oc": {"debit_column": dc.amount_oc.debit_column, "credit_column": dc.amount_oc.credit_column, "operator": dc.amount_oc.operator},
                "amount_ec": {"debit_column": dc.amount_ec.debit_column, "credit_column": dc.amount_ec.credit_column, "operator": dc.amount_ec.operator},
                "amount_gc": {"debit_column": dc.amount_gc.debit_column, "credit_column": dc.amount_gc.credit_column, "operator": dc.amount_gc.operator},
            },
            "dc_indicator": {
                "column": dci.column,
                "credit_value": dci.credit_value,
                "valid_values": dci.valid_values,
                "columns_to_apply_to": dci.columns_to_apply_to,
                "transform_dc_indicators": dci.transform_dc_indicators,
            },
        },
    }
    formatted = _pretty_repr(raw, indent=4)
    return f"transformation_config = {formatted}"


def _pretty_repr(obj: object, indent: int = 4, _level: int = 0) -> str:
    pad = " " * indent * _level
    inner_pad = " " * indent * (_level + 1)

    if isinstance(obj, dict):
        if not obj:
            return "{}"
        items = []
        for k, v in obj.items():
            items.append(f"{inner_pad}{repr(k)}: {_pretty_repr(v, indent, _level + 1)}")
        return "{\n" + ",\n".join(items) + f"\n{pad}}}"
    elif isinstance(obj, list):
        if not obj:
            return "[]"
        if all(isinstance(x, str) for x in obj) and len(obj) <= 6:
            return repr(obj)
        items = [f"{inner_pad}{_pretty_repr(x, indent, _level + 1)}" for x in obj]
        return "[\n" + ",\n".join(items) + f"\n{pad}]"
    else:
        return repr(obj)


def generate_config_variable_assignment(tc: TransformConfig) -> str:
    """Generate the variable extraction code block from the config dict."""
    lines = [
        "# Extract configuration variables for processing",
        "",
        "#Read-In Variables",
        'read_in_vars = transformation_config["read_in_variables"]',
        "",
        'header = read_in_vars["header"]',
        'report_format = read_in_vars["report_format"]',
        "",
        "#Transformation Variables",
        'transformation_vars = transformation_config["transformation_variables"]',
        "",
        'required_columns = transformation_vars["required_columns"]',
        'extra_table_columns = transformation_vars["extra_table_columns"]',
        'join_columns = transformation_vars["join_columns"]',
        'date_columns = transformation_vars["date_columns"]',
        'amount_columns = transformation_vars["amount_columns"]',
        "transform_dc_indicators = transformation_vars['dc_indicator'][\"transform_dc_indicators\"]",
        "",
        "",
        "logger.info(f\"Template loaded for: {transformation_config['erp_system']}\")",
        "logger.info(f\"Required GL columns: {len(required_columns)} fields\")",
        "logger.info(f\"Date columns: {date_columns}\")",
        "logger.info(f\"Amount columns: {amount_columns}\")",
    ]
    return "\n".join(lines)


def generate_cdm_mapping_section(tc: TransformConfig) -> str:
    """Generate the CDM mapping call, extending amount_columns if debit_credit is populated."""
    lines = ["## CDM Mapping and Final Response Generation"]

    dc = tc.debit_credit
    has_debit_credit = any([
        dc.amount_oc.debit_column, dc.amount_oc.credit_column,
        dc.amount_ec.debit_column, dc.amount_ec.credit_column,
        dc.amount_gc.debit_column, dc.amount_gc.credit_column,
    ])

    if has_debit_credit:
        lines.append("# Debit/credit columns configured - extend amount_columns with generated columns")
        lines.append("cdm_amount_columns = list(amount_columns)")
        for suffix in ["amount_oc", "amount_ec", "amount_gc"]:
            amt = getattr(dc, suffix)
            if amt.debit_column or amt.credit_column:
                lines.append(f'if "{suffix}" not in cdm_amount_columns:')
                lines.append(f'    cdm_amount_columns.append("{suffix}")')
        lines.append("response = run_cdm_mapping_final_output_generation(df, params, date_columns, cdm_amount_columns)")
    else:
        lines.append("response = run_cdm_mapping_final_output_generation(df, params, date_columns, amount_columns)")

    return "\n".join(lines)


def build_custom_section_prompt(
    tc: TransformConfig,
    erp_schema: dict | None,
    user_description: str,
) -> str:
    """Build a rich context block for the custom transform section (Sections 6/7)."""
    lines = ["# --- Custom Transform Section ---"]
    lines.append(f"# ERP: {tc.erp_system} | Data Model: {tc.data_model}")

    has_joins = bool(tc.join_columns)
    if has_joins:
        join_desc = ", ".join(
            f"{name} on {cols}" for name, cols in tc.join_columns.items()
        )
        lines.append(f"# Has joins: Yes ({join_desc})")
    else:
        lines.append("# Has joins: No")

    dci = tc.dc_indicator
    if dci.column:
        lines.append(
            f"# DC Indicator: column={dci.column}, credit_value={dci.credit_value}, "
            f"transform={dci.transform_dc_indicators}"
        )

    if user_description:
        lines.append(f"# User notes: \"{user_description}\"")

    if erp_schema:
        dci_patterns = erp_schema.get("dc_indicator_patterns", {})
        dc_patterns = erp_schema.get("debit_credit_patterns", {})
        multi_file = erp_schema.get("multi_file_specs", {})
        if dci_patterns or dc_patterns or multi_file:
            lines.append("#")
            lines.append(f"# ERP schema context for {tc.erp_system}:")
            if dci_patterns:
                lines.append(f"#   DC indicator patterns: {json.dumps(dci_patterns)}")
            if dc_patterns:
                lines.append(f"#   Debit/credit patterns: {json.dumps(dc_patterns)}")
            if multi_file:
                lines.append(f"#   Multi-file specs: {json.dumps(multi_file)}")

    lines.append("#")
    lines.append("# TODO: Review and adjust the code below")
    lines.append("")

    if has_joins:
        lines.append("try:")
        lines.append("    # Perform joins with extra tables")
        lines.append("    original_count = gl_df.count()")
        lines.append('    logger.info(f"GL data prepared: {original_count} records")')
        lines.append("")

        for table_name in tc.extra_table_columns:
            join_key = None
            for jk in tc.join_columns:
                if table_name in jk:
                    join_key = jk
                    break

            if join_key:
                lines.append(f"    if extra_table_dataframes.get('{table_name}'):")
                lines.append(f"        {table_name}_df = extra_table_dataframes['{table_name}']")
                lines.append(f"        join_cols = join_columns['{join_key}']")
                lines.append(f'        logger.info(f"Joining with {table_name} on {{join_cols}}")')
                lines.append(f"        gl_df = gl_df.join({table_name}_df, on=join_cols, how='left')")
                lines.append(f"        joined_count = gl_df.count()")
                lines.append(f"        if joined_count != original_count:")
                lines.append(f'            raise TransformationError(f"Join integrity check failed. Original: {{original_count}}, Joined: {{joined_count}}")')
                lines.append(f'        logger.info(f"Join with {table_name} completed - {{joined_count}} records")')
                lines.append("")

        lines.append('    logger.info("Data preparation completed - Ready for transformation")')
        lines.append("")
        lines.append("except Exception as e:")
        lines.append('    logger.error(f"Data join failed: {e}")')
        lines.append("    error_response = {")
        lines.append('        "Status": "FAILED",')
        lines.append('        "ErrorMessageKey": f"Data join failed: {str(e)}",')
        lines.append('        "ErrorType": type(e).__name__,')
        lines.append('        "Traceback": traceback.format_exc(),')
        lines.append('        "InputId": params.input_id')
        lines.append("    }")
        lines.append("    dbutils.notebook.exit(json.dumps(error_response))")
    else:
        lines.append("# No join logic needed for this ERP configuration")
        lines.append("pass")

    lines.append("")
    lines.append("# COMMAND ----------")
    lines.append("")
    lines.append("# DBTITLE 1,🟡 Time Posted Creation")

    posted_date_col = tc.posted_date or "posted_date"
    lines.append(f"# Creating time_posted column from {posted_date_col}")
    lines.append("try:")
    lines.append(f"    gl_df = gl_df.withColumn('time_posted', col('`{posted_date_col}`'))")
    lines.append("    if 'time_posted' not in gl_df.columns:")
    lines.append('        raise ColumnValidationError("Failed to create time_posted column")')
    lines.append(f'    logger.info(f"Successfully created time_posted column from {posted_date_col}")')
    lines.append("")
    lines.append("except Exception as e:")
    lines.append('    logger.error(f"Unable to create time_posted column: {e}")')
    lines.append("    error_response = {")
    lines.append('        "Status": "FAILED",')
    lines.append('        "ErrorMessageKey": f"Custom transformation failed: {str(e)}",')
    lines.append('        "ErrorType": type(e).__name__,')
    lines.append('        "Traceback": traceback.format_exc(),')
    lines.append('        "InputId": params.input_id')
    lines.append("    }")
    lines.append("    dbutils.notebook.exit(json.dumps(error_response))")

    return "\n".join(lines)


def _get_erp_schema_safe(erp_system: str, headers: dict | None = None) -> dict | None:
    """Try to fetch ERP schema from KB, return None on failure."""
    try:
        from cdm_tools.kb_queries import get_erp_schema
        return get_erp_schema(erp_system, headers=headers)
    except Exception:
        return None


def generate_notebook(
    tc: TransformConfig,
    erp_system: str,
    notebook_title: str,
    user_description: str = "",
    headers: dict | None = None,
) -> NotebookGenerationResult:
    """Generate a complete notebook from template + TransformConfig."""
    tc_with_erp = tc.model_copy(update={"erp_system": erp_system}) if not tc.erp_system else tc
    warnings: list[str] = []

    template = load_template(tc_with_erp.data_model)

    # Fill all 5 template placeholders
    config_code = serialize_transform_config(tc_with_erp)
    variable_assignment = generate_config_variable_assignment(tc_with_erp)
    cdm_mapping = generate_cdm_mapping_section(tc_with_erp)

    erp_schema = _get_erp_schema_safe(erp_system, headers=headers)
    custom_section = build_custom_section_prompt(tc_with_erp, erp_schema, user_description)

    notebook_code = template
    notebook_code = notebook_code.replace("{{NOTEBOOK_TITLE}}", notebook_title)
    notebook_code = notebook_code.replace("{{TRANSFORMATION_CONFIG}}", config_code)
    notebook_code = notebook_code.replace("{{CONFIG_VARIABLE_ASSIGNMENT}}", variable_assignment)
    notebook_code = notebook_code.replace("{{CUSTOM_TRANSFORM_SECTION}}", custom_section)
    notebook_code = notebook_code.replace("{{CDM_MAPPING_SECTION}}", cdm_mapping)

    if tc_with_erp.read_in_variables.get("report_format"):
        warnings.append("report_format=True: generated notebook may need manual adjustments")

    has_joins = bool(tc_with_erp.join_columns)
    has_custom = has_joins or bool(tc_with_erp.dc_indicator.column)
    suggested_filename = f"Transform_{erp_system}_{notebook_title.replace(' ', '_')}.py"

    return NotebookGenerationResult(
        notebook_code=notebook_code,
        notebook_title=notebook_title,
        erp_system=erp_system,
        cdm_name=tc_with_erp.data_model,
        has_joins=has_joins,
        has_custom_transforms=has_custom,
        warnings=warnings,
        suggested_filename=suggested_filename,
    )
