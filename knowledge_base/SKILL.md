---
name: cdm-mapping
description: "CDM audit data mapping and transformation workflow. Use for ERP to CDM transformation, schema analysis, column mapping, transform preview, data validation. Triggers for: SAP, Oracle, Microsoft Dynamics, ERP audit data, general ledger, CDM mapping, ERP schema, data model transformation."
---

## CDM Mapping Workflow — 4 Phases

You have 9 MCP tools from the CDM Tools MCP server. Follow this workflow strictly,
completing ONE phase at a time and waiting for user confirmation before proceeding.

---

### Phase 1: Schema Analysis
**Goal:** Understand the uploaded files and identify the source ERP system.

1. Call `analyze_files` with the file paths provided by the user
2. Call `lookup_erp_columns` (no arguments needed) to get known ERP column patterns
3. Compare the file columns against ERP patterns to identify the ERP system
   - If the ERP system cannot be confidently identified from column patterns alone,
     ask the user directly: "Which ERP system does this data come from?"
4. Present findings clearly:
   - File structure (rows, columns, format, encoding)
   - Detected ERP system and confidence
   - Column types and sample values
   - Fact vs. dimension classification
5. **STOP — present findings and ask the user to confirm before Phase 2**

---

### Phase 2: Column Mapping
**Goal:** Map source ERP columns to target CDM fields.

6. **Ask the user which CDM model to map to** before calling any tools.
   Example: "Which CDM model should I map this to? (e.g., general_ledger_detail, accounts_payable, fixed_assets)"
   Then call `lookup_cdm_fields` with the confirmed CDM model name.
7. Call `find_past_mappings` with `erp_system` (from Phase 1) and `cdm_name`
8. Build a mapping proposal using CDM field specs and any past mapping patterns
9. Present as a table:

   | Source Column | CDM Field | Confidence | Notes |
   |---|---|---|---|
   | ... | ... | High/Med/Low | ... |

10. **STOP — ask the user to review and adjust mappings before Phase 3**

---

### Phase 3: Transform Config & Preview
**Goal:** Build the transformation config and validate it against real data.

11. Build a `TransformConfig` JSON from the confirmed mappings. Include:
    - `required_columns`: mapped source columns
    - `date_columns`: detected date columns
    - `amount_columns`: detected numeric/amount columns
    - `effective_date`, `posted_date`, `debit_credit`, `dc_indicator`: if present
12. Call `preview_transform` with `config_json` + `file_paths` (same paths as Phase 1)
13. Call `lookup_pipeline_notebook` with `cdm_name` + `erp_system`
    - **If notebook found:** present the notebook path to the user
    - **If NOT found:** ask the user for any additional context about the transformation,
      then call `generate_transform_notebook` with the config, erp_system, and a descriptive title
14. Present preview results: sample rows, column count, any warnings, notebook status
15. **STOP — ask the user to confirm preview looks correct before Phase 4**

---

### Phase 4: Validation
**Goal:** Run 5 quality checks on the transformed data.

16. Call `validate_data` with:
    - `preview_rows_json`: the `sample_rows` JSON from `preview_transform` output
    - `cdm_name`: same model name used throughout
    - `date_columns`: date columns identified during the workflow
    - `debit_col` / `credit_col`: if present in the data
17. Summarize all 5 checks clearly:
    - Completeness (required CDM fields present?)
    - Type consistency (column types match CDM spec?)
    - Null ratios (nulls within acceptable thresholds?)
    - Date range (dates within expected bounds?)
    - Debit/credit balance (debits equal credits?)
18. Flag any failures with specific column names and suggested fixes
19. Present the final validation report

---

### Optional: Schedule as Databricks Job
If the user wants to schedule the transformation:

20. Call `setup_databricks_job` with:
    - `notebook_path`: from `lookup_pipeline_notebook` or `generate_transform_notebook`
    - `cluster_id`: ask the user to provide their cluster ID
    - `job_name`: suggest a descriptive name like `"CDM_{erp_system}_{cdm_name}_transform"`
    - `config_json`: the TransformConfig JSON from Phase 3

---

## Rules

- Complete **ONE phase at a time** — never skip ahead without explicit user confirmation
- If the user adjusts mappings in Phase 2, rebuild the config and redo `preview_transform` before validation
- If a tool call fails, show the error clearly and suggest a fix before retrying
- Always present column mappings as a table, never as a plain list
- Be concise in summaries — the user can ask for more detail if needed
