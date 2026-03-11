# Databricks notebook source
# MAGIC %md
# MAGIC # ADP Transformation Template - [{{NOTEBOOK_TITLE}}]
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook transforms **[{{NOTEBOOK_TITLE}}]** data to CDM format following ADP (Automated Data Processing) standards.
# MAGIC It implements clean architecture principles and serves as a template for data specialists.
# MAGIC
# MAGIC ## Required Parameters
# MAGIC - **input_id**: Unique identifier for this transformation (e.g., "DYNAMICS_001")
# MAGIC - **source_file_path**: Path to source data file(s)
# MAGIC - **target_file_path**: Output path (format: RAW/{datamodel}_{input_id}.parquet)
# MAGIC - **notebook_name**: Name of this transformation notebook
# MAGIC - **report_id**: Report ID for CDM mapping retrieval

# COMMAND ----------

# DBTITLE 1,Install CortexPy with Databricks Serverless Configuration
# Install CortexPy with proper configuration for Databricks Serverless environments
%pip install cortexpy --no-cache-dir --quiet --index-url https://pypi.org/simple/ --trusted-host pypi.org

# COMMAND ----------

# DBTITLE 1,Restart Python Environment
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load ADP Utilities
# MAGIC %run ./adp_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ##🔴 1. Parameter Definition and Validation
# MAGIC This section implements the required ADP parameter system with comprehensive validation.
# MAGIC All transformation notebooks **MUST** implement these parameters for SJM integration.

# COMMAND ----------

# DBTITLE 1,🔴Parameter Widgets Definition - Complete ADP Specification
setup_adp_parameter_widgets()

# COMMAND ----------

# DBTITLE 1,🔴Load and Validate Parameters
import json
import traceback

try:
    params = ADPTransformationParameters()

    print("=" * 80)
    print("Parameters loaded successfully")
    print("=" * 80)
    print(f"Notebook: {params.notebook_name}")
    print(f"ERP System: {params.erp_system_name}")
    print(f"Data Model: {params.datamodel_tech_name}")
    print(f"Input ID: {params.input_id}")
    print(f"Source: {params.source_file_path}")
    print(f"Target: {params.target_file_path}")
    print("=" * 80)

    # Verify source file exists
    try:
        file_info = dbutils.fs.ls(params.source_file_path)
        print(f"Source file verified")
    except Exception as e:
        print(f"Warning: Could not verify source file: {e}")

except Exception as e:
    error_response = {
        "Status": "FAILED",
        "ErrorMessageKey": f"Parameter initialization failed: {str(e)}",
        "ErrorType": type(e).__name__,
        "Traceback": traceback.format_exc()
    }
    print("PARAMETER ERROR:", json.dumps(error_response, indent=2))
    dbutils.notebook.exit(json.dumps(error_response))

# COMMAND ----------

# MAGIC %md
# MAGIC ##🔴 2. Initialization Phase
# MAGIC
# MAGIC This phase initializes the environment, installs dependencies, and sets up the context.

# COMMAND ----------

# DBTITLE 1,🔴Environment Variables and Secrets
"""Environment Setup - Secure secret management"""

import os


print(f"Input ID: {params.input_id}")
print(f"Source: {params.source_file_path}")
print(f"Target: {params.target_file_path}")
print(f"Report ID: {params.report_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##🔴 3. Import Libraries and Setup Logging
# MAGIC
# MAGIC Import required libraries and configure logging for the transformation process.

# COMMAND ----------

# DBTITLE 1,🔴Import Libraries and Setup Global Context
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

"""Library Imports and Global Context Setup"""

# Core CortexPy imports
from cortexpy.context.entry_context import *
from cortexpy.helper.utils import *
from cortexpy.helper.logging_helper import *
from cortexpy.helper.enums import *

# PySpark imports
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, NullType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

# Standard library imports
import json
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# Setup global logger and context
logger, cp = setup_adp_logger_context(__name__)

logger.info(f"Initialized transformation context for: {params.input_id}")
logger.info(f"CortexPy context ready: {type(cp)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##🟢 4. ERP-Specific Transformation Template
# MAGIC
# MAGIC This section defines the transformation template.

# COMMAND ----------

# DBTITLE 1,🟢ERP-Specific Transformation Configuration
"""ERP-Specific Transformation Configuration"""

{{TRANSFORMATION_CONFIG}}

### Extra parameters for template processing
# validate_early: Validate columns before transformation (slower but safer) (Default value is True)
# enable_caching: Cache result for reuse (uses memory) (Default value is True)
# strict_validation: Fail on missing columns (False = warn only) (Default value is True)
validate_early = True
enable_caching = True
strict_validation = True

# COMMAND ----------

# DBTITLE 1,🔴Assigning Config Variables
{{CONFIG_VARIABLE_ASSIGNMENT}}

# COMMAND ----------

# MAGIC %md
# MAGIC ##🔴 5. File Processing and Data Loading
# MAGIC
# MAGIC This section handles file processing including archive extraction and format detection.

# COMMAND ----------

# DBTITLE 1,🔴Main File Processing Execution
try:
    logger.info(f"Started transformation process for {params.input_id}")

    source_paths = [params.source_file_path]
    logger.info(f"Source file path: {params.source_file_path}")

    # Process input files (handles archives and regular files)
    processed_files = process_input_files(source_paths)
    logger.info(f"Processing {len(processed_files)} files")

    # Initialize data containers
    gl_df = None
    extra_table_dataframes = {}

    # Process each file
    for file_path in processed_files:
        logger.info(f"Processing file: {file_path}")

        # Detect file format
        format_info = detect_file_formats(file_path)
        logger.info(f"Format detected: {format_info}")

        # Load data with detected format
        file_df = load_data_with_format(file_path, format_info, header_offset=header)

        # Classify data type and merge
        classification = classify_and_process_data(file_df)

        # Union files together
        gl_df, extra_table_dataframes = union_files(classification, extra_table_columns, gl_df, extra_table_dataframes)


    # Validation - Check if GL data exists
    if gl_df is None:
        raise ColumnValidationError("No GL data found in input files")
    elif enable_caching:
        gl_df = gl_df.cache()
        logger.info("Fact dataframe flagged for cache")

    # Validate input data AFTER processing all files
    gl_df = validate_input_data(gl_df, params, required_columns)
    logger.info(f"Data validation passed for all files")

    # Count once after all files processed
    gl_count = gl_df.count()
    logger.info(f"File processing completed - GL records: {gl_count}")


    if extra_table_dataframes:
        for table_name, columns in extra_table_columns.items():
            if extra_table_dataframes[table_name] is not None:
                # Validate input data AFTER processing all files
                extra_table_dataframes[table_name] = validate_input_data(extra_table_dataframes[table_name], params, columns)

                table_count = extra_table_dataframes[table_name].count()
                logger.info(f"{table_name} data loaded: {table_count} records")
            logger.info(f"Data validation passed for all extra files")

    # Execute data indexing
    if gl_df is None:
        raise ColumnValidationError("GL data not found - cannot proceed with transformation")

    gl_df = add_row_indexing(gl_df)



except (ValueError, ColumnValidationError) as validation_error:
    logger.error(f"Data validation failed: {validation_error}")

    error_response = {
        "Status": "FAILED",
        "ErrorMessageKey": f"Data validation failed: {str(validation_error)}",
        "ErrorType": type(validation_error).__name__,
        "Traceback": traceback.format_exc(),
        "InputId": params.input_id,
        "SourcePath": params.source_file_path
    }

    print("=" * 80)
    print("DATA VALIDATION ERROR")
    print("=" * 80)
    print(json.dumps(error_response, indent=2))
    print("=" * 80)

    dbutils.notebook.exit(json.dumps(error_response))

except Exception as e:
    logger.error(f"File processing failed: {e}")

    error_response = {
        "Status": "FAILED",
        "ErrorMessageKey": f"File processing failed: {str(e)}",
        "ErrorType": type(e).__name__,
        "Traceback": traceback.format_exc(),
        "InputId": params.input_id if 'params' in locals() else "unknown",
        "SourcePath": params.source_file_path if 'params' in locals() else "unknown"
    }

    print("=" * 80)
    print("FILE PROCESSING ERROR")
    print("=" * 80)
    print(json.dumps(error_response, indent=2))
    print("=" * 80)

    dbutils.notebook.exit(json.dumps(error_response))

# COMMAND ----------

# MAGIC %md
# MAGIC ##🟡 6/7. Custom Transforms and Join Logic
# MAGIC [Optional] ERP-specific joins, custom column creation, and pre-processing.

# COMMAND ----------

# DBTITLE 1,🟡Custom Transform Section
{{CUSTOM_TRANSFORM_SECTION}}

# COMMAND ----------

# MAGIC %md
# MAGIC ##🔴 8. Template-Based Data Transformation
# MAGIC
# MAGIC Apply template-driven transformations using the shared `process_template_transformation()` function from adp_utils.

# COMMAND ----------

# DBTITLE 1,🔴Template-Based Data Transformation

try:
    df = gl_df

    # Check if DC indicator config has been populated
    if transformation_vars['dc_indicator']['columns_to_apply_to']:
        apply_dc_indicators = True
    else:
        apply_dc_indicators = False

    # Check if Debit-Credit config has been populated:
    debit_credit_config = transformation_vars['debit_credit']
    apply_debit_credit = False
    for debit_credit_dict in debit_credit_config.values():
        if debit_credit_dict.get("debit_column") or debit_credit_dict.get("credit_column"):
            apply_debit_credit = True


    # Apply template-based transformations
    df = process_template_transformation(df,
                                         transformation_config,
                                         apply_dc_indicators=apply_dc_indicators,
                                         transform_dc_indicators=transform_dc_indicators,
                                         apply_debit_credit=apply_debit_credit,
                                         validate_early=validate_early,
                                         enable_caching=enable_caching,
                                         strict_validation=strict_validation,
                                         logger=logger)
    # Validate transformation
    if df.isEmpty():
        raise TransformationError("Transformation resulted in zero records")

    transformed_count = df.count()
    logger.info(f"Template transformation completed - {transformed_count} records")

except Exception as e:
    logger.error(f"Template transformation failed: {e}")

    error_response = {
        "Status": "FAILED",
        "ErrorMessageKey": f"Template transformation failed: {str(e)}",
        "ErrorType": type(e).__name__,
        "Traceback": traceback.format_exc(),
        "InputId": params.input_id
    }

    print("=" * 80)
    print("TRANSFORMATION ERROR")
    print("=" * 80)
    print(json.dumps(error_response, indent=2))
    print("=" * 80)

    dbutils.notebook.exit(json.dumps(error_response))

# COMMAND ----------

# MAGIC %md
# MAGIC ##🔴 9. CDM Mapping and Final Response Generation
# MAGIC - Apply CDM (Common Data Model) mapping to transform data to standardized format.
# MAGIC - Save the transformed data in the required STRING format to the specified output path.
# MAGIC - Construct the final response with field mapping.

# COMMAND ----------

# DBTITLE 1,🔴CDM Mapping and Final Response Generation
{{CDM_MAPPING_SECTION}}

# COMMAND ----------

# MAGIC %md
# MAGIC ##🔴 10. Notebook Exit
# MAGIC
# MAGIC Return the final response to the calling notebook/system.
# MAGIC

# COMMAND ----------

# DBTITLE 1,🔴Output Final Response
import json

# Log completion
if response['Status']  == "SUCCESS":
    logger.info(f"Transformation completed successfully for {params.input_id}")
else:
    logger.warning(f"Transformation completed with errors for {params.input_id}")

# Exit with response
response_json = json.dumps(response, indent=4)
print("=" * 80)
print(f"Final Status: {response['Status'] }")
print(f"Response: {response_json[:200]}...")
print("=" * 80)

# COMMAND ----------

# DBTITLE 1,🔴 Exit Notebook
# NO TRY-CATCH! Just exit directly
dbutils.notebook.exit(response_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Notebook Complete
# MAGIC
# MAGIC This ADP transformation notebook has completed execution.
# MAGIC
# MAGIC **Key Features Implemented:**
# MAGIC - Full parameterization for SJM integration
# MAGIC - Job status managed by adp_master_notebook
# MAGIC - Archive handling with CortexPy utilities
# MAGIC - Standardized format detection functions
# MAGIC - Template-based transformations
# MAGIC - CDM mapping with proper error handling
# MAGIC - STRING format output to RAW/{datamodel}_{input_id}.parquet
# MAGIC - Comprehensive error handling with custom exceptions
# MAGIC - Clean architecture implementation
# MAGIC
# MAGIC **For Data Specialists:**
# MAGIC - Update the template configuration in Section 6 for different ERP systems
# MAGIC - Modify field mappings as needed for your specific data structure
# MAGIC - Follow the parameter pattern for consistent SJM integration
# MAGIC - Use this notebook as a template for additional transformation notebooks
# MAGIC
# MAGIC ---
