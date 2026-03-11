# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Knowledge Base Delta Tables (Idempotent)
# MAGIC
# MAGIC **Run this ONCE** to set up the knowledge base tables.
# MAGIC
# MAGIC Creates 3 Delta tables if they don't exist:
# MAGIC - `cdm_definitions` - CDM field specifications
# MAGIC - `erp_schemas` - ERP system column patterns
# MAGIC - `mapping_history` - Past transformation configs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog settings - update these for your environment
CATALOG = "cortex_dev_catalog"
SCHEMA = "0000_ram"
VOLUME_NAME = "cdm_mcp_kb"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
VOLUME_BASE = f"{VOLUME_PATH}/knowledge_base"

# Set to True to force recreate tables even if they exist
FORCE_RECREATE = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Path
# MAGIC
# MAGIC Volume should already exist - create it via CLI before running:
# MAGIC ```
# MAGIC databricks volumes create cortex_dev_catalog 0000_ram cdm_mcp_kb MANAGED --profile dev
# MAGIC ```

# COMMAND ----------

print(f"Using Volume: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

import json

def table_exists(catalog: str, schema: str, table: str) -> bool:
    """Check if a Delta table exists."""
    try:
        spark.table(f"{catalog}.{schema}.{table}")
        return True
    except Exception:
        return False

def should_create_table(table_name: str) -> bool:
    """Determine if we should create the table."""
    exists = table_exists(CATALOG, SCHEMA, table_name)
    if exists and not FORCE_RECREATE:
        print(f"Table {CATALOG}.{SCHEMA}.{table_name} already exists. Skipping.")
        return False
    if exists and FORCE_RECREATE:
        print(f"Table {CATALOG}.{SCHEMA}.{table_name} exists but FORCE_RECREATE=True. Recreating.")
    return True

def check_volume_files():
    """Check if knowledge_base files exist in Volume."""
    try:
        files = dbutils.fs.ls(VOLUME_BASE)
        print(f"Found {len(files)} items in {VOLUME_BASE}")
        for f in files:
            print(f"  - {f.name}")
        return True
    except Exception as e:
        print(f"ERROR: Knowledge base files not found in {VOLUME_BASE}")
        print(f"Please upload knowledge_base folder to the Volume first:")
        print(f"  databricks fs cp -r ./knowledge_base {VOLUME_PATH}/knowledge_base --recursive")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Prerequisites

# COMMAND ----------

if not check_volume_files():
    dbutils.notebook.exit("FAILED: Knowledge base files not uploaded to Volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Seed CDM Definitions

# COMMAND ----------

if should_create_table("cdm_definitions"):
    cdm_specs_dir = f"{VOLUME_BASE}/cdm_specs"
    cdm_rows = []

    for fname in dbutils.fs.ls(cdm_specs_dir):
        if fname.name.endswith(".json"):
            content = dbutils.fs.head(fname.path, 1_000_000)
            spec = json.loads(content)
            cdm_rows.append({
                "cdm_name": spec["data_model"],
                "fields_json": json.dumps(spec["fields"]),
                "pipeline_count": spec.get("pipeline_count", 0),
            })

    df_cdm = spark.createDataFrame(cdm_rows)
    df_cdm.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.cdm_definitions")
    print(f"Created cdm_definitions: {len(cdm_rows)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seed ERP Schemas

# COMMAND ----------

if should_create_table("erp_schemas"):
    erp_path = f"{VOLUME_BASE}/erp_schemas.json"
    content = dbutils.fs.head(erp_path, 5_000_000)
    erp_list = json.loads(content)

    erp_rows = []
    for erp in erp_list:
        erp_rows.append({
            "erp_system": erp["erp_system"],
            "pipeline_ids_json": json.dumps(erp["pipeline_ids"]),
            "data_models_json": json.dumps(erp["data_models"]),
            "known_columns_json": json.dumps(erp["known_columns"]),
            "file_patterns_json": json.dumps(erp.get("file_patterns", {})),
            "multi_file_specs_json": json.dumps(erp.get("multi_file_specs")),
            "dc_indicator_patterns_json": json.dumps(erp.get("dc_indicator_patterns")),
            "debit_credit_patterns_json": json.dumps(erp.get("debit_credit_patterns")),
        })

    df_erp = spark.createDataFrame(erp_rows)
    df_erp.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.erp_schemas")
    print(f"Created erp_schemas: {len(erp_rows)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seed Mapping History

# COMMAND ----------

if should_create_table("mapping_history"):
    configs_dir = f"{VOLUME_BASE}/configs"
    config_rows = []

    for fname in dbutils.fs.ls(configs_dir):
        if fname.name.endswith(".json"):
            content = dbutils.fs.head(fname.path, 1_000_000)
            config = json.loads(content)
            pipeline_id = config.get("_pipeline_id", fname.name.replace(".json", ""))
            config_rows.append({
                "pipeline_id": pipeline_id,
                "erp_system": config.get("erp_system", ""),
                "data_model": config.get("data_model", "general_ledger_detail"),
                "config_json": json.dumps(config),
                "source_notebook": config.get("_source_notebook", ""),
            })

    df_configs = spark.createDataFrame(config_rows)
    df_configs.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.mapping_history")
    print(f"Created mapping_history: {len(config_rows)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables

# COMMAND ----------

print("\n" + "="*50)
print("Knowledge Base Tables Status:")
print("="*50)

for table in ["cdm_definitions", "erp_schemas", "mapping_history"]:
    full_name = f"{CATALOG}.{SCHEMA}.{table}"
    if table_exists(CATALOG, SCHEMA, table):
        count = spark.table(full_name).count()
        print(f"  {table}: {count} rows")
    else:
        print(f"  {table}: NOT FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC Tables created in: `cortex_dev_catalog.0000_ram`
# MAGIC - `cdm_definitions`
# MAGIC - `erp_schemas`
# MAGIC - `mapping_history`
