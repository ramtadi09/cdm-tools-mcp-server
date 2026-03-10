# Sample Data Inventory

This knowledge base was built by extracting `transformation_config` dicts and schema metadata from 20 sample pipeline notebooks in `sample_data/`. The sample data itself is gitignored (contains client test data), but this inventory documents what was used to seed the knowledge base.

## Source Locations

- **Transform Notebooks**: Azure DevOps `content-library-artifacts` repo, `develop` branch
  - Path: `/files/workspace/CONTENT_LIBRARY/NOTEBOOKS/adp/`
- **InputFile Zips**: SharePoint Content Contribution site
  - Path: `Content Tracker/Content Packages/Manual Extraction Reports/`

## Pipeline Packages

| System | Pipeline ID | Transform Notebook | Input File | Active | Status |
|--------|------------|-------------------|------------|--------|--------|
| SAP | 3 | Transform_SAP_ID3.py | ID3_InputFile.zip | Y | Complete |
| SAP | 21 | Transform_SAP_ID21.py | ID21_InputFile.zip | Y | Complete |
| SAP | 22 | Transform_SAP_ID22.py | ID22_InputFile.zip | Y | Complete |
| SAP | 24 | Transform_SAP_ID24.py | ID24_InputFile.zip | Y | Complete |
| SAP | 28 | Transform_SAP_ID28.py | ID28_InputFile.zip | Y | Complete |
| Generic | 74 | Transform_Generic_Serengeti_ID74.py | ID74_InputFile.zip | Y | Complete |
| Generic | 75 | Transform_Generic_Serengeti_AP_ID75.py | ID75_InputFile.zip | Y | Complete |
| Generic | 76 | Transform_Generic_Serengeti_AR_ID76.py | ID76_InputFile.zip | Y | Complete |
| Oracle | 2 | Transform_Oracle_ID2.py | ID2_InputFile.zip | N | Complete |
| Oracle | 12 | Transform_Oracle_ID12.py | ID12_InputFile.zip | N | Complete |
| Oracle | 13 | Transform_Oracle_ID13.py | ID13_InputFile.zip | N | Complete |
| Oracle | 14 | Transform_Oracle_ID14.py | ID14_InputFile.zip | N | Complete |
| Oracle | 23 | Transform_Oracle_ID23.py | ID23_InputFile.zip | N | Complete |
| Oracle | 29 | Transform_Oracle_ID29.py | ID29_InputFile.zip | N | Complete |
| Dynamics | 6 | Transform_Dynamics_ID6.py | ID6_InputFile.zip | N | Complete |
| Dynamics | 15 | Transform_Dynamics_ID15.py | ID15_InputFile.zip | N | Complete |
| Dynamics | 16 | Transform_Dynamics_ID16.py | ID16_InputFile.zip | N | Complete |
| Sage | 4 | Transform_Sage_ID4.py | ID4_InputFile.zip | N | Complete |
| Sage | 19 | Transform_Sage_ID19.py | ID19_InputFile.zip | N | Complete |
| Sage | 20 | Transform_Sage_ID20.py | ID20_InputFile.zip | N | Complete |

## Summary by ERP

- **SAP** (5 pipelines): XLSX + TXT, one multi-file (BSEG+BKPF join), DC indicator transform (`SHKZG`)
- **Oracle** (6 pipelines): XLSX + CSV, single-file, standard GL columns
- **Dynamics** (3 pipelines): XLSX + TXT, one multi-file (User+GL join)
- **Sage** (3 pipelines): XLSX only, report-format with embedded headers, separate Debit/Credit columns
- **Generic** (3 pipelines): TXT + CSV, opaque field names (`DS001.Fxx`), includes AP and AR data

## What Was Extracted

Each pipeline's transform notebook was parsed to produce:
- `configs/` — `transformation_config` dicts as JSON (one per pipeline)
- `erp_schemas.json` — known column patterns per ERP system
- `pipeline_notebooks.json` — notebook path registry
- `cdm_specs/` — CDM field definitions derived from target field names across all notebooks
- `templates/` — reusable transform and validation templates
