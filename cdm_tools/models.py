"""Pydantic models for CDM tool data contracts."""
from __future__ import annotations

from pydantic import BaseModel, Field


# ── Schema Analyzer I/O ──


class FileInfo(BaseModel):
    file_path: str
    file_type: str
    delimiter: str | None = None
    encoding: str | None = None
    header_row: int = 0
    report_format: bool = False
    row_count: int = 0
    column_count: int = 0


class ColumnProfileModel(BaseModel):
    name: str
    inferred_type: str
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    sample_values: list[str] = Field(default_factory=list)


class JoinSpecModel(BaseModel):
    fact_table: str
    dimension_table: str
    join_columns: list[str]
    join_type: str = "left"


class SchemaReport(BaseModel):
    files: list[FileInfo] = Field(default_factory=list)
    profiles: dict[str, list[ColumnProfileModel]] = Field(default_factory=dict)
    fact_table: str | None = None
    dimension_tables: list[str] = Field(default_factory=list)
    joins: list[JoinSpecModel] = Field(default_factory=list)
    detected_erp: str | None = None


# ── Transform Agent I/O ──


class DebitCreditAmountConfig(BaseModel):
    debit_column: str = ""
    credit_column: str = ""
    operator: str = "-"


class DebitCreditConfig(BaseModel):
    amount_oc: DebitCreditAmountConfig = Field(default_factory=DebitCreditAmountConfig)
    amount_ec: DebitCreditAmountConfig = Field(default_factory=DebitCreditAmountConfig)
    amount_gc: DebitCreditAmountConfig = Field(default_factory=DebitCreditAmountConfig)


class DCIndicatorConfig(BaseModel):
    column: str = ""
    credit_value: str = "C"
    valid_values: list[str] = Field(default_factory=lambda: ["D", "C"])
    columns_to_apply_to: list[str] = Field(default_factory=list)
    transform_dc_indicators: bool = False


class TransformConfig(BaseModel):
    erp_system: str = ""
    data_model: str = "general_ledger_detail"
    read_in_variables: dict = Field(default_factory=lambda: {"header": 0, "report_format": False})
    required_columns: list[str] = Field(default_factory=list)
    extra_table_columns: dict[str, list[str]] = Field(default_factory=dict)
    join_columns: dict[str, list[str]] = Field(default_factory=dict)
    date_columns: list[str] = Field(default_factory=list)
    amount_columns: list[str] = Field(default_factory=list)
    effective_date: str = ""
    posted_date: str = ""
    debit_credit: DebitCreditConfig = Field(default_factory=DebitCreditConfig)
    dc_indicator: DCIndicatorConfig = Field(default_factory=DCIndicatorConfig)

    @classmethod
    def from_raw_config(cls, raw: dict) -> TransformConfig:
        """Parse a raw transformation_config JSON (CortexPy format) into TransformConfig."""
        tv = raw.get("transformation_variables", {})
        dc_raw = tv.get("debit_credit", {})
        dci_raw = tv.get("dc_indicator", {})
        return cls(
            erp_system=raw.get("erp_system", ""),
            data_model=raw.get("data_model", "general_ledger_detail"),
            read_in_variables=raw.get("read_in_variables", {"header": 0, "report_format": False}),
            required_columns=tv.get("required_columns", []),
            extra_table_columns=tv.get("extra_table_columns", {}),
            join_columns=tv.get("join_columns", {}),
            date_columns=tv.get("date_columns", []),
            amount_columns=tv.get("amount_columns", []),
            effective_date=tv.get("effective_date", ""),
            posted_date=tv.get("posted_date", ""),
            debit_credit=DebitCreditConfig(
                amount_oc=DebitCreditAmountConfig(**dc_raw.get("amount_oc", {})) if dc_raw.get("amount_oc") else DebitCreditAmountConfig(),
                amount_ec=DebitCreditAmountConfig(**dc_raw.get("amount_ec", {})) if dc_raw.get("amount_ec") else DebitCreditAmountConfig(),
                amount_gc=DebitCreditAmountConfig(**dc_raw.get("amount_gc", {})) if dc_raw.get("amount_gc") else DebitCreditAmountConfig(),
            ),
            dc_indicator=DCIndicatorConfig(**dci_raw) if dci_raw else DCIndicatorConfig(),
        )


class TransformPreview(BaseModel):
    row_count: int = 0
    column_count: int = 0
    columns: list[str] = Field(default_factory=list)
    sample_rows: list[dict] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class JobSetupResult(BaseModel):
    job_id: int | None = None
    job_url: str = ""
    notebook_path: str = ""
    status: str = "created"
    message: str = ""


class NotebookGenerationResult(BaseModel):
    notebook_code: str
    notebook_title: str
    erp_system: str
    cdm_name: str = "general_ledger_detail"
    has_joins: bool = False
    has_custom_transforms: bool = False
    warnings: list[str] = Field(default_factory=list)
    suggested_filename: str = ""


# ── Validation Agent I/O ──


class ValidationCheck(BaseModel):
    name: str
    passed: bool
    message: str
    details: dict = Field(default_factory=dict)


class ValidationReport(BaseModel):
    checks: list[ValidationCheck] = Field(default_factory=list)
    overall_pass: bool = True
    summary: str = ""
