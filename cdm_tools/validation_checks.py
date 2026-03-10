"""5 rule-based validation checks on preview DataFrames."""
from __future__ import annotations

import pandas as pd

from cdm_tools.models import ValidationCheck, ValidationReport


def check_completeness(df: pd.DataFrame, required_fields: list[str]) -> ValidationCheck:
    missing = [f for f in required_fields if f not in df.columns]
    passed = len(missing) == 0
    return ValidationCheck(
        name="completeness", passed=passed,
        message=f"All {len(required_fields)} required fields present" if passed
                else f"Missing {len(missing)} fields: {missing}",
        details={"missing_fields": missing, "total_required": len(required_fields)},
    )


def check_type_consistency(df: pd.DataFrame, field_specs: dict) -> ValidationCheck:
    issues = []
    for field_name, spec in field_specs.items():
        if field_name not in df.columns:
            continue
        col = df[field_name]
        if spec.get("type") == "decimal":
            non_null = col.dropna()
            if len(non_null) > 0:
                numeric = pd.to_numeric(non_null, errors="coerce")
                bad_count = int(numeric.isna().sum())
                if bad_count > 0:
                    issues.append(f"{field_name}: {bad_count} non-numeric values")
        elif spec.get("type") == "date":
            non_null = col.dropna()
            if len(non_null) > 0 and not pd.api.types.is_datetime64_any_dtype(non_null):
                parsed = pd.to_datetime(non_null, errors="coerce")
                bad_count = int(parsed.isna().sum())
                if bad_count > 0:
                    issues.append(f"{field_name}: {bad_count} unparseable dates")

    passed = len(issues) == 0
    return ValidationCheck(
        name="type_consistency", passed=passed,
        message="All field types consistent" if passed else f"{len(issues)} type issues found",
        details={"issues": issues},
    )


def check_null_ratios(df: pd.DataFrame, threshold: float = 0.5) -> ValidationCheck:
    deduped = df.loc[:, ~df.columns.duplicated()]
    high_null_cols = []
    for col in deduped.columns:
        series = deduped[col]
        null_count = int(series.isna().sum()) + int((series == "").sum())
        null_ratio = null_count / len(deduped) if len(deduped) > 0 else 0
        if null_ratio > threshold:
            high_null_cols.append({"column": col, "null_ratio": round(float(null_ratio), 3)})

    passed = len(high_null_cols) == 0
    return ValidationCheck(
        name="null_ratios", passed=passed,
        message=f"All columns below {threshold*100}% null threshold" if passed
                else f"{len(high_null_cols)} columns exceed {threshold*100}% nulls",
        details={"high_null_columns": high_null_cols},
    )


def check_date_range(df: pd.DataFrame, date_columns: list[str]) -> ValidationCheck:
    issues = []
    for col in date_columns:
        if col not in df.columns:
            continue
        dates = pd.to_datetime(df[col], errors="coerce").dropna()
        if len(dates) == 0:
            issues.append(f"{col}: no valid dates")
            continue
        min_date = dates.min()
        max_date = dates.max()
        if min_date.year < 1990:
            issues.append(f"{col}: earliest date {min_date.date()} is before 1990")
        if max_date.year > 2030:
            issues.append(f"{col}: latest date {max_date.date()} is after 2030")

    passed = len(issues) == 0
    return ValidationCheck(
        name="date_range", passed=passed,
        message="Date ranges look reasonable" if passed else f"{len(issues)} date range issues",
        details={"issues": issues},
    )


def check_balance(df: pd.DataFrame, debit_col: str, credit_col: str) -> ValidationCheck:
    if debit_col not in df.columns or credit_col not in df.columns:
        return ValidationCheck(
            name="balance", passed=True,
            message=f"Balance check skipped — columns '{debit_col}'/'{credit_col}' not found",
            details={"skipped": True},
        )

    debits = pd.to_numeric(df[debit_col], errors="coerce").fillna(0).sum()
    credits = pd.to_numeric(df[credit_col], errors="coerce").fillna(0).sum()
    diff = abs(float(debits) - float(credits))
    total = abs(float(debits)) + abs(float(credits))
    tolerance = total * 0.001 if total > 0 else 0

    passed = diff <= tolerance
    return ValidationCheck(
        name="balance", passed=passed,
        message=f"Debits ({debits:.2f}) ≈ Credits ({credits:.2f})" if passed
                else f"Imbalance: debits={debits:.2f}, credits={credits:.2f}, diff={diff:.2f}",
        details={"debits": float(debits), "credits": float(credits), "difference": diff},
    )


def run_all_checks(
    df: pd.DataFrame,
    required_fields: list[str] | None = None,
    field_specs: dict | None = None,
    date_columns: list[str] | None = None,
    debit_col: str = "",
    credit_col: str = "",
    null_threshold: float = 0.5,
) -> ValidationReport:
    """Run all 5 validation checks and return a report."""
    checks = [
        check_completeness(df, required_fields or []),
        check_type_consistency(df, field_specs or {}),
        check_null_ratios(df, null_threshold),
        check_date_range(df, date_columns or []),
        check_balance(df, debit_col, credit_col),
    ]

    overall = all(c.passed for c in checks)
    fail_names = [c.name for c in checks if not c.passed]
    summary = "All checks passed" if overall else f"Failed checks: {', '.join(fail_names)}"

    return ValidationReport(checks=checks, overall_pass=overall, summary=summary)
