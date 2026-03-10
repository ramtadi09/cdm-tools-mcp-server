"""Profile DataFrame columns — infer types, count nulls, detect patterns."""
import re
from dataclasses import dataclass

import pandas as pd


@dataclass
class ColumnProfile:
    name: str
    inferred_type: str
    total_count: int
    null_count: int
    unique_count: int
    sample_values: list[str]


DATE_PATTERNS = [
    r"^\d{4}-\d{2}-\d{2}$",
    r"^\d{2}/\d{2}/\d{4}$",
    r"^\d{2}-\d{2}-\d{4}$",
    r"^\d{2}\.\d{2}\.\d{4}$",
    r"^\d{4}/\d{2}/\d{2}$",
    r"^\d{8}$",
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}",
    r"^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}",
]
_compiled_date_patterns = [re.compile(p) for p in DATE_PATTERNS]


def profile_columns(df: pd.DataFrame) -> dict[str, ColumnProfile]:
    """Profile all columns in a DataFrame."""
    profiles = {}
    for col in df.columns:
        profiles[col] = _profile_single_column(df[col], col)
    return profiles


def _profile_single_column(series: pd.Series, name: str) -> ColumnProfile:
    total = len(series)
    null_count = int(series.isna().sum() + (series == "").sum())
    non_null = series.dropna()
    non_null = non_null[non_null != ""]
    unique_count = int(non_null.nunique())
    sample_values = non_null.head(5).tolist()

    inferred_type = _infer_type(non_null)

    return ColumnProfile(
        name=name, inferred_type=inferred_type, total_count=total,
        null_count=null_count, unique_count=unique_count,
        sample_values=[str(v) for v in sample_values],
    )


def _infer_type(series: pd.Series) -> str:
    """Infer column type from non-null string values."""
    if len(series) == 0:
        return "string"

    sample = series.head(100)

    numeric_count = 0
    for val in sample:
        s = str(val).strip().replace(",", "")
        if s == "":
            continue
        try:
            float(s)
            numeric_count += 1
        except (ValueError, TypeError):
            pass

    non_empty = sum(1 for v in sample if str(v).strip() != "")
    if non_empty > 0 and numeric_count / non_empty > 0.8:
        return "numeric"

    date_count = 0
    for val in sample:
        s = str(val).strip()
        if s == "":
            continue
        if any(p.match(s) for p in _compiled_date_patterns):
            date_count += 1

    if non_empty > 0 and date_count / non_empty > 0.8:
        return "date"

    bool_values = {"true", "false", "yes", "no", "1", "0", "y", "n"}
    bool_count = sum(1 for v in sample if str(v).strip().lower() in bool_values)
    if non_empty > 0 and bool_count / non_empty > 0.8:
        return "boolean"

    return "string"
