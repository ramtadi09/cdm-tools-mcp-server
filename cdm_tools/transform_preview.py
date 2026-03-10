"""Pandas-based transform preview engine."""
from __future__ import annotations

import pandas as pd

from cdm_tools.models import TransformConfig


def apply_preview(
    dfs: dict[str, pd.DataFrame],
    config: TransformConfig,
    max_rows: int = 100,
) -> tuple[pd.DataFrame, list[str]]:
    """Apply transform config to DataFrames and return preview + warnings."""
    warnings: list[str] = []

    if not dfs:
        return pd.DataFrame(), ["No DataFrames provided"]

    fact_name = next(iter(dfs))
    df = dfs[fact_name].copy()

    # Join dimension tables
    for join_name, join_cols in config.join_columns.items():
        dim_name = None
        for name in dfs:
            if name != fact_name and name in config.extra_table_columns:
                dim_name = name
                break
            if name != fact_name and join_name.lower() in name.lower():
                dim_name = name
                break

        if dim_name and dim_name in dfs:
            dim_df = dfs[dim_name]
            valid_cols = [c for c in join_cols if c in df.columns and c in dim_df.columns]
            if valid_cols:
                df = df.merge(dim_df, on=valid_cols, how="left", suffixes=("", "_dim"))
            else:
                warnings.append(f"Join columns {join_cols} not found in both tables for {join_name}")
        elif config.extra_table_columns:
            warnings.append(f"Dimension table for join '{join_name}' not found")

    # Select required columns
    if config.required_columns:
        available = [c for c in config.required_columns if c in df.columns]
        missing = [c for c in config.required_columns if c not in df.columns]
        if missing:
            warnings.append(f"Missing required columns: {missing}")
        if available:
            df = df[available]

    # Convert amount columns to numeric
    for col in config.amount_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")

    # Convert date columns to datetime
    for col in config.date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Apply DC indicator transformation
    dci = config.dc_indicator
    if dci.transform_dc_indicators and dci.column and dci.column in df.columns:
        credit_mask = df[dci.column] == dci.credit_value
        cols_to_negate = dci.columns_to_apply_to if dci.columns_to_apply_to else config.amount_columns
        for col in cols_to_negate:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                df.loc[credit_mask, col] = -df.loc[credit_mask, col].abs()

    # Apply debit/credit calculation
    dc = config.debit_credit
    for amount_key, dc_cfg in [("amount_oc", dc.amount_oc), ("amount_ec", dc.amount_ec), ("amount_gc", dc.amount_gc)]:
        if dc_cfg.debit_column and dc_cfg.credit_column:
            debit_col = dc_cfg.debit_column
            credit_col = dc_cfg.credit_column
            if debit_col in df.columns and credit_col in df.columns:
                debit_vals = pd.to_numeric(df[debit_col], errors="coerce").fillna(0)
                credit_vals = pd.to_numeric(df[credit_col], errors="coerce").fillna(0)
                net_col = f"net_{amount_key}"
                if dc_cfg.operator == "-":
                    df[net_col] = debit_vals - credit_vals
                else:
                    df[net_col] = debit_vals + credit_vals

    return df.head(max_rows), warnings
