"""Classify multiple uploaded files as fact vs dimension tables and infer joins."""
from dataclasses import dataclass

import pandas as pd


@dataclass
class JoinSpec:
    fact_table: str
    dimension_table: str
    join_columns: list[str]
    join_type: str = "left"


@dataclass
class FileClassification:
    fact_table: str
    dimension_tables: list[str]
    joins: list[JoinSpec]


_AMOUNT_KEYWORDS = {
    "amount", "amt", "value", "debit", "credit", "dmbtr", "wrbtr",
    "total", "balance", "sum", "price", "cost", "currency",
}


def classify_files(dfs: dict[str, pd.DataFrame]) -> FileClassification:
    """Classify uploaded files as fact vs dimension tables."""
    if len(dfs) == 1:
        name = next(iter(dfs))
        return FileClassification(fact_table=name, dimension_tables=[], joins=[])

    scores = {}
    for name, df in dfs.items():
        score = float(len(df))
        for col in df.columns:
            if any(kw in col.lower() for kw in _AMOUNT_KEYWORDS):
                score += 1000
                break
        scores[name] = score

    sorted_files = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    fact_name = sorted_files[0][0]
    dim_names = [name for name, _ in sorted_files[1:]]

    joins = []
    fact_cols = set(dfs[fact_name].columns)
    for dim_name in dim_names:
        dim_cols = set(dfs[dim_name].columns)
        overlap = sorted(fact_cols & dim_cols)
        if overlap:
            joins.append(JoinSpec(
                fact_table=fact_name, dimension_table=dim_name,
                join_columns=overlap, join_type="left",
            ))

    return FileClassification(fact_table=fact_name, dimension_tables=dim_names, joins=joins)
