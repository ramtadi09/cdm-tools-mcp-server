"""Load data files into pandas DataFrames."""
from pathlib import Path

import pandas as pd

from cdm_tools.ingestion.format_detector import FormatInfo


def load_file(file_path: Path, format_info: FormatInfo) -> pd.DataFrame:
    """Load a data file into a pandas DataFrame.

    All columns are loaded as strings to avoid premature type inference.
    """
    if format_info.file_type == "xlsx":
        df = pd.read_excel(file_path, header=format_info.header_row, dtype=str)
    elif format_info.file_type in ("csv", "txt"):
        df = pd.read_csv(
            file_path, sep=format_info.delimiter,
            header=format_info.header_row, dtype=str,
            encoding=format_info.encoding,
        )
    else:
        raise ValueError(f"Unsupported file type: {format_info.file_type}")

    df.columns = [str(c).strip() for c in df.columns]
    return df
