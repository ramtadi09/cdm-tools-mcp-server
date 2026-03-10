"""Detect file format, delimiter, encoding, and header row for ERP data files."""
import csv
from dataclasses import dataclass
from pathlib import Path

import chardet


@dataclass
class FormatInfo:
    file_type: str
    delimiter: str | None
    encoding: str | None
    header_row: int
    report_format: bool


def detect_format(file_path: Path) -> FormatInfo:
    """Detect format metadata for a data file."""
    suffix = file_path.suffix.lower()

    if suffix == ".xlsx":
        return FormatInfo(
            file_type="xlsx", delimiter=None, encoding=None,
            header_row=0, report_format=False,
        )

    raw_bytes = file_path.read_bytes()
    detected = chardet.detect(raw_bytes[:10000])
    encoding = detected.get("encoding", "utf-8") or "utf-8"

    text = raw_bytes[:10000].decode(encoding, errors="replace")
    lines = text.splitlines()

    delimiter = _sniff_delimiter(lines)
    file_type = "csv" if suffix == ".csv" else "txt"

    return FormatInfo(
        file_type=file_type, delimiter=delimiter, encoding=encoding,
        header_row=0, report_format=False,
    )


def _sniff_delimiter(lines: list[str]) -> str:
    """Detect delimiter from first few lines using csv.Sniffer."""
    if not lines:
        return ","

    sample = "\n".join(line for line in lines[:20] if line.strip())

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t|;")
        return dialect.delimiter
    except csv.Error:
        first_line = lines[0]
        counts = {
            "\t": first_line.count("\t"),
            ",": first_line.count(","),
            "|": first_line.count("|"),
            ";": first_line.count(";"),
        }
        best = max(counts, key=counts.get)
        return best if counts[best] > 0 else ","
