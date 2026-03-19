from __future__ import annotations

from pathlib import Path

import pandas as pd

from .loader_common import normalize_sheet_index, read_csv_frame, read_excel_frame


def read_raw_frame(path: Path, *, sheet_index: int, nrows: int | None) -> tuple[pd.DataFrame, str]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_frame(path, header_row=None, nrows=nrows), path.stem
    if suffix in {".xlsx", ".xls", ".xlsm", ".xltx", ".xltm"}:
        return read_excel_frame(path, normalize_sheet_index(sheet_index), header_row=None, nrows=nrows)
    raise RuntimeError(f"Unsupported spreadsheet format: {suffix}")


def read_default_frame(path: Path, *, sheet_index: int, nrows: int | None) -> tuple[pd.DataFrame, str]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_frame(path, header_row=0, nrows=nrows), path.stem
    if suffix in {".xlsx", ".xls", ".xlsm", ".xltx", ".xltm"}:
        return read_excel_frame(path, normalize_sheet_index(sheet_index), header_row=0, nrows=nrows)
    raise RuntimeError(f"Unsupported spreadsheet format: {suffix}")
