from __future__ import annotations

import io
from pathlib import Path

import pandas as pd


SUPPORTED_SPREADSHEET_SUFFIXES = {".xlsx", ".xls", ".xlsm", ".xltx", ".xltm", ".csv"}
HEADER_PLAN_ATTR = "_talk2sheet_header_plan"
HEADER_HEALTH_ATTR = "_talk2sheet_header_health"
SHEET_NAME_ATTR = "_talk2sheet_sheet_name"
SOURCE_PATH_ATTR = "_talk2sheet_source_path"
SOURCE_SHEET_INDEX_ATTR = "_talk2sheet_source_sheet_index"


def path_cache_key(path: Path) -> tuple[str, int, int]:
    resolved = path.resolve()
    stat = resolved.stat()
    return (str(resolved), int(stat.st_mtime_ns), int(stat.st_size))


def normalize_sheet_index(sheet_index: int | None, sheet_count: int | None = None) -> int:
    if sheet_index is None:
        idx = 0
    else:
        idx = int(sheet_index)
        if idx >= 1:
            idx -= 1
    if sheet_count is not None:
        idx = max(0, min(idx, sheet_count - 1))
    return idx


def read_csv_frame(path: Path, *, header_row: int | None = 0, nrows: int | None = None) -> pd.DataFrame:
    if header_row is None:
        try:
            return pd.read_csv(path, header=None, nrows=nrows)
        except UnicodeDecodeError:
            return pd.read_csv(io.StringIO(path.read_text(encoding="utf-8", errors="replace")), header=None, nrows=nrows)
    return pd.read_csv(path, header=header_row, nrows=nrows)


def read_excel_frame(path: Path, sheet_index: int, *, header_row: int | None = 0, nrows: int | None = None) -> tuple[pd.DataFrame, str]:
    workbook = pd.ExcelFile(path)
    idx = normalize_sheet_index(sheet_index + 1, len(workbook.sheet_names))
    sheet_name = workbook.sheet_names[idx]
    df = pd.read_excel(workbook, sheet_name=sheet_name, header=header_row, nrows=nrows)
    return df, sheet_name
