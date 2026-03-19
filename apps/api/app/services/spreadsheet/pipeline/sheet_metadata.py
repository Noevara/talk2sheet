from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from app.schemas import SheetDescriptor
from .loader_common import path_cache_key, read_csv_frame


def read_sheet_descriptors(path: Path) -> list[SheetDescriptor]:
    payloads = _read_sheet_descriptors_cached(path_cache_key(path))
    return [SheetDescriptor.model_validate(payload) for payload in payloads]


@lru_cache(maxsize=64)
def _read_sheet_descriptors_cached(cache_key: tuple[str, int, int]) -> tuple[dict[str, Any], ...]:
    path = Path(cache_key[0])
    if path.suffix.lower() == ".csv":
        df = read_csv_frame(path, header_row=0, nrows=10)
        return ({"index": 1, "name": path.stem, "rows": None, "columns": len(df.columns)},)

    workbook = pd.ExcelFile(path)
    sheets: list[dict[str, Any]] = []
    for idx, sheet_name in enumerate(workbook.sheet_names, start=1):
        preview = pd.read_excel(workbook, sheet_name=sheet_name, nrows=10)
        sheets.append({"index": idx, "name": sheet_name, "rows": None, "columns": len(preview.columns)})
    return tuple(sheets)
