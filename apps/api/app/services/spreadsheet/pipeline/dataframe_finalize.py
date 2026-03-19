from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .column_profile import attach_column_profiles
from .header_detection import compute_header_health
from .loader_common import (
    HEADER_HEALTH_ATTR,
    HEADER_PLAN_ATTR,
    SHEET_NAME_ATTR,
    SOURCE_PATH_ATTR,
    SOURCE_SHEET_INDEX_ATTR,
)


def finalize_loaded_dataframe(
    df: pd.DataFrame,
    *,
    sheet_name: str,
    header_plan: dict[str, Any],
    source_path: Path,
    source_sheet_index: int,
) -> pd.DataFrame:
    finalized = attach_column_profiles(df)
    finalized.attrs[SHEET_NAME_ATTR] = sheet_name
    finalized.attrs[HEADER_PLAN_ATTR] = header_plan
    finalized.attrs[HEADER_HEALTH_ATTR] = compute_header_health([str(column) for column in finalized.columns.tolist()])
    finalized.attrs[SOURCE_PATH_ATTR] = str(source_path)
    finalized.attrs[SOURCE_SHEET_INDEX_ATTR] = int(source_sheet_index or 1)
    return finalized
