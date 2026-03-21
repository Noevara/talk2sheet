from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.spreadsheet.pipeline.sheet_metadata import read_sheet_descriptors


def _write_workbook(path: Path) -> None:
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(
            {
                "Order Date": ["2025-01-01", "2025-01-03", "2025-01-05"],
                "Amount": [120, 80, 160],
                "Region": ["cn-sh", "cn-bj", "cn-sh"],
            }
        ).to_excel(writer, sheet_name="Sales", index=False)
        pd.DataFrame(
            {
                "User ID": [101, 102],
                "Signup Date": ["2025-02-01", "2025-02-05"],
            }
        ).to_excel(writer, sheet_name="Users", index=False)


def test_read_sheet_descriptors_includes_rows_columns_and_field_summary_for_csv(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    pd.DataFrame(
        {
            "Date": ["2025-01-01", "2025-01-02"],
            "Amount": [10, 20],
            "Region": ["cn-sh", "cn-bj"],
        }
    ).to_csv(path, index=False)

    descriptors = read_sheet_descriptors(path)
    assert len(descriptors) == 1
    assert descriptors[0].rows == 2
    assert descriptors[0].columns == 3
    assert descriptors[0].field_summary
    assert descriptors[0].field_summary[0].startswith("Date")


def test_read_sheet_descriptors_includes_rows_columns_and_field_summary_for_workbook(tmp_path: Path) -> None:
    path = tmp_path / "sample.xlsx"
    _write_workbook(path)

    descriptors = read_sheet_descriptors(path)
    assert len(descriptors) == 2
    assert descriptors[0].name == "Sales"
    assert descriptors[0].rows == 3
    assert descriptors[0].columns == 3
    assert descriptors[0].field_summary
    assert descriptors[1].name == "Users"
    assert descriptors[1].rows == 2
    assert descriptors[1].columns == 2
    assert descriptors[1].field_summary
