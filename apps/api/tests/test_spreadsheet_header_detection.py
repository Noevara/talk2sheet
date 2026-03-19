from __future__ import annotations

from pathlib import Path

from app.services.spreadsheet.analysis import analyze
import app.services.spreadsheet.pipeline.header_detection as header_detection
from app.services.spreadsheet.core.schema import HeaderPlan
from app.services.spreadsheet.pipeline.header_detection import detect_header_plan_by_rules, maybe_detect_header_plan, read_preview_table
from app.services.spreadsheet.pipeline import HEADER_PLAN_ATTR, load_dataframe


def test_load_dataframe_reloads_after_title_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "messy_header.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Monthly Sales Report,,",
                "Generated at 2025-03-01,,",
                "Region,Amount,Date",
                "East,100,2025-01-01",
                "West,80,2025-01-02",
            ]
        ),
        encoding="utf-8",
    )

    df, sheet_name = load_dataframe(csv_path, sheet_index=1)

    assert sheet_name == "messy_header"
    assert df.columns.tolist() == ["Region", "Amount", "Date"]
    assert df.iloc[0].to_dict() == {"Region": "East", "Amount": 100, "Date": "2025-01-01"}
    assert df.attrs[HEADER_PLAN_ATTR]["header_row_1based"] == 3
    assert df.attrs[HEADER_PLAN_ATTR]["header_depth"] == 1


def test_load_dataframe_merges_two_row_headers(tmp_path: Path) -> None:
    csv_path = tmp_path / "multi_header.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Monthly Sales,,",
                "Region,Amount,Amount",
                ",Jan,Feb",
                "East,10,12",
                "West,8,9",
            ]
        ),
        encoding="utf-8",
    )

    df, _sheet_name = load_dataframe(csv_path, sheet_index=1)

    assert df.columns.tolist() == ["Region", "Amount/Jan", "Amount/Feb"]
    assert df.attrs[HEADER_PLAN_ATTR]["header_row_1based"] == 2
    assert df.attrs[HEADER_PLAN_ATTR]["header_depth"] == 2
    assert df.iloc[1].to_dict() == {"Region": "West", "Amount/Jan": 8, "Amount/Feb": 9}


def test_analyze_exposes_source_header_metadata(tmp_path: Path) -> None:
    csv_path = tmp_path / "header_analysis.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Billing Export,,",
                "For internal review,,",
                "Category,Amount,Date",
                "Compute,100,2025-01-01",
                "Storage,80,2025-01-02",
            ]
        ),
        encoding="utf-8",
    )

    df, _sheet_name = load_dataframe(csv_path, sheet_index=1)
    result = analyze(
        df,
        chat_text="What is the total amount?",
        requested_mode="text",
        locale="en",
        rows_loaded=len(df),
    )

    assert result.pipeline["source_header_plan"]["header_row_1based"] == 3
    assert result.pipeline["source_header_plan"]["header_depth"] == 1


def test_header_detection_does_not_merge_first_data_row_into_headers_for_export_like_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "billing_export_like.csv"
    csv_path.write_text(
        "\n".join(
            [
                "标识信息/账单明细ID,标识信息/订单号,账单信息/账单月份,账单信息/账单日期,应付信息/应付金额（含税）",
                "202601_abc_0001,D2026050UE680772,202601,20260105,102",
                "202601_abc_0002,D2026050U9X80670,202601,20260116,100",
            ]
        ),
        encoding="utf-8",
    )

    _sheet_name, rows = read_preview_table(csv_path, sheet_index=1, max_rows=5)
    plan = detect_header_plan_by_rules(rows)
    assert plan is not None
    assert plan["header_row_1based"] == 1
    assert plan["header_depth"] == 1

    df, _sheet_name = load_dataframe(csv_path, sheet_index=1)
    assert df.columns.tolist() == [
        "标识信息/账单明细ID",
        "标识信息/订单号",
        "账单信息/账单月份",
        "账单信息/账单日期",
        "应付信息/应付金额（含税）",
    ]
    assert df.iloc[0].to_dict() == {
        "标识信息/账单明细ID": "202601_abc_0001",
        "标识信息/订单号": "D2026050UE680772",
        "账单信息/账单月份": 202601,
        "账单信息/账单日期": 20260105,
        "应付信息/应付金额（含税）": 102,
    }


def test_header_detection_caches_llm_fallback_for_same_file(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "cached_header_detection.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Monthly Sales Report,,",
                "Generated at 2025-03-01,,",
                "Region,Amount,Date",
                "East,100,2025-01-01",
                "West,80,2025-01-02",
            ]
        ),
        encoding="utf-8",
    )

    class _FakeClient:
        enabled = True

        def __init__(self) -> None:
            self.calls = 0

        def generate_json(self, schema_model, *, system_prompt: str, user_prompt: str) -> HeaderPlan:
            self.calls += 1
            return HeaderPlan(
                has_header=True,
                header_row_1based=3,
                header_depth=1,
                data_start_row_1based=4,
                confidence=0.95,
                reason="fake_llm",
        )

    fake_client = _FakeClient()
    header_detection._read_preview_table_cached.cache_clear()
    header_detection._maybe_detect_header_plan_cached.cache_clear()
    monkeypatch.setattr(
        header_detection,
        "detect_header_plan_by_rules",
        lambda rows: {
            "has_header": True,
            "header_row_1based": 3,
            "header_depth": 1,
            "data_start_row_1based": 4,
            "confidence": 0.2,
            "reason": "forced_low_confidence",
        },
    )
    monkeypatch.setattr(header_detection, "build_default_llm_client", lambda: fake_client)

    first = maybe_detect_header_plan(csv_path, sheet_index=1)
    second = maybe_detect_header_plan(csv_path, sheet_index=1)

    assert fake_client.calls == 1
    assert first.header_row_1based == 3
    assert second.header_row_1based == 3
