from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.config import get_settings
from app.services.spreadsheet.analysis import analyze
from app.services.spreadsheet.conversation.context_interpreter import FollowupInterpretation, InterpretationResult
from app.services.spreadsheet.core.schema import ChartSpec, DerivedColumn, Metric, SelectionPlan, Sort, TransformPlan
from app.services.spreadsheet.openai_compatible import OpenAICompatibleError
from app.services.spreadsheet.pipeline import load_dataframe, preview_sheet
from app.services.spreadsheet.pipeline.column_profile import attach_column_profiles, get_column_profiles
from app.services.spreadsheet.planning.planner import HeuristicPlanner, OpenAIJsonPlanner, get_default_planner
from app.services.spreadsheet.quality.repair import repair_selection_plan


def _sample_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-10", "2025-02-03", "2025-02-15"],
                "Category": ["A", "B", "A", "C"],
                "Amount": [100, 50, 80, 20],
            }
        )
    )


def _transaction_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Transaction ID": ["T-001", "T-002", "T-003", "T-004"],
                "Item": ["Compute", "Storage", "Compute", "AI"],
                "Category": ["A", "B", "A", "C"],
                "Amount": [100, 50, 180, 20],
            }
        )
    )


def _billing_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Date": ["2025-01-03", "2025-01-04", "2025-01-06", "2025-01-11", "2025-02-03"],
                "Service Name": ["Compute", "Storage", "Compute", "AI", "Compute"],
                "Billing Item Name": ["Instance", "Disk", "Instance", "Token", "Bandwidth"],
                "Region": ["cn-sh", "cn-bj", "cn-sh", "us-west", "cn-bj"],
                "Transaction ID": ["T-101", "T-102", "T-103", "T-104", "T-105"],
                "Amount": [120, 80, 60, 200, 40],
            }
        )
    )


def _monthly_region_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Region": ["West", "West", "East", "East"],
                "Month": ["2025-01", "2025-02", "2025-01", "2025-02"],
                "Amount": [40, 60, 30, 35],
            }
        )
    )


def _compact_month_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "账单月份": ["202501", "202501", "202502"],
                "应付金额": [300, 160, 40],
            }
        )
    )


def _march_days_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "账单日期": ["2026-03-01", "2026-03-01", "2026-03-05", "2026-03-09", "2026-02-20"],
                "应付金额": [10, 20, 30, 40, 50],
            }
        )
    )


def _compact_march_days_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "账单日期": ["20260301", "20260301", "20260305", "20260309", "20260220"],
                "应付金额": [10, 20, 30, 40, 50],
            }
        )
    )


def _billing_month_and_date_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "账单月份": ["202603", "202603", "202603", "202603", "202602"],
                "账单日期": ["20260301", "20260301", "20260305", "20260309", "20260220"],
                "应付金额": [10, 20, 30, 40, 50],
            }
        )
    )


def _monthly_forecast_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "账单月份": ["2026-01", "2026-02", "2026-03"],
                "应付金额": [100.0, 120.0, 140.0],
            }
        )
    )


def test_analyze_builds_ranking_chart_pipeline() -> None:
    result = analyze(
        _sample_df(),
        chat_text="Show the top 2 categories by amount as a bar chart.",
        requested_mode="chart",
        locale="en",
        rows_loaded=4,
    )

    assert result.mode == "chart"
    assert result.chart_spec is not None
    assert result.chart_spec["type"] == "bar"
    assert len(result.chart_data or []) == 2
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["analysis_intent"]["kind"] == "ranking"
    assert result.pipeline["planner"]["analysis_intent"]["answer_expectation"] == "chart"
    assert result.pipeline["planner"]["chart_context"]["x_label"] == "Category"
    assert result.pipeline["chart_context"]["x_label"] == "Category"
    assert result.pipeline["chart_context"]["y_label"] == "value"
    assert result.pipeline["chart_context"]["y_unit"] == result.pipeline["planner"]["chart_context"]["y_unit"]
    assert result.pipeline["chart_context"]["point_count"] == 2
    assert result.pipeline["answer_generation"]["chart_context"]["x_label"] == "Category"
    assert result.pipeline["exact_execution"]["eligible"] is True
    assert result.execution_disclosure.exact_used is True


def test_analyze_returns_intent_level_clarification_before_selection_stage() -> None:
    result = analyze(
        _billing_df(),
        chat_text="按名称统计费用排行",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["status"] == "clarification"
    assert result.pipeline["clarification_stage"] == "intent"
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["analysis_intent"]["kind"] == "ranking"
    assert result.pipeline["clarification"]["field"] == "名称"
    assert "selection_validation" not in result.pipeline


def test_analyze_blocks_advanced_statistics_questions() -> None:
    result = analyze(
        _sample_df(),
        chat_text="Run a regression analysis on this workbook.",
        requested_mode="auto",
        locale="en",
        rows_loaded=4,
    )

    assert result.mode == "text"
    assert "not supported" in result.answer.lower()
    assert result.pipeline["status"] == "blocked"
    assert result.pipeline["reason_code"] == "unsupported_capability"


def test_analyze_blocks_cross_sheet_questions_with_workbook_routing_boundary_copy() -> None:
    result = analyze(
        _sample_df(),
        chat_text="Join sheet 1 and sheet 2, then compare totals.",
        requested_mode="auto",
        locale="en",
        rows_loaded=4,
    )

    assert result.mode == "text"
    assert "routing to one sheet is supported" in result.answer.lower()
    assert "cross-sheet analysis is not supported yet" in result.answer.lower()
    assert result.pipeline["status"] == "blocked"
    assert result.pipeline["reason_code"] == "unsupported_capability"


def test_repair_selection_plan_resolves_approximate_column_name() -> None:
    repaired, meta = repair_selection_plan(
        _sample_df(),
        SelectionPlan(columns=["Ammount"]),
        question="Total amount",
        mode="text",
    )

    assert repaired.columns == ["Amount"]
    assert meta["changes"]


def test_analyze_detail_rows_includes_preview_rows() -> None:
    result = analyze(
        _sample_df(),
        chat_text="List the top 2 detail rows by amount.",
        requested_mode="text",
        locale="en",
        rows_loaded=4,
    )

    assert result.pipeline["planner"]["intent"] == "detail_rows"
    assert len(result.pipeline["preview_rows"]) == 2
    answer_meta = result.pipeline["answer_generation"]
    assert answer_meta["summary_kind"] == "detail"
    assert answer_meta["summary_source"] == "rule_based_detail_summary"
    assert answer_meta["detail_source"] == "result_df_rows"
    assert answer_meta["detail_row_count"] == 2
    assert answer_meta["segments"]["conclusion"] == result.answer


def test_analyze_single_transaction_top_n_returns_detail_rows() -> None:
    result = analyze(
        _transaction_df(),
        chat_text="最多2次单笔消费是什么",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "detail_rows"
    assert len(result.pipeline["preview_rows"]) == 2
    assert result.pipeline["preview_rows"][0][0] == "T-003"


def test_analyze_single_transaction_top_n_supports_chart() -> None:
    result = analyze(
        _transaction_df(),
        chat_text="最多2次单笔消费是什么，用图表展示",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
    )

    assert result.mode == "chart"
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.chart_spec is not None
    assert result.chart_spec["type"] == "bar"
    assert result.chart_spec["x"] == "Transaction ID"
    assert len(result.chart_data or []) == 2


def test_analyze_downgrades_to_text_when_scatter_chart_is_not_renderable() -> None:
    result = analyze(
        _sample_df(),
        chat_text="Show the top 2 categories by amount as a scatter chart.",
        requested_mode="chart",
        locale="en",
        rows_loaded=4,
    )

    assert result.mode == "chart"
    assert result.chart_spec is None
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["chart_context"]["requested"] is True
    assert result.pipeline["chart_context"]["rendered"] is False
    assert result.pipeline["chart_context"]["requested_type"] == "scatter"
    assert result.pipeline["chart_context"]["applied_type"] == "scatter"
    assert "scatter x-axis should be numeric" in result.pipeline["chart_context"]["fallback_reason"].lower()
    assert "chart output was downgraded to text" in result.execution_disclosure.fallback_reason.lower()
    assert "chart output was downgraded to text" in result.pipeline["answer_generation"]["segments"]["risk_note"].lower()


def test_service_ranking_prefers_service_dimension_from_question() -> None:
    result = analyze(
        _billing_df(),
        chat_text="按服务统计费用最高的前2项",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["ranking_column"] == "Service Name"
    assert result.pipeline["result_columns"] == ["Service Name", "value"]
    assert "Compute" in result.answer


def test_billing_item_ranking_prefers_billing_item_dimension_from_question() -> None:
    result = analyze(
        _billing_df(),
        chat_text="按计费项统计费用排行",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["ranking_column"] == "Billing Item Name"
    assert result.pipeline["result_columns"] == ["Billing Item Name", "value"]
    assert "Token" in result.answer


def test_region_ranking_chart_prefers_region_dimension_from_question() -> None:
    result = analyze(
        _billing_df(),
        chat_text="按地域展示费用排行",
        requested_mode="chart",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "chart"
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["ranking_column"] == "Region"
    assert result.chart_spec is not None
    assert result.chart_spec["x"] == "Region"
    assert len(result.chart_data or []) == 3


def test_weekpart_compare_question_is_supported() -> None:
    result = analyze(
        _billing_df(),
        chat_text="工作日和周末哪个消费更高",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "weekpart_compare"
    assert result.pipeline["result_columns"] == ["weekpart", "value"]
    assert "周末" in result.answer
    assert "280" in result.answer


def test_share_question_can_return_text_summary_without_forcing_chart() -> None:
    result = analyze(
        _billing_df(),
        chat_text="各服务费用Top3占比是什么",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "share"
    assert result.chart_spec is None
    assert result.pipeline["result_columns"] == ["Service Name", "value"]
    assert "%" in result.answer


def test_multi_month_total_question_returns_monthly_breakdown() -> None:
    result = analyze(
        _billing_df(),
        chat_text="请你看一下一月份，二月份总消费额是多少呢",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "period_breakdown"
    assert result.pipeline["planner"]["requested_months"] == ["2025-01", "2025-02"]
    assert result.pipeline["result_columns"] == ["month_bucket", "value"]
    assert "2025-01为 460" in result.answer
    assert "2025-02为 40" in result.answer


def test_multi_month_total_question_supports_compact_month_values() -> None:
    result = analyze(
        _compact_month_df(),
        chat_text="请你看一下一月份，二月份总消费额是多少呢",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=3,
    )

    assert result.pipeline["planner"]["intent"] == "period_breakdown"
    assert result.pipeline["planner"]["requested_months"] == ["2025-01", "2025-02"]
    assert result.pipeline["result_columns"] == ["month_bucket", "value"]
    assert "2025-01为 460" in result.answer
    assert "2025-02为 40" in result.answer


def test_month_day_count_question_counts_distinct_days_with_data() -> None:
    result = analyze(
        _march_days_df(),
        chat_text="三月份的数据有多少天啊",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
        followup_context={
            "conversation_id": "conv-day-count",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "period_breakdown",
                "mode": "text",
                "groupby": ["month_bucket"],
                "metric_aliases": ["value"],
                "result_columns": ["month_bucket", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "请你看一下一月份，二月份，三月份总消费额是多少呢",
                "intent": "period_breakdown",
            },
        },
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "active_day_count"
    assert result.pipeline["planner"]["analysis_intent"]["target_metric"] == "count_distinct_day"
    assert result.pipeline["planner"]["analysis_intent"]["time_scope"]["requested_period"] == "2026-03"
    assert result.pipeline["result_columns"] == ["active_day_count"]
    assert "2026-03 共有 3 天有数据" in result.answer


def test_month_day_count_question_counts_distinct_days_with_compact_dates() -> None:
    result = analyze(
        _compact_march_days_df(),
        chat_text="目前3月份有几天是有数据的呢",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "active_day_count"
    assert result.pipeline["result_columns"] == ["active_day_count"]
    assert result.pipeline["selection_plan"]["filters"] == [
        {"col": "账单日期", "op": ">=", "value": "2026-03-01"},
        {"col": "账单日期", "op": "<", "value": "2026-04-01"},
    ]
    assert "2026-03 共有 3 天有数据" in result.answer


def test_direct_trend_question_can_apply_day_grain_with_last_month_filter() -> None:
    result = analyze(
        _sample_df(),
        chat_text="按天看上月趋势",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
    )

    assert result.pipeline["planner"]["intent"] == "trend"
    assert result.pipeline["planner"]["bucket_grain"] == "day"
    assert result.pipeline["planner"]["requested_period"] == "2025-01"
    assert result.pipeline["selection_plan"]["filters"] == [
        {"col": "Date", "op": ">=", "value": "2025-01-01"},
        {"col": "Date", "op": "<", "value": "2025-02-01"},
    ]
    assert result.pipeline["result_columns"] == ["day_bucket", "value"]


def test_direct_trend_question_can_apply_recent_month_window() -> None:
    result = analyze(
        _sample_df(),
        chat_text="看最近2个月趋势",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
    )

    assert result.pipeline["planner"]["intent"] == "trend"
    assert result.pipeline["planner"]["requested_recent_period_count"] == 2
    assert result.pipeline["planner"]["requested_recent_periods"] == ["2025-01", "2025-02"]
    assert result.pipeline["transform_plan"]["having"] == [{"col": "month_bucket", "op": "in", "value": ["2025-01", "2025-02"]}]
    assert result.pipeline["result_columns"] == ["month_bucket", "value"]


def test_single_month_total_amount_question_uses_month_range_for_compact_dates() -> None:
    result = analyze(
        _compact_march_days_df(),
        chat_text="3月份总消费额是多少？",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "total_amount"
    assert result.pipeline["selection_plan"]["filters"] == [
        {"col": "账单日期", "op": ">=", "value": "2026-03-01"},
        {"col": "账单日期", "op": "<", "value": "2026-04-01"},
    ]
    assert "总计为 100" in result.answer


def test_month_day_count_prefers_daily_date_column_over_month_column() -> None:
    result = analyze(
        _billing_month_and_date_df(),
        chat_text="目前3月份有几天是有数据的呢",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "active_day_count"
    assert result.pipeline["selection_plan"]["columns"] == ["账单日期"]
    assert result.pipeline["selection_plan"]["filters"] == [
        {"col": "账单日期", "op": ">=", "value": "2026-03-01"},
        {"col": "账单日期", "op": "<", "value": "2026-04-01"},
    ]
    assert "2026-03 共有 3 天有数据" in result.answer


def test_forecast_question_is_unsupported_when_history_is_too_short() -> None:
    result = analyze(
        _billing_df(),
        chat_text="你能预测一下4月份的总费用是多少吗",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "unsupported"


def test_forecast_question_predicts_next_month_total_amount() -> None:
    result = analyze(
        _monthly_forecast_df(),
        chat_text="你能预测一下4月份的总费用是多少吗",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=3,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "forecast_timeseries"
    assert result.pipeline["result_columns"] == ["month_bucket", "forecast_value", "lower_bound", "upper_bound", "model"]
    assert result.pipeline["transform_meta"]["forecast"]["target_period"] == "2026-04"
    assert result.pipeline["transform_meta"]["forecast"]["model"] == "linear_regression"
    assert abs(result.pipeline["transform_meta"]["forecast"]["forecast_value"] - 160.0) < 1e-6
    assert "预测 2026-04 的总费用约为 160" in result.answer


def test_forecast_question_predicts_remaining_daily_values_in_month() -> None:
    result = analyze(
        _billing_month_and_date_df(),
        chat_text="你能预测3月份剩余天每天的费用吗",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "forecast_timeseries"
    assert result.pipeline["planner"]["forecast_grain"] == "day"
    assert result.pipeline["planner"]["forecast_target_start_period"] == "2026-03-10"
    assert result.pipeline["planner"]["forecast_target_end_period"] == "2026-03-31"
    assert result.pipeline["planner"]["forecast_target_count"] == 22
    assert result.pipeline["result_columns"] == ["day_bucket", "forecast_value", "lower_bound", "upper_bound", "model"]
    assert result.pipeline["result_row_count"] == 22
    assert result.pipeline["transform_meta"]["forecast"]["multi_step"] is True
    assert result.pipeline["transform_meta"]["forecast"]["target_count"] == 22
    assert "2026-03-10 到 2026-03-31" in result.answer


def test_date_breakdown_fast_path_can_explain_specific_day() -> None:
    result = analyze(
        _billing_df(),
        chat_text="2025-01-04 这天主要是哪项费用？",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["fast_path"] == "date_breakdown"
    assert result.pipeline["planner"]["date_filter_value"] == "2025-01-04"
    assert result.pipeline["result_columns"] == ["Billing Item Name", "value"]
    assert "Disk" in result.answer


def test_what_if_reduction_fast_path_is_supported() -> None:
    result = analyze(
        _billing_df(),
        chat_text="如果把Instance这一计费项费用降低20%，总金额会降低多少？",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "what_if_reduction"
    assert result.pipeline["planner"]["analysis_intent"]["comparison_type"] == "simulation"
    assert result.pipeline["planner"]["analysis_intent"]["target_dimension"] == "Billing Item Name"
    assert result.pipeline["planner"]["what_if_target_value"] == "Instance"
    assert result.pipeline["result_columns"] == ["matched_amount", "reduction_amount"]
    assert "36" in result.answer


def test_followup_can_switch_detail_rows_to_item_ranking_summary() -> None:
    result = analyze(
        _transaction_df(),
        chat_text="不要返回表格的2行，应该是返回2个消费最高项和对应的费用",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "last_turn": {
                "question": "最多2次单笔消费是什么",
                "intent": "detail_rows",
            },
        },
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["ranking_column"] == "Item"
    assert result.pipeline["result_columns"] == ["Item", "value"]
    assert len(result.pipeline.get("preview_rows") or []) == 3


def test_followup_can_explain_top_ranked_item_with_detail_rows() -> None:
    result = analyze(
        _transaction_df(),
        chat_text="上面排第一个的 Compute 是什么东西啊，能结合表格解释一下吗",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "last_turn": {
                "question": "不要返回表格的2行，应该是返回2个消费最高项和对应的费用",
                "intent": "ranking",
                "selection_plan": {"columns": ["Item", "Amount"], "filters": [], "distinct_by": None, "sort": None, "limit": None},
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Item"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 2,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "explain_ranked_item"
    assert result.pipeline["planner"]["explain_target_value"] == "Compute"
    assert result.pipeline["result_columns"][0] == "Item"
    assert len(result.pipeline.get("preview_rows") or []) == 2


def test_followup_can_explain_explicit_ranked_target_from_previous_ranking() -> None:
    result = analyze(
        _billing_df(),
        chat_text="为什么 Instance 消费这么高呢，能分析一下吗",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Billing Item Name"],
                "metric_aliases": ["value"],
                "result_columns": ["Billing Item Name", "value"],
                "result_row_count": 4,
            },
            "last_turn": {
                "question": "不要返回表格的5行，应该是返回5个消费最高项和对应的费用",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Billing Item Name", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Billing Item Name"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "explain_ranked_item"
    assert result.pipeline["planner"]["explain_target_value"] == "Instance"
    assert result.pipeline["result_columns"] == ["Billing Item Name", "Date", "Amount"]
    assert "2 条相关明细" in result.answer


def test_followup_can_lookup_second_ranked_item_from_previous_ranking() -> None:
    result = analyze(
        _billing_df(),
        chat_text="那第二名呢",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Service Name"],
                "metric_aliases": ["value"],
                "result_columns": ["Service Name", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "Show the top 5 services by amount.",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Service Name", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Service Name"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "ranked_item_lookup"
    assert result.pipeline["planner"]["analysis_intent"]["comparison_type"] == "rank_lookup"
    assert result.pipeline["planner"]["analysis_intent"]["target_dimension"] == "Service Name"
    assert result.pipeline["planner"]["rank_position"] == 2
    assert result.pipeline["result_columns"] == ["Service Name", "value"]
    assert "AI" in result.answer
    assert "200" in result.answer


def test_followup_can_lookup_last_ranked_item_from_previous_ranking() -> None:
    result = analyze(
        _billing_df(),
        chat_text="最后一名呢",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Service Name"],
                "metric_aliases": ["value"],
                "result_columns": ["Service Name", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "Show the top 5 services by amount.",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Service Name", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Service Name"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "ranked_item_lookup"
    assert result.pipeline["planner"]["rank_position"] == -1
    assert "Storage" in result.answer
    assert "80" in result.answer


def test_direct_question_can_compare_latest_month_with_previous_month() -> None:
    result = analyze(
        _monthly_region_df(),
        chat_text="对比上个月总金额变化",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "period_compare"
    assert result.pipeline["planner"]["compare_basis"] == "previous_period"
    assert result.pipeline["planner"]["comparison_type"] == "delta"
    assert result.pipeline["planner"]["current_period"] == "2025-02"
    assert result.pipeline["planner"]["previous_period"] == "2025-01"
    assert result.pipeline["planner"]["analysis_intent"]["comparison_type"] == "delta"
    assert result.pipeline["planner"]["analysis_intent"]["time_scope"]["base_period"] == "2025-01"
    assert result.pipeline["result_columns"] == ["2025-01", "2025-02", "change_value", "change_pct"]
    assert "2025-02" in result.answer
    assert "25" in result.answer


def test_direct_question_can_compare_year_over_year_ratio() -> None:
    df = attach_column_profiles(
        pd.DataFrame(
            {
                "Region": ["West", "West", "West"],
                "Month": ["2024-02", "2025-01", "2025-02"],
                "Amount": [45, 40, 60],
            }
        )
    )
    result = analyze(
        df,
        chat_text="同比占比是多少",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=3,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "period_compare"
    assert result.pipeline["planner"]["compare_basis"] == "year_over_year"
    assert result.pipeline["planner"]["comparison_type"] == "ratio"
    assert result.pipeline["planner"]["current_period"] == "2025-02"
    assert result.pipeline["planner"]["previous_period"] == "2024-02"
    assert result.pipeline["planner"]["analysis_intent"]["comparison_type"] == "ratio"
    assert result.pipeline["result_columns"] == ["2024-02", "2025-02", "change_value", "change_pct", "compare_ratio"]
    assert "1.333x" in result.answer


def test_direct_question_can_apply_value_filter_with_groupby_topk() -> None:
    result = analyze(
        _billing_df(),
        chat_text="只看 cn-sh，按服务统计费用前2",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
    )

    assert result.mode == "text"
    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["top_k"] == 2
    assert result.pipeline["planner"]["value_filters"] == [{"column": "Region", "value": "cn-sh"}]
    assert result.pipeline["selection_plan"]["filters"] == [{"col": "Region", "op": "=", "value": "cn-sh"}]
    assert result.pipeline["result_columns"] == ["Service Name", "value"]
    assert result.pipeline["result_row_count"] == 1
    assert "Compute" in result.answer


def test_followup_can_compare_current_month_with_previous_month() -> None:
    result = analyze(
        _monthly_region_df(),
        chat_text="和上个月对比一下",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
        followup_context={
            "conversation_id": "conv-compare-1",
            "turn_count": 2,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "share",
                "mode": "text",
                "result_columns": ["Region", "value"],
                "result_row_count": 1,
            },
            "last_turn": {
                "question": "只看 West 的 2025-02",
                "intent": "share",
                "selection_plan": {
                    "columns": ["Region", "Amount"],
                    "filters": [
                        {"col": "Region", "op": "=", "value": "West"},
                        {"col": "Month", "op": "contains", "value": "2025-02"},
                    ],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Region"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "period_compare"
    assert result.pipeline["planner"]["current_period"] == "2025-02"
    assert result.pipeline["planner"]["previous_period"] == "2025-01"
    assert result.pipeline["result_columns"] == ["2025-01", "2025-02", "change_value", "change_pct"]
    assert "2025-02" in result.answer
    assert "50.0%" in result.answer
    assert "2025-01=40" in result.analysis_text


def test_followup_can_compare_current_month_year_over_year() -> None:
    df = attach_column_profiles(
        pd.DataFrame(
            {
                "Region": ["West", "West", "West"],
                "Month": ["2024-02", "2025-01", "2025-02"],
                "Amount": [45, 40, 60],
            }
        )
    )
    result = analyze(
        df,
        chat_text="同比呢",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=3,
        followup_context={
            "conversation_id": "conv-compare-2",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {"intent": "ranking"},
            "last_turn": {
                "question": "只看 West 的 2025-02",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Region", "Amount"],
                    "filters": [
                        {"col": "Region", "op": "=", "value": "West"},
                        {"col": "Month", "op": "contains", "value": "2025-02"},
                    ],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Region"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "period_compare"
    assert result.pipeline["planner"]["compare_basis"] == "year_over_year"
    assert result.pipeline["planner"]["previous_period"] == "2024-02"
    assert "33.3%" in result.answer


def test_followup_can_break_down_ranked_item_by_region() -> None:
    result = analyze(
        _billing_df(),
        chat_text="为什么这个高，按地区拆解一下",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
        followup_context={
            "conversation_id": "conv-breakdown-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Billing Item Name"],
                "metric_aliases": ["value"],
                "result_columns": ["Billing Item Name", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "不要返回表格的5行，应该是返回5个消费最高项和对应的费用",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Billing Item Name", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Billing Item Name"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "explain_breakdown"
    assert result.pipeline["planner"]["analysis_intent"]["target_dimension"] == "Region"
    assert result.pipeline["planner"]["analysis_intent"]["target_metric"] == "Amount"
    assert result.pipeline["planner"]["breakdown_target_value"] == "Token"
    assert result.pipeline["planner"]["breakdown_dimension"] == "Region"
    assert result.pipeline["result_columns"] == ["Region", "value"]
    assert "Region" in result.answer
    assert "us-west" in result.answer


def test_followup_can_switch_previous_ranking_to_region_dimension() -> None:
    result = analyze(
        _billing_df(),
        chat_text="换成按地区看",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Service Name"],
                "metric_aliases": ["value"],
                "result_columns": ["Service Name", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "Show the top 5 services by amount.",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Service Name", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Service Name"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "ranking"
    assert result.pipeline["planner"]["switch_dimension_to"] == "Region"
    assert result.pipeline["result_columns"] == ["Region", "value"]
    assert "us-west" in result.answer
    assert len(result.pipeline.get("preview_rows") or []) == 3


def test_followup_can_switch_previous_ranking_to_share_summary() -> None:
    result = analyze(
        _billing_df(),
        chat_text="这个改成占比",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=5,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Region"],
                "metric_aliases": ["value"],
                "result_columns": ["Region", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "按地域展示费用排行",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Region", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Region"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "share"
    assert result.pipeline["result_columns"] == ["Region", "value"]
    assert "%" in result.answer


def test_followup_can_apply_month_filter_to_previous_share() -> None:
    result = analyze(
        _monthly_region_df(),
        chat_text="只看 2025-02",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 2,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "share",
                "mode": "text",
                "groupby": ["Region"],
                "metric_aliases": ["value"],
                "result_columns": ["Region", "value"],
                "result_row_count": 2,
            },
            "last_turn": {
                "question": "这个改成占比",
                "intent": "share",
                "selection_plan": {
                    "columns": ["Region", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Region"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "share"
    assert result.pipeline["planner"]["date_filter_value"] == "2025-02"
    assert result.pipeline["result_columns"] == ["Region", "value"]
    assert "63.2%" in result.answer


def test_followup_can_apply_region_and_month_filters_together() -> None:
    result = analyze(
        _monthly_region_df(),
        chat_text="只看 West 的 2025-02",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 2,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "share",
                "mode": "text",
                "groupby": ["Region"],
                "metric_aliases": ["value"],
                "result_columns": ["Region", "value"],
                "result_row_count": 2,
            },
            "last_turn": {
                "question": "这个改成占比",
                "intent": "share",
                "selection_plan": {
                    "columns": ["Region", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Region"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "share"
    assert result.pipeline["planner"]["value_filters"] == [{"column": "Region", "value": "West"}]
    assert result.pipeline["planner"]["date_filter_value"] == "2025-02"
    assert "100.0%" in result.answer


def test_followup_can_switch_filtered_context_to_trend() -> None:
    result = analyze(
        _monthly_region_df(),
        chat_text="改成趋势",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 2,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Region"],
                "metric_aliases": ["value"],
                "result_columns": ["Region", "value"],
                "result_row_count": 1,
            },
            "last_turn": {
                "question": "只看 West 的 2025-02",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Region", "Amount"],
                    "filters": [
                        {"col": "Region", "op": "=", "value": "West"},
                        {"col": "Month", "op": "contains", "value": "2025-02"}
                    ],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Region"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5
                }
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "trend"
    assert result.pipeline["result_columns"] == ["month_bucket", "value"]
    assert "2025-02" in result.answer
    assert "60" in result.answer


def test_followup_can_switch_trend_to_detail_rows() -> None:
    result = analyze(
        _monthly_region_df(),
        chat_text="改成明细",
        requested_mode="auto",
        locale="zh-CN",
        rows_loaded=4,
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 3,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "trend",
                "mode": "text",
                "groupby": ["month_bucket"],
                "metric_aliases": ["value"],
                "result_columns": ["month_bucket", "value"],
                "result_row_count": 2,
            },
            "last_turn": {
                "question": "改成趋势",
                "intent": "trend",
                "selection_plan": {
                    "columns": ["Month", "Amount"],
                    "filters": [
                        {"col": "Region", "op": "=", "value": "West"},
                        {"col": "Month", "op": "contains", "value": "2025-02"},
                    ],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None
                },
                "transform_plan": {
                    "derived_columns": [{"as_name": "month_bucket", "kind": "date_bucket", "source_col": "Month", "grain": "month"}],
                    "groupby": ["month_bucket"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "month_bucket", "dir": "asc"},
                    "top_k": 24
                }
            },
        },
    )

    assert result.pipeline["planner"]["intent"] == "detail_rows"
    assert result.pipeline["result_columns"] == ["Month", "Amount"]
    assert result.pipeline["preview_rows"] == [["2025-02", 60]]


def test_default_planner_falls_back_to_heuristic_without_llm_config(monkeypatch) -> None:
    monkeypatch.setenv("TALK2SHEET_PLANNER_PROVIDER", "openai")
    monkeypatch.delenv("TALK2SHEET_LLM_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    get_settings.cache_clear()

    planner = get_default_planner()

    assert planner.name == "heuristic-v1"
    get_settings.cache_clear()


def test_short_total_amount_question_does_not_reuse_previous_row_count_intent() -> None:
    draft = HeuristicPlanner().plan(
        _sample_df(),
        chat_text="统计费用总金额。",
        requested_mode="auto",
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "last_turn": {
                "question": "当前工作表有多少行？",
                "intent": "row_count",
            },
        },
    )

    assert draft.intent == "total_amount"
    assert draft.transform_plan.metrics[0].agg == "sum"
    assert draft.transform_plan.metrics[0].col == "Amount"


def test_short_followup_can_switch_previous_ranking_to_chart_mode() -> None:
    draft = HeuristicPlanner().plan(
        _sample_df(),
        chat_text="改成图表",
        requested_mode="auto",
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Category"],
                "metric_aliases": ["value"],
                "result_columns": ["Category", "value"],
                "result_row_count": 2,
            },
            "last_turn": {
                "question": "Show the top 2 categories by amount.",
                "intent": "ranking",
            },
        },
    )

    assert draft.mode == "chart"
    assert draft.intent == "ranking"
    assert draft.chart_spec is not None
    assert draft.chart_spec.type == "bar"


def test_short_followup_can_switch_previous_trend_to_text_mode() -> None:
    draft = HeuristicPlanner().plan(
        _sample_df(),
        chat_text="改成文字说明",
        requested_mode="auto",
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "chart",
            "last_pipeline_summary": {
                "intent": "trend",
                "mode": "chart",
                "groupby": ["month_bucket"],
                "metric_aliases": ["value"],
                "chart_type": "line",
                "result_columns": ["month_bucket", "value"],
                "result_row_count": 2,
            },
            "last_turn": {
                "question": "Show the monthly trend of amount as a line chart.",
                "intent": "trend",
            },
        },
    )

    assert draft.mode == "text"
    assert draft.intent == "trend"
    assert draft.transform_plan.groupby == ["month_bucket"]


def test_short_followup_can_reuse_previous_ranking_and_only_adjust_top_k() -> None:
    draft = HeuristicPlanner().plan(
        _sample_df(),
        chat_text="只看前2个",
        requested_mode="auto",
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Category"],
                "metric_aliases": ["value"],
                "result_columns": ["Category", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "Show the top 5 categories by amount.",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Category", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Category"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 5,
                },
            },
        },
    )

    assert draft.mode == "text"
    assert draft.intent == "ranking"
    assert draft.transform_plan.groupby == ["Category"]
    assert draft.transform_plan.top_k == 2
    assert draft.planner_meta["followup_reused_previous_plan"] is True


def test_amount_column_prefers_numeric_total_over_fee_type_label() -> None:
    df = attach_column_profiles(
        pd.DataFrame(
            {
                "账单信息/费用类型": ["餐饮", "酒店", "交通"],
                "应付信息/应付金额（含税）": [120.5, 340.0, 89.3],
                "账单信息/账单日期": ["2025-01-01", "2025-01-02", "2025-01-03"],
            }
        )
    )

    profiles = get_column_profiles(df)
    draft = HeuristicPlanner().plan(
        df,
        chat_text="统计费用总金额。",
        requested_mode="auto",
    )

    assert profiles["账单信息/费用类型"]["semantic_type"] != "numeric"
    assert profiles["应付信息/应付金额（含税）"]["semantic_type"] == "numeric"
    assert draft.intent == "total_amount"
    assert draft.planner_meta["amount_column"] == "应付信息/应付金额（含税）"
    assert draft.transform_plan.metrics[0].col == "应付信息/应付金额（含税）"


def test_amount_column_prefers_payable_amount_over_catalog_total() -> None:
    df = attach_column_profiles(
        pd.DataFrame(
            {
                "费用信息/目录总价": [120.5, 340.0, 89.3],
                "优惠信息/优惠后金额": [110.5, 320.0, 79.3],
                "应付信息/应付金额（含税）": [110.5, 320.0, 79.3],
            }
        )
    )

    draft = HeuristicPlanner().plan(
        df,
        chat_text="统计费用总金额。",
        requested_mode="auto",
    )


def test_analyze_uses_full_source_file_for_exact_execution(tmp_path: Path) -> None:
    csv_path = tmp_path / "exact_full_source.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Category,Amount,Date",
                "A,10,2025-01-01",
                "B,20,2025-01-02",
                "C,30,2025-01-03",
                "D,40,2025-01-04",
                "E,50,2025-01-05",
            ]
        ),
        encoding="utf-8",
    )

    sampled_df, _sheet_name = load_dataframe(csv_path, sheet_index=1, limit=2)
    result = analyze(
        sampled_df,
        chat_text="What is the total amount?",
        requested_mode="text",
        locale="en",
        rows_loaded=2,
        source_path=csv_path,
        source_sheet_index=1,
    )

    assert result.execution_disclosure.exact_used is True
    assert result.execution_disclosure.data_scope == "exact_full_table"
    assert result.answer == "The total Amount is 150."


def test_analyze_full_source_exact_execution_respects_detected_header_plan(tmp_path: Path) -> None:
    csv_path = tmp_path / "exact_header_source.csv"
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

    sampled_df, _sheet_name = load_dataframe(csv_path, sheet_index=1, limit=1)
    result = analyze(
        sampled_df,
        chat_text="What is the total amount?",
        requested_mode="text",
        locale="en",
        rows_loaded=1,
        source_path=csv_path,
        source_sheet_index=1,
    )

    assert result.execution_disclosure.exact_used is True
    assert result.pipeline["source_header_plan"]["header_row_1based"] == 3
    assert result.answer == "The total Amount is 180."


def test_preview_sheet_returns_total_rows_and_preview_row_count(tmp_path: Path) -> None:
    csv_path = tmp_path / "preview_counts.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Category,Amount,Date",
                "A,10,2025-01-01",
                "B,20,2025-01-02",
                "C,30,2025-01-03",
                "D,40,2025-01-04",
                "E,50,2025-01-05",
            ]
        ),
        encoding="utf-8",
    )

    response = preview_sheet(csv_path, file_id="test-preview", sheet_index=1)

    assert response.total_rows == 5
    assert response.preview_row_count == 5
    assert len(response.rows) == 5


def test_preview_sheet_counts_rows_without_full_reload_for_shifted_headers(tmp_path: Path, monkeypatch) -> None:
    import app.services.spreadsheet.pipeline as pipeline_module

    csv_path = tmp_path / "preview_shifted_headers.csv"
    csv_path.write_text(
        "\n".join(
            [
                "Monthly Sales Report,,",
                "Generated at 2025-03-01,,",
                "Category,Amount,Date",
                "A,10,2025-01-01",
                ",,",
                "B,20,2025-01-02",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        pipeline_module,
        "load_full_dataframe",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("preview should not fully reload the sheet")),
    )

    response = preview_sheet(csv_path, file_id="test-preview-shifted", sheet_index=1)

    assert response.total_rows == 2
    assert response.preview_row_count == 2
    assert len(response.rows) == 2


class _FakePlannerClient:
    def __init__(self) -> None:
        self.enabled = True
        self.calls: list[dict[str, str]] = []

    def generate_json(self, schema_model, *, system_prompt: str, user_prompt: str):
        self.calls.append(
            {
                "schema": schema_model.__name__,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
            }
        )
        if schema_model is SelectionPlan:
            return SelectionPlan(columns=["Date", "Amount"])
        if schema_model is TransformPlan:
            return TransformPlan(
                derived_columns=[DerivedColumn(as_name="month_bucket", kind="date_bucket", source_col="Date", grain="month")],
                groupby=["month_bucket"],
                metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                order_by=Sort(col="month_bucket", dir="asc"),
            )
        if schema_model is ChartSpec:
            return ChartSpec(type="line", title="Trend", x="month_bucket", y="value", top_k=12)
        raise AssertionError(f"Unexpected schema: {schema_model}")


class _FakeContextInterpreter:
    name = "fake-followup-v1"

    def __init__(self, interpretation: FollowupInterpretation | None) -> None:
        self.interpretation = interpretation
        self.calls: list[dict[str, object]] = []

    def interpret(self, df, *, chat_text: str, requested_mode: str, followup_context=None):
        self.calls.append(
            {
                "chat_text": chat_text,
                "requested_mode": requested_mode,
                "followup_context": followup_context,
            }
        )
        return InterpretationResult(
            interpretation=self.interpretation,
            meta={
                "provider": self.name,
                "used": self.interpretation is not None,
                "confidence": float(self.interpretation.confidence or 0.0) if self.interpretation is not None else 0.0,
            },
        )


def test_openai_planner_uses_staged_prompts(monkeypatch) -> None:
    import app.services.spreadsheet.planning.planner as planner_module

    fake_client = _FakePlannerClient()
    monkeypatch.setattr(planner_module, "build_default_llm_client", lambda: fake_client)
    monkeypatch.setattr(planner_module, "get_default_context_interpreter", lambda: _FakeContextInterpreter(None))

    planner = OpenAIJsonPlanner()
    draft = planner.plan(
        _sample_df(),
        chat_text="Show the monthly trend of amount as a line chart.",
        requested_mode="chart",
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "total_amount",
                "mode": "text",
                "metric_aliases": ["total_amount"],
                "result_columns": ["total_amount"],
                "result_row_count": 1,
            },
            "last_turn": {
                "question": "What is the total amount?",
                "intent": "total_amount",
            },
            "recent_pipeline_history": [
                {
                    "question": "What is the total amount?",
                    "mode": "text",
                    "intent": "total_amount",
                    "pipeline_summary": {
                        "intent": "total_amount",
                        "mode": "text",
                        "metric_aliases": ["total_amount"],
                        "result_columns": ["total_amount"],
                        "result_row_count": 1,
                    },
                }
            ],
        },
    )

    assert [call["schema"] for call in fake_client.calls] == ["SelectionPlan", "TransformPlan", "ChartSpec"]
    assert "SelectionPlan schema" in fake_client.calls[0]["system_prompt"]
    assert "不要使用limit/distinct_by" in fake_client.calls[0]["system_prompt"]
    assert "TransformPlan schema" in fake_client.calls[1]["system_prompt"]
    assert "date_bucket" in fake_client.calls[1]["system_prompt"]
    assert "unique_key_candidates=" in fake_client.calls[1]["user_prompt"]
    assert "pivot_value_samples=" in fake_client.calls[1]["user_prompt"]
    assert "followup_context=" in fake_client.calls[0]["user_prompt"]
    assert "What is the total amount?" in fake_client.calls[0]["user_prompt"]
    assert "recent_pipeline_history" in fake_client.calls[0]["user_prompt"]
    assert "last_pipeline_summary" in fake_client.calls[1]["user_prompt"]
    assert "ChartSpec schema" in fake_client.calls[2]["system_prompt"]
    assert "result_csv" in fake_client.calls[2]["user_prompt"]
    assert draft.intent == "trend"
    assert draft.chart_spec is not None
    assert draft.chart_spec.type == "line"
    assert draft.planner_meta["prompt_style"] == "staged-v1"


def test_heuristic_planner_can_use_context_interpreter_for_pronoun_followup(monkeypatch) -> None:
    import app.services.spreadsheet.planning.planner as planner_module

    fake_interpreter = _FakeContextInterpreter(
        FollowupInterpretation(
            kind="followup_explain",
            standalone_question="Explain why Compute is ranked first by amount and support it with table details.",
            requires_previous_context=True,
            preserve_previous_analysis=True,
            target_label="Compute",
            target_rank=1,
            confidence=0.93,
            reasoning="Resolved the pronoun to the top ranked item in the previous turn.",
        )
    )
    monkeypatch.setattr(planner_module, "get_default_context_interpreter", lambda: fake_interpreter)

    draft = HeuristicPlanner().plan(
        _billing_df(),
        chat_text="为什么它这么高，能分析一下吗",
        requested_mode="auto",
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "ranking",
                "mode": "text",
                "groupby": ["Service Name"],
                "metric_aliases": ["value"],
                "result_columns": ["Service Name", "value"],
                "result_row_count": 3,
            },
            "last_turn": {
                "question": "Show the top 3 services by amount.",
                "intent": "ranking",
                "selection_plan": {
                    "columns": ["Service Name", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [],
                    "groupby": ["Service Name"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "value", "dir": "desc"},
                    "top_k": 3,
                },
            },
        },
    )

    assert fake_interpreter.calls
    assert draft.intent == "explain_ranked_item"
    assert draft.selection_plan.filters[0].col == "Service Name"
    assert draft.selection_plan.filters[0].value == "Compute"
    assert draft.planner_meta["context_interpreter"]["provider"] == "fake-followup-v1"


def test_heuristic_planner_can_use_context_interpreter_for_time_grain_switch(monkeypatch) -> None:
    import app.services.spreadsheet.planning.planner as planner_module

    fake_interpreter = _FakeContextInterpreter(
        FollowupInterpretation(
            kind="followup_switch",
            standalone_question="Keep the previous trend analysis but change the time grain from month to day.",
            requires_previous_context=True,
            preserve_previous_analysis=True,
            view_intent="trend",
            time_grain="day",
            confidence=0.91,
            reasoning="The user is refining the previous trend view.",
        )
    )
    monkeypatch.setattr(planner_module, "get_default_context_interpreter", lambda: fake_interpreter)

    draft = HeuristicPlanner().plan(
        _sample_df(),
        chat_text="不要按月，改成按天",
        requested_mode="auto",
        followup_context={
            "conversation_id": "conv-1",
            "turn_count": 2,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {
                "intent": "trend",
                "mode": "text",
                "groupby": ["month_bucket"],
                "metric_aliases": ["value"],
                "result_columns": ["month_bucket", "value"],
                "result_row_count": 2,
            },
            "last_turn": {
                "question": "Show the monthly trend of amount.",
                "intent": "trend",
                "selection_plan": {
                    "columns": ["Date", "Amount"],
                    "filters": [],
                    "distinct_by": None,
                    "sort": None,
                    "limit": None,
                },
                "transform_plan": {
                    "derived_columns": [{"as_name": "month_bucket", "kind": "date_bucket", "source_col": "Date", "grain": "month"}],
                    "groupby": ["month_bucket"],
                    "metrics": [{"agg": "sum", "col": "Amount", "as_name": "value"}],
                    "formula_metrics": [],
                    "having": [],
                    "pivot": None,
                    "post_pivot_formula_metrics": [],
                    "post_pivot_having": [],
                    "return_rows": False,
                    "order_by": {"col": "month_bucket", "dir": "asc"},
                    "top_k": 24,
                },
            },
        },
    )

    assert fake_interpreter.calls
    assert draft.intent == "trend"
    assert draft.transform_plan.derived_columns[0].grain == "day"
    assert draft.transform_plan.groupby == ["day_bucket"]
    assert draft.planner_meta["bucket_grain"] == "day"


def test_openai_planner_uses_interpreted_standalone_question_in_prompts(monkeypatch) -> None:
    import app.services.spreadsheet.planning.planner as planner_module

    fake_client = _FakePlannerClient()
    fake_interpreter = _FakeContextInterpreter(
        FollowupInterpretation(
            kind="followup_switch",
            standalone_question="Keep the same analysis and change it to a chart.",
            requires_previous_context=True,
            preserve_previous_analysis=True,
            output_mode="chart",
            confidence=0.88,
        )
    )
    monkeypatch.setattr(planner_module, "build_default_llm_client", lambda: fake_client)
    monkeypatch.setattr(planner_module, "get_default_context_interpreter", lambda: fake_interpreter)

    planner = OpenAIJsonPlanner()
    planner.plan(
        _sample_df(),
        chat_text="改成图表",
        requested_mode="auto",
        followup_context={
            "conversation_id": "conv-2",
            "turn_count": 1,
            "is_followup": True,
            "last_mode": "text",
            "last_pipeline_summary": {"intent": "ranking"},
            "last_turn": {"question": "Show the top 2 categories by amount.", "intent": "ranking"},
        },
    )

    assert "Keep the same analysis and change it to a chart." in fake_client.calls[0]["user_prompt"]
    assert "question=改成图表" not in fake_client.calls[0]["user_prompt"]


class _FailingTransformClient:
    def __init__(self) -> None:
        self.enabled = True

    def generate_json(self, schema_model, *, system_prompt: str, user_prompt: str):
        if schema_model is SelectionPlan:
            return SelectionPlan(columns=["Category", "Amount"])
        if schema_model is TransformPlan:
            raise OpenAICompatibleError("transform stage failed")
        raise AssertionError(f"Unexpected schema: {schema_model}")


class _InvalidSelectionClient:
    def __init__(self) -> None:
        self.enabled = True

    def generate_json(self, schema_model, *, system_prompt: str, user_prompt: str):
        if schema_model is SelectionPlan:
            return SelectionPlan(columns=["Missing Metric"])
        if schema_model is TransformPlan:
            return TransformPlan(metrics=[Metric(agg="count_rows", as_name="row_count")])
        raise AssertionError(f"Unexpected schema: {schema_model}")


def test_default_planner_falls_back_when_staged_llm_transform_fails(monkeypatch) -> None:
    import app.services.spreadsheet.planning.planner as planner_module

    monkeypatch.setattr(planner_module, "build_default_llm_client", lambda: _FailingTransformClient())
    get_settings.cache_clear()

    planner = get_default_planner()
    draft = planner.plan(
        _sample_df(),
        chat_text="Show the top 2 categories by amount as a bar chart.",
        requested_mode="chart",
    )

    assert draft.intent == "ranking"
    assert draft.chart_spec is not None
    assert draft.planner_meta["planner_provider_requested"] == "openai-json-v2"
    assert draft.planner_meta["planner_provider_used"] == "heuristic-v1"
    assert "transform stage failed" in draft.planner_meta["planner_fallback_reason"]


def test_analysis_governance_repairs_invalid_transform_plan() -> None:
    from app.services.spreadsheet.quality.policy import ANALYSIS_FULL_POLICY, govern_plan

    result = govern_plan(
        _sample_df(),
        TransformPlan(
            groupby=["Category"],
            metrics=[Metric(agg="sum", col="Ammount", as_name="value")],
            order_by=Sort(col="Ammount", dir="desc"),
        ),
        question="按分类统计消费金额",
        mode="text",
        policy=ANALYSIS_FULL_POLICY,
    )

    assert isinstance(result.plan, TransformPlan)
    assert result.plan.metrics[0].col == "Amount"
    assert result.plan.order_by is None
    assert result.issues == []
    assert result.repair["policy"] == "analysis_full"
    assert result.repair["rule"]["used"] is True


def test_default_planner_falls_back_when_light_governance_rejects_invalid_selection(monkeypatch) -> None:
    import app.services.spreadsheet.planning.planner as planner_module

    monkeypatch.setattr(planner_module, "build_default_llm_client", lambda: _InvalidSelectionClient())
    get_settings.cache_clear()

    planner = get_default_planner()
    draft = planner.plan(
        _sample_df(),
        chat_text="How many rows are in this sheet?",
        requested_mode="text",
    )

    assert draft.intent == "row_count"
    assert draft.planner_meta["planner_provider_requested"] == "openai-json-v2"
    assert draft.planner_meta["planner_provider_used"] == "heuristic-v1"
    assert "Column match is too weak: Missing Metric -> Category" in draft.planner_meta["planner_fallback_reason"]
