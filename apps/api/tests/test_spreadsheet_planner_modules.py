from __future__ import annotations

import pandas as pd

from app.services.spreadsheet.conversation.context_interpreter import (
    FollowupInterpretation,
    HeuristicContextInterpreter,
    InterpretationResult,
)
from app.services.spreadsheet.core.schema import ChartSpec, Metric, SelectionPlan, Sort, TransformPlan
from app.services.spreadsheet.pipeline.column_profile import attach_column_profiles, get_column_profiles
from app.services.spreadsheet.planning.planner_heuristic import HeuristicPlanner
from app.services.spreadsheet.planning.followup.planner_followup_context import (
    _effective_chat_text,
    _preserve_previous_analysis,
    _rank_lookup_followup,
    _with_interpreted_followup,
)
from app.services.spreadsheet.planning.followup.reuse_analysis import (
    _comparison_period_context,
)
from app.services.spreadsheet.planning.followup.reuse_base import (
    _load_previous_structured_turn,
    _previous_ranking_target_from_question,
)
from app.services.spreadsheet.planning.followup.planner_reuse import try_reuse_followup_plan
from app.services.spreadsheet.planning.planner_columns import (
    _find_amount_column,
    _find_date_column,
    _find_question_dimension_column,
    _find_single_transaction_group_column,
)
from app.services.spreadsheet.planning.planner_heuristic_actions import build_forecast_plan, try_build_heuristic_action
from app.services.spreadsheet.planning.planner_rules import (
    _build_resolved_columns,
    build_heuristic_planning_context,
)
from app.services.spreadsheet.planning.planner_runtime import (
    build_action_runtime_context,
    build_followup_signal_resolvers,
    build_reuse_runtime_context,
)
from app.services.spreadsheet.planning.planner_text_utils import _extract_top_k
from app.services.spreadsheet.planning.planner_time import (
    _build_month_range_filters,
    _extract_recent_period_count,
    _extract_time_grain,
    _resolve_recent_period_buckets,
    _resolve_requested_month_buckets,
    _resolve_requested_single_month_bucket,
)
from app.services.spreadsheet.planning.planner_types import (
    FollowupSignalResolvers,
    HeuristicActionRuntimeContext,
    PlanDraft,
    ReuseFollowupRuntimeContext,
)


def _billing_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "账单月份": ["202603", "202603", "202602"],
                "账单日期": ["20260301", "20260305", "20260220"],
                "Service Name": ["Compute", "Storage", "Compute"],
                "Billing Item Name": ["Instance", "Disk", "Bandwidth"],
                "Region": ["cn-sh", "cn-bj", "cn-bj"],
                "Transaction ID": ["T-101", "T-102", "T-103"],
                "应付金额": [120, 80, 40],
            }
        )
    )


def _monthly_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Date": ["2025-01-03", "2025-02-04", "2025-03-06"],
                "Amount": [100.0, 120.0, 140.0],
                "Category": ["A", "B", "A"],
            }
        )
    )


def _monthly_compare_df() -> pd.DataFrame:
    return attach_column_profiles(
        pd.DataFrame(
            {
                "Date": ["2024-02-03", "2025-01-04", "2025-02-06"],
                "Amount": [90.0, 120.0, 140.0],
                "Category": ["A", "A", "A"],
            }
        )
    )


def _ranking_followup_context() -> dict[str, object]:
    selection_plan = SelectionPlan(columns=["Category", "Amount"])
    transform_plan = TransformPlan(
        groupby=["Category"],
        metrics=[Metric(agg="sum", col="Amount", as_name="value")],
        order_by=Sort(col="value", dir="desc"),
        top_k=5,
    )
    return {
        "is_followup": True,
        "last_mode": "chart",
        "last_turn": {
            "intent": "ranking",
            "question": "按分类统计费用排行",
            "selection_plan": selection_plan.model_dump(),
            "transform_plan": transform_plan.model_dump(),
            "chart_spec": ChartSpec(type="bar", title="Ranking", x="Category", y="value", top_k=5).model_dump(),
        },
    }


def test_planner_columns_prefers_real_date_and_amount_columns() -> None:
    profiles = get_column_profiles(_billing_df())

    assert _find_date_column(profiles) == "账单日期"
    assert _find_amount_column(profiles) == "应付金额"


def test_planner_columns_picks_question_dimension_and_unique_transaction_column() -> None:
    df = _billing_df()
    profiles = get_column_profiles(df)

    assert _find_question_dimension_column(
        profiles,
        "按服务统计费用",
        item_column="Billing Item Name",
        service_column="Service Name",
        region_column="Region",
        category_column="Billing Item Name",
    ) == "Service Name"
    assert _find_single_transaction_group_column(df, profiles, exclude={"应付金额"}) == "Transaction ID"


def test_planner_time_resolves_single_and_multiple_month_buckets() -> None:
    df = _monthly_df()

    assert _resolve_requested_single_month_bucket(df, date_column="Date", chat_text="看一下2月总金额") == "2025-02"
    assert _resolve_requested_month_buckets(df, date_column="Date", chat_text="看一下1月和3月分别多少") == ["2025-01", "2025-03"]
    assert _resolve_requested_single_month_bucket(df, date_column="Date", chat_text="看上个月趋势") == "2025-02"
    assert _resolve_requested_single_month_bucket(df, date_column="Date", chat_text="看本月趋势") == "2025-03"


def test_planner_time_extracts_recent_period_count_and_grain() -> None:
    assert _extract_time_grain("最近7天趋势", default="month") == "day"
    assert _extract_recent_period_count("最近7天趋势", grain="day") == 7
    assert _extract_time_grain("按周看最近4周趋势", default="month") == "week"
    assert _extract_recent_period_count("按周看最近4周趋势", grain="week") == 4
    assert _extract_time_grain("近三个月趋势", default="month") == "month"
    assert _extract_recent_period_count("近三个月趋势", grain="month") == 3


def test_planner_time_resolves_recent_period_bucket_window() -> None:
    assert _resolve_recent_period_buckets(_monthly_df(), date_column="Date", grain="month", count=2) == ["2025-02", "2025-03"]


def test_planner_time_builds_month_range_filters() -> None:
    filters = _build_month_range_filters("Date", "2025-02")

    assert [(flt.col, flt.op, flt.value) for flt in filters] == [
        ("Date", ">=", "2025-02-01"),
        ("Date", "<", "2025-03-01"),
    ]


def test_planner_text_utils_extract_top_k_is_shared() -> None:
    assert _extract_top_k("继续看前12个") == 12
    assert _extract_top_k("top 7 services") == 7


def test_heuristic_planner_remains_constructible_from_canonical_module() -> None:
    draft = HeuristicPlanner().plan(
        _monthly_df(),
        chat_text="总金额是多少",
        requested_mode="auto",
    )

    assert draft.intent == "total_amount"
    assert draft.analysis_intent is not None
    assert draft.analysis_intent.kind == "total_amount"
    assert draft.analysis_intent.target_metric == "Amount"
    assert draft.analysis_intent.answer_expectation == "single_value"


def test_heuristic_planner_builds_structured_intent_for_service_ranking() -> None:
    draft = HeuristicPlanner().plan(
        _billing_df(),
        chat_text="按服务统计费用排行",
        requested_mode="chart",
    )

    assert draft.analysis_intent is not None
    assert draft.analysis_intent.kind == "ranking"
    assert draft.analysis_intent.target_metric == "应付金额"
    assert draft.analysis_intent.target_dimension == "Service Name"
    assert draft.analysis_intent.comparison_type == "ranking"
    assert draft.analysis_intent.answer_expectation == "chart"


def test_heuristic_planner_marks_join_beta_candidate_signals() -> None:
    draft = HeuristicPlanner().plan(
        _billing_df(),
        chat_text="Join Sales and Users by email and show top 5 by total amount.",
        requested_mode="auto",
    )

    assert draft.analysis_intent is not None
    assert draft.analysis_intent.join_requested is True
    assert draft.analysis_intent.join_key == "email"
    assert draft.analysis_intent.join_type == "unspecified"
    assert draft.analysis_intent.join_beta_eligible is True
    assert draft.analysis_intent.join_gate_reasons == []


def test_heuristic_planner_marks_join_out_of_scope_signals() -> None:
    draft = HeuristicPlanner().plan(
        _billing_df(),
        chat_text="Join Sales and Users then list detail rows.",
        requested_mode="auto",
    )

    assert draft.analysis_intent is not None
    assert draft.analysis_intent.join_requested is True
    assert draft.analysis_intent.join_beta_eligible is False
    assert "join_key_missing" in draft.analysis_intent.join_gate_reasons
    assert "join_non_aggregate_query" in draft.analysis_intent.join_gate_reasons


def test_heuristic_planner_requests_intent_level_clarification_for_generic_name_dimension() -> None:
    draft = HeuristicPlanner().plan(
        _billing_df(),
        chat_text="按名称统计费用排行",
        requested_mode="auto",
    )

    assert draft.intent == "ranking"
    assert draft.analysis_intent is not None
    assert draft.analysis_intent.clarification is not None
    assert draft.analysis_intent.answer_expectation == "clarification"
    assert draft.analysis_intent.clarification.field == "名称"
    assert [option["value"] for option in draft.analysis_intent.clarification.options] == [
        "Service Name",
        "Billing Item Name",
    ]


def test_planner_rules_builds_nested_followup_and_columns_context() -> None:
    context = build_heuristic_planning_context(
        _billing_df(),
        chat_text="按服务统计费用排行",
        requested_mode="auto",
        followup_context=None,
    )

    assert context.followup.mode == "text"
    assert context.followup.effective_chat_text == "按服务统计费用排行"
    assert context.columns.amount_column == "应付金额"
    assert context.columns.date_column == "账单日期"
    assert context.columns.service_column == "Service Name"
    assert context.columns.question_dimension_column == "Service Name"
    assert context.planner_meta["service_column"] == "Service Name"


def test_planner_rules_resolved_columns_builder_matches_expected_columns() -> None:
    df = _billing_df()
    profiles = get_column_profiles(df)

    columns = _build_resolved_columns(
        df,
        profiles=profiles,
        chat_text="按服务统计费用排行",
        effective_chat_text="按服务统计费用排行",
    )

    assert columns.amount_column == "应付金额"
    assert columns.date_column == "账单日期"
    assert columns.service_column == "Service Name"
    assert columns.item_preferred_column is not None
    assert columns.item_column is not None
    assert columns.question_dimension_column == "Service Name"


def test_planner_rules_clarification_resolution_overrides_question_dimension_column() -> None:
    context = build_heuristic_planning_context(
        _billing_df(),
        chat_text="按名称统计费用排行",
        requested_mode="auto",
        followup_context={
            "is_followup": True,
            "clarification_resolution": {
                "kind": "column_resolution",
                "source_field": "名称",
                "selected_value": "Billing Item Name",
            },
        },
    )

    assert context.columns.question_dimension_column == "Billing Item Name"
    assert context.columns.raw_question_dimension_column == "Billing Item Name"
    assert context.planner_meta["clarification_resolution"]["selected_value"] == "Billing Item Name"


def test_planner_rules_analysis_anchor_overrides_followup_core_columns() -> None:
    context = build_heuristic_planning_context(
        _billing_df(),
        chat_text="继续分析",
        requested_mode="auto",
        followup_context={
            "is_followup": True,
            "analysis_anchor": {
                "intent": "ranking",
                "metric_column": "应付金额",
                "dimension_column": "Service Name",
                "time_column": "账单日期",
                "time_grain": "month",
            },
        },
    )

    assert context.columns.amount_column == "应付金额"
    assert context.columns.date_column == "账单日期"
    assert context.columns.question_dimension_column == "Service Name"
    assert context.columns.raw_question_dimension_column == "Service Name"


def test_planner_runtime_builders_derive_reuse_and_action_contexts() -> None:
    context = build_heuristic_planning_context(
        _billing_df(),
        chat_text="按服务统计费用排行",
        requested_mode="chart",
        followup_context={"is_followup": True},
    )

    reuse_runtime = build_reuse_runtime_context(context)
    action_runtime = build_action_runtime_context(context)

    assert reuse_runtime.mode == "chart"
    assert reuse_runtime.service_column == "Service Name"
    assert action_runtime.mode == "chart"
    assert action_runtime.question_dimension_column == "Service Name"
    assert action_runtime.profiles is context.profiles


def test_planner_runtime_builds_default_followup_signal_resolvers() -> None:
    resolvers = build_followup_signal_resolvers()

    assert resolvers.rank_position_from_text("第二名") == 2
    assert resolvers.top_k_followup("继续看前3个", {"is_followup": True, "last_turn": {"intent": "ranking"}}) == 3


def test_followup_context_builds_effective_question_and_preserve_flag() -> None:
    followup_context = {
        "is_followup": True,
        "last_mode": "chart",
        "last_pipeline_summary": {"intent": "ranking"},
        "last_turn": {"intent": "ranking", "question": "按分类统计费用排行"},
    }

    effective = _effective_chat_text("继续看前3个", followup_context)

    assert "Previous question: 按分类统计费用排行" in effective
    assert "Follow-up request: 继续看前3个" in effective
    assert _preserve_previous_analysis("继续", followup_context) is True
    assert _preserve_previous_analysis("继续看前3个", followup_context) is False
    assert _preserve_previous_analysis("改成趋势", followup_context) is False


def test_followup_context_effective_question_includes_analysis_anchor_summary() -> None:
    followup_context = {
        "is_followup": True,
        "last_mode": "chart",
        "last_turn": {"intent": "ranking", "question": "按分类统计费用排行"},
        "analysis_anchor": {
            "metric_column": "Amount",
            "dimension_column": "Category",
            "time_column": "Date",
            "time_grain": "month",
            "filters": [{"column": "Region", "op": "=", "value": "cn-sh"}],
        },
    }

    effective = _effective_chat_text("继续看前3个", followup_context)

    assert "Previous analysis anchor: metric=Amount" in effective
    assert "dimension=Category" in effective
    assert "time=Date(month)" in effective


def test_followup_context_preserves_previous_analysis_for_sheet_switch_requests() -> None:
    switch_followup_context = {
        "is_followup": True,
        "wants_sheet_switch": True,
        "last_mode": "text",
        "last_turn": {"intent": "trend", "question": "看一下 Sales 的月度趋势"},
    }
    interpreted_switch_context = {
        "is_followup": True,
        "_interpreted": {"kind": "followup_refine", "switch_sheet": True},
        "last_turn": {"intent": "trend", "question": "看一下 Sales 的月度趋势"},
    }

    assert _preserve_previous_analysis("再看另一个 sheet", switch_followup_context) is True
    assert _preserve_previous_analysis("继续", interpreted_switch_context) is True


def test_followup_context_rank_lookup_uses_interpreted_rank() -> None:
    followup_context = {
        "last_turn": {"intent": "ranking"},
        "_interpreted": {"target_rank": 2, "kind": "followup_lookup"},
    }

    assert _rank_lookup_followup("看第二名", followup_context) == 2


def test_with_interpreted_followup_adds_confident_interpretation() -> None:
    class FakeInterpreter:
        name = "fake"

        def interpret(self, df, *, chat_text, requested_mode, followup_context):  # noqa: ANN001
            return InterpretationResult(
                interpretation=FollowupInterpretation(
                    kind="followup_refine",
                    confidence=0.9,
                    standalone_question="按服务统计费用",
                ),
                meta={"provider": "fake"},
            )

    enhanced, meta = _with_interpreted_followup(
        _billing_df(),
        chat_text="按服务",
        requested_mode="auto",
        followup_context={"is_followup": True},
        context_interpreter_factory=lambda: FakeInterpreter(),
    )

    assert enhanced is not None
    assert enhanced["_interpreted"]["standalone_question"] == "按服务统计费用"
    assert meta == {"provider": "fake"}


def test_followup_reuse_loads_previous_turn_and_resolves_previous_target() -> None:
    df = _monthly_df()
    followup_context = _ranking_followup_context()

    loaded = _load_previous_structured_turn(followup_context)
    assert loaded is not None
    assert loaded[0] == "ranking"

    target = _previous_ranking_target_from_question(
        df,
        "上面第一个是什么",
        followup_context,
        rank_position_from_text=lambda _text: 1,
    )
    assert target is not None
    assert target[0] == "Category"
    assert target[1] == "A"
    assert target[3] == 1


def test_followup_reuse_skips_previous_plan_when_sheet_has_switched() -> None:
    followup_context = _ranking_followup_context()
    followup_context.update(
        {
            "last_sheet_index": 2,
            "current_sheet_index": 1,
        }
    )

    loaded = _load_previous_structured_turn(followup_context)

    assert loaded is None


def test_followup_reuse_can_load_anchor_structured_turn_when_sheet_has_switched() -> None:
    followup_context = _ranking_followup_context()
    followup_context.update(
        {
            "last_sheet_index": 2,
            "current_sheet_index": 1,
            "analysis_anchor": {
                "intent": "ranking",
                "selection_plan": SelectionPlan(columns=["Category", "Amount"]).model_dump(),
                "transform_plan": TransformPlan(
                    groupby=["Category"],
                    metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=5,
                ).model_dump(),
                "chart_spec": ChartSpec(type="bar", title="Ranking", x="Category", y="value", top_k=5).model_dump(),
            },
        }
    )

    loaded = _load_previous_structured_turn(followup_context)

    assert loaded is not None
    assert loaded[0] == "ranking"


def test_heuristic_context_interpreter_resolves_previous_sheet_reference() -> None:
    interpreter = HeuristicContextInterpreter()

    result = interpreter.interpret(
        _billing_df(),
        chat_text="回到上一个 sheet 继续",
        requested_mode="auto",
        followup_context={
            "is_followup": True,
            "sheet_reference_hint": "previous",
        },
    )

    assert result.interpretation is not None
    assert result.interpretation.kind == "followup_switch"
    assert result.interpretation.switch_sheet is True
    assert result.interpretation.sheet_reference == "previous"
    assert result.interpretation.preserve_previous_analysis is True


def test_reuse_runtime_context_exposes_candidate_columns_in_priority_order() -> None:
    runtime_context = ReuseFollowupRuntimeContext(
        mode="chart",
        amount_column="Amount",
        date_column="Date",
        raw_question_dimension_column="Service Name",
        question_dimension_column="Service Name",
        service_column="Service Name",
        region_column="Region",
        item_column="Billing Item Name",
        item_preferred_column="Billing Item Name",
        category_column="Category",
        followup_context=None,
        planner_meta={},
        profiles={},
    )

    assert runtime_context.candidate_columns == [
        "Service Name",
        "Service Name",
        "Service Name",
        "Region",
        "Billing Item Name",
        "Billing Item Name",
        "Category",
    ]


def test_followup_signal_resolvers_bundle_callables() -> None:
    resolvers = FollowupSignalResolvers(
        rank_position_from_text=lambda _text: 1,
        top_k_followup=lambda _text, _context: 3,
        mode_switch_followup=lambda _text, **_kwargs: True,
        rank_lookup_followup=lambda _text, _context: 2,
        share_switch_followup=lambda _text, _context: False,
        dimension_switch_followup=lambda _text, **_kwargs: False,
        trend_switch_followup=lambda _text, _context: True,
        detail_switch_followup=lambda _text, _context: False,
        time_filter_followup=lambda _text, _context: ("contains", "2025-03"),
    )

    assert resolvers.rank_position_from_text("first") == 1
    assert resolvers.top_k_followup("top3", None) == 3
    assert resolvers.mode_switch_followup("切换图表", mode="chart", followup_context=None) is True
    assert resolvers.rank_lookup_followup("第二名", None) == 2
    assert resolvers.trend_switch_followup("改成趋势", None) is True
    assert resolvers.time_filter_followup("看3月", None) == ("contains", "2025-03")


def test_followup_reuse_builds_period_compare_context_from_previous_selection() -> None:
    followup_context = {
        "last_turn": {
            "intent": "total_amount",
            "selection_plan": SelectionPlan(filters=[{"col": "Date", "op": "contains", "value": "2025-03"}]).model_dump(),
            "transform_plan": TransformPlan(metrics=[Metric(agg="sum", col="Amount", as_name="value")]).model_dump(),
        }
    }

    context = _comparison_period_context(
        _monthly_df(),
        chat_text="和上个月比",
        date_column="Date",
        followup_context=followup_context,
    )

    assert context == ("month", "contains", "2025-03", "2025-02")


def test_planner_reuse_adds_unified_reuse_strategy_trace() -> None:
    draft = try_reuse_followup_plan(
        _monthly_df(),
        chat_text="继续看前3个",
        runtime_context=ReuseFollowupRuntimeContext(
            mode="chart",
            amount_column="Amount",
            date_column="Date",
            raw_question_dimension_column="Category",
            question_dimension_column="Category",
            service_column=None,
            region_column=None,
            item_column="Category",
            item_preferred_column="Category",
            category_column="Category",
            followup_context=_ranking_followup_context(),
            planner_meta={"planner": "heuristic-v1"},
            profiles=get_column_profiles(_monthly_df()),
        ),
        signal_resolvers=build_followup_signal_resolvers(),
        plan_draft_factory=PlanDraft,
    )

    assert draft is not None
    assert draft.planner_meta["followup_reused_previous_plan"] is True
    assert draft.planner_meta["reuse_strategy"] == "top_k"


def test_heuristic_actions_build_forecast_plan_returns_timeseries_plan() -> None:
    draft = build_forecast_plan(
        _monthly_df(),
        chat_text="预测下个月",
        date_column="Date",
        amount_column="Amount",
        planner_meta={},
        plan_draft_factory=PlanDraft,
    )

    assert draft is not None
    assert draft.intent == "forecast_timeseries"
    assert draft.planner_meta["forecast_target_period"] == "2025-04"


def test_heuristic_planner_builds_direct_period_compare_plan() -> None:
    draft = HeuristicPlanner().plan(
        _monthly_df(),
        chat_text="和上个月对比差值",
        requested_mode="auto",
    )

    assert draft.intent == "period_compare"
    assert draft.planner_meta["compare_basis"] == "previous_period"
    assert draft.planner_meta["comparison_type"] == "delta"
    assert draft.planner_meta["current_period"] == "2025-03"
    assert draft.planner_meta["previous_period"] == "2025-02"
    assert draft.transform_plan.pivot is not None
    assert [metric.as_name for metric in draft.transform_plan.post_pivot_formula_metrics] == ["change_value", "change_pct"]
    assert draft.analysis_intent is not None
    assert draft.analysis_intent.comparison_type == "delta"
    assert draft.analysis_intent.time_scope is not None
    assert draft.analysis_intent.time_scope.base_period == "2025-02"
    assert draft.analysis_intent.time_scope.compare_window == ["2025-02", "2025-03"]


def test_heuristic_planner_builds_year_over_year_ratio_compare_plan() -> None:
    draft = HeuristicPlanner().plan(
        _monthly_compare_df(),
        chat_text="同比占比是多少",
        requested_mode="auto",
    )

    assert draft.intent == "period_compare"
    assert draft.planner_meta["compare_basis"] == "year_over_year"
    assert draft.planner_meta["comparison_type"] == "ratio"
    assert draft.planner_meta["current_period"] == "2025-02"
    assert draft.planner_meta["previous_period"] == "2024-02"
    assert [metric.as_name for metric in draft.transform_plan.post_pivot_formula_metrics] == [
        "change_value",
        "change_pct",
        "compare_ratio",
    ]
    assert draft.analysis_intent is not None
    assert draft.analysis_intent.comparison_type == "ratio"


def test_heuristic_planner_applies_direct_value_filters_for_ranking() -> None:
    draft = HeuristicPlanner().plan(
        _billing_df(),
        chat_text="只看 cn-sh，按服务统计费用前2",
        requested_mode="auto",
    )

    assert draft.intent == "ranking"
    assert draft.selection_plan.filters
    assert [(flt.col, flt.op, flt.value) for flt in draft.selection_plan.filters] == [("Region", "=", "cn-sh")]
    assert draft.transform_plan.top_k == 2
    assert draft.planner_meta["value_filters"] == [{"column": "Region", "value": "cn-sh"}]


def test_heuristic_planner_applies_day_grain_with_relative_month_filter() -> None:
    draft = HeuristicPlanner().plan(
        _monthly_df(),
        chat_text="按天看上月趋势",
        requested_mode="auto",
    )

    assert draft.intent == "trend"
    assert draft.transform_plan.derived_columns[0].grain == "day"
    assert draft.selection_plan.filters
    assert [(flt.col, flt.op, flt.value) for flt in draft.selection_plan.filters] == [
        ("Date", ">=", "2025-02-01"),
        ("Date", "<", "2025-03-01"),
    ]
    assert draft.planner_meta["requested_period"] == "2025-02"


def test_heuristic_planner_applies_recent_period_window_for_trend() -> None:
    draft = HeuristicPlanner().plan(
        _monthly_df(),
        chat_text="看最近2个月趋势",
        requested_mode="auto",
    )

    assert draft.intent == "trend"
    assert draft.transform_plan.having
    assert [(flt.col, flt.op, flt.value) for flt in draft.transform_plan.having] == [("month_bucket", "in", ["2025-02", "2025-03"])]
    assert draft.planner_meta["requested_recent_period_count"] == 2


def test_heuristic_actions_build_ranking_and_share_plans() -> None:
    df = _billing_df()
    profiles = get_column_profiles(df)

    ranking = try_build_heuristic_action(
        df,
        chat_text="按服务统计费用排行",
        runtime_context=HeuristicActionRuntimeContext(
            effective_chat_text="按服务统计费用排行",
            mode="chart",
            followup_context=None,
            preserve_previous_analysis=False,
            profiles=profiles,
            planner_meta={},
            amount_column="应付金额",
            date_column="账单日期",
            category_column="Billing Item Name",
            single_transaction_column="Transaction ID",
            item_preferred_column="Billing Item Name",
            item_column="Billing Item Name",
            question_dimension_column="Service Name",
        ),
        plan_draft_factory=PlanDraft,
    )

    assert ranking is not None
    assert ranking.intent == "ranking"
    assert ranking.chart_spec is not None
    assert ranking.chart_spec.x == "Service Name"
    assert ranking.planner_meta["chart_context"]["x_label"] == "Service Name"
    assert ranking.planner_meta["chart_context"]["y_label"] == "value"
    assert ranking.planner_meta["chart_context"]["y_unit"] == "amount"

    share = try_build_heuristic_action(
        df,
        chat_text="看一下费用占比",
        runtime_context=HeuristicActionRuntimeContext(
            effective_chat_text="看一下费用占比",
            mode="chart",
            followup_context=None,
            preserve_previous_analysis=False,
            profiles=profiles,
            planner_meta={},
            amount_column="应付金额",
            date_column="账单日期",
            category_column="Billing Item Name",
            single_transaction_column="Transaction ID",
            item_preferred_column="Billing Item Name",
            item_column="Billing Item Name",
            question_dimension_column="Service Name",
        ),
        plan_draft_factory=PlanDraft,
    )

    assert share is not None
    assert share.intent == "share"
    assert share.chart_spec is not None
    assert share.chart_spec.type == "pie"
    assert share.planner_meta["chart_context"]["recommended_type"] == "pie"
    assert share.planner_meta["chart_context"]["y_unit"] == "amount"


def test_heuristic_actions_recommend_bar_for_large_share_slice_count() -> None:
    df = _billing_df()
    profiles = get_column_profiles(df)

    share = try_build_heuristic_action(
        df,
        chat_text="看一下前12个服务费用占比",
        runtime_context=HeuristicActionRuntimeContext(
            effective_chat_text="看一下前12个服务费用占比",
            mode="chart",
            followup_context=None,
            preserve_previous_analysis=False,
            profiles=profiles,
            planner_meta={},
            amount_column="应付金额",
            date_column="账单日期",
            category_column="Billing Item Name",
            single_transaction_column="Transaction ID",
            item_preferred_column="Billing Item Name",
            item_column="Billing Item Name",
            question_dimension_column="Service Name",
        ),
        plan_draft_factory=PlanDraft,
    )

    assert share is not None
    assert share.intent == "share"
    assert share.chart_spec is not None
    assert share.chart_spec.type == "bar"
    assert share.planner_meta["chart_context"]["recommended_type"] == "bar"


def test_heuristic_actions_set_metric_unit_for_trend_chart_context() -> None:
    df = _monthly_df()
    profiles = get_column_profiles(df)

    trend = try_build_heuristic_action(
        df,
        chat_text="看最近两个月趋势图",
        runtime_context=HeuristicActionRuntimeContext(
            effective_chat_text="看最近两个月趋势图",
            mode="chart",
            followup_context=None,
            preserve_previous_analysis=False,
            profiles=profiles,
            planner_meta={},
            amount_column="Amount",
            date_column="Date",
            category_column="Category",
            single_transaction_column=None,
            item_preferred_column=None,
            item_column="Category",
            question_dimension_column="Category",
        ),
        plan_draft_factory=PlanDraft,
    )

    assert trend is not None
    assert trend.intent == "trend"
    assert trend.chart_spec is not None
    assert trend.planner_meta["chart_context"]["y_unit"] == "amount"
