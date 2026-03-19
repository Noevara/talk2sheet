from __future__ import annotations

import pandas as pd

from app.config import get_settings
from app.schemas import ExecutionDisclosure
from app.services.spreadsheet.analysis import analyze
from app.services.spreadsheet.conversation.answer_generator import (
    AnswerGeneratorContext,
    LLMAnswerGenerator,
    RuleBasedAnswerGenerator,
    get_default_answer_generator,
)
from app.services.spreadsheet.core.schema import ChartSpec, FormulaMetric, Metric, PivotSpec, SelectionPlan, Sort, TransformPlan
from app.services.spreadsheet.openai_compatible import OpenAICompatibleError
from app.services.spreadsheet.planning.intent_models import AnalysisIntent
from app.services.spreadsheet.planning.planner import PlanDraft


def _ranking_context() -> AnswerGeneratorContext:
    return AnswerGeneratorContext(
        locale="en",
        draft=PlanDraft(
            mode="chart",
            intent="ranking",
            selection_plan=SelectionPlan(columns=["Category", "Amount"]),
            transform_plan=TransformPlan(
                groupby=["Category"],
                metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                order_by=Sort(col="value", dir="desc"),
                top_k=2,
            ),
            chart_spec=ChartSpec(type="bar", x="Category", y="value", top_k=2),
            analysis_intent=AnalysisIntent(
                kind="ranking",
                legacy_intent="ranking",
                target_metric="Amount",
                target_dimension="Category",
                comparison_type="ranking",
                answer_expectation="chart",
            ),
        ),
        result_df=pd.DataFrame({"Category": ["A", "B"], "value": [180, 50]}),
        selection_plan=SelectionPlan(columns=["Category", "Amount"]),
        transform_plan=TransformPlan(
            groupby=["Category"],
            metrics=[Metric(agg="sum", col="Amount", as_name="value")],
            order_by=Sort(col="value", dir="desc"),
            top_k=2,
        ),
        chart_spec=ChartSpec(type="bar", x="Category", y="value", top_k=2),
    )


def test_rule_answer_generator_summarizes_ranking_result() -> None:
    result = RuleBasedAnswerGenerator().generate(_ranking_context())

    assert "A" in result.answer
    assert "180" in result.answer
    assert "B" in result.analysis_text
    assert "Top 2" in result.analysis_text
    assert "bar chart" in result.analysis_text
    assert result.meta["summary_kind"] == "ranking"
    assert result.meta["analysis_intent"]["kind"] == "ranking"


def test_rule_answer_generator_prefers_structured_intent_kind_over_legacy_string() -> None:
    context = _ranking_context()
    context.draft.intent = "unsupported"
    result = RuleBasedAnswerGenerator().generate(context)

    assert result.meta["summary_kind"] == "ranking"
    assert "A" in result.answer


def test_rule_answer_generator_summarizes_trend_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="en",
            draft=PlanDraft(
                mode="chart",
                intent="trend",
                selection_plan=SelectionPlan(columns=["Date", "Amount"]),
                transform_plan=TransformPlan(
                    groupby=["month_bucket"],
                    metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                    order_by=Sort(col="month_bucket", dir="asc"),
                ),
                chart_spec=ChartSpec(type="line", x="month_bucket", y="value", top_k=12),
            ),
            result_df=pd.DataFrame(
                {
                    "month_bucket": ["2025-01", "2025-02", "2025-03"],
                    "value": [10, 30, 20],
                }
            ),
            selection_plan=SelectionPlan(columns=["Date", "Amount"]),
            transform_plan=TransformPlan(
                groupby=["month_bucket"],
                metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                order_by=Sort(col="month_bucket", dir="asc"),
            ),
            chart_spec=ChartSpec(type="line", x="month_bucket", y="value", top_k=12),
        )
    )

    assert "2025-02" in result.answer
    assert "30" in result.answer
    assert "2025-03" in result.analysis_text
    assert "up 10" in result.analysis_text
    assert result.meta["summary_kind"] == "trend"


def test_rule_answer_generator_summarizes_period_breakdown_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="period_breakdown",
                selection_plan=SelectionPlan(columns=["Date", "Amount"]),
                transform_plan=TransformPlan(
                    derived_columns=[],
                    groupby=["month_bucket"],
                    metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                    order_by=Sort(col="month_bucket", dir="asc"),
                ),
            ),
            result_df=pd.DataFrame({"month_bucket": ["2025-01", "2025-02"], "value": [460.0, 1110.0]}),
            selection_plan=SelectionPlan(columns=["Date", "Amount"]),
            transform_plan=TransformPlan(
                groupby=["month_bucket"],
                metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                order_by=Sort(col="month_bucket", dir="asc"),
            ),
        )
    )

    assert "2025-01为 460" in result.answer
    assert "2025-02为 1,110" in result.answer
    assert result.meta["summary_kind"] == "period_breakdown"


def test_rule_answer_generator_summarizes_active_day_count_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="active_day_count",
                selection_plan=SelectionPlan(columns=["账单日期"]),
                transform_plan=TransformPlan(metrics=[Metric(agg="count_distinct", col="day_bucket", as_name="active_day_count")]),
                planner_meta={"requested_period": "2026-03", "bucket_name": "day_bucket"},
            ),
            result_df=pd.DataFrame({"active_day_count": [3]}),
            selection_plan=SelectionPlan(columns=["账单日期"]),
            transform_plan=TransformPlan(metrics=[Metric(agg="count_distinct", col="day_bucket", as_name="active_day_count")]),
        )
    )

    assert result.answer == "2026-03 共有 3 天有数据。"
    assert result.meta["summary_kind"] == "active_day_count"


def test_rule_answer_generator_summarizes_forecast_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="forecast_timeseries",
                selection_plan=SelectionPlan(columns=["账单月份", "应付金额"]),
                transform_plan=TransformPlan(
                    groupby=["month_bucket"],
                    metrics=[Metric(agg="sum", col="应付金额", as_name="value")],
                    order_by=Sort(col="month_bucket", dir="asc"),
                ),
                planner_meta={"forecast_target_period": "2026-04", "forecast_grain": "month", "bucket_name": "month_bucket"},
            ),
            result_df=pd.DataFrame(
                {
                    "month_bucket": ["2026-04"],
                    "forecast_value": [160.0],
                    "lower_bound": [140.0],
                    "upper_bound": [180.0],
                    "model": ["linear_regression"],
                }
            ),
            selection_plan=SelectionPlan(columns=["账单月份", "应付金额"]),
            transform_plan=TransformPlan(
                groupby=["month_bucket"],
                metrics=[Metric(agg="sum", col="应付金额", as_name="value")],
                order_by=Sort(col="month_bucket", dir="asc"),
            ),
            transform_meta={
                "forecast": {
                    "target_period": "2026-04",
                    "grain": "month",
                    "model": "linear_regression",
                    "history_points": 3,
                    "history_start": "2026-01",
                    "history_end": "2026-03",
                    "forecast_value": 160.0,
                    "lower_bound": 140.0,
                    "upper_bound": 180.0,
                }
            },
        )
    )

    assert result.answer == "预测 2026-04 的总费用约为 160。"
    assert "线性回归趋势" in result.segments["evidence"]
    assert "95% 参考区间约为 140 到 180" in result.segments["risk_note"]
    assert result.meta["summary_kind"] == "forecast_timeseries"


def test_rule_answer_generator_summarizes_forecast_series_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="forecast_timeseries",
                selection_plan=SelectionPlan(columns=["账单日期", "应付金额"]),
                transform_plan=TransformPlan(
                    groupby=["day_bucket"],
                    metrics=[Metric(agg="sum", col="应付金额", as_name="value")],
                    order_by=Sort(col="day_bucket", dir="asc"),
                ),
                planner_meta={
                    "forecast_target_period": "2026-03-31",
                    "forecast_target_periods": ["2026-03-16", "2026-03-17"],
                    "forecast_grain": "day",
                    "bucket_name": "day_bucket",
                },
            ),
            result_df=pd.DataFrame(
                {
                    "day_bucket": ["2026-03-16", "2026-03-17"],
                    "forecast_value": [80.0, 82.0],
                    "lower_bound": [60.0, 58.0],
                    "upper_bound": [100.0, 106.0],
                    "model": ["linear_regression", "linear_regression"],
                }
            ),
            selection_plan=SelectionPlan(columns=["账单日期", "应付金额"]),
            transform_plan=TransformPlan(
                groupby=["day_bucket"],
                metrics=[Metric(agg="sum", col="应付金额", as_name="value")],
                order_by=Sort(col="day_bucket", dir="asc"),
            ),
            transform_meta={
                "forecast": {
                    "target_period": "2026-03-31",
                    "target_periods": ["2026-03-16", "2026-03-17"],
                    "target_start_period": "2026-03-16",
                    "target_end_period": "2026-03-17",
                    "target_count": 2,
                    "grain": "day",
                    "model": "linear_regression",
                    "history_points": 74,
                    "history_start": "2026-01-01",
                    "history_end": "2026-03-15",
                    "forecast_value": 162.0,
                    "average_forecast_value": 81.0,
                    "lower_bound": 118.0,
                    "upper_bound": 206.0,
                    "multi_step": True,
                }
            },
        )
    )

    assert result.answer == "已预测 2026-03-16 到 2026-03-17 共 2 个未来天周期的费用。"
    assert "预测总计约为 162" in result.segments["evidence"]
    assert "每个预测点都带有 95% 参考区间" in result.segments["risk_note"]
    assert result.meta["summary_kind"] == "forecast_timeseries"
    assert result.meta["multi_step"] is True


def test_rule_answer_generator_summarizes_weekpart_compare_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="weekpart_compare",
                selection_plan=SelectionPlan(columns=["Date", "Amount"]),
                transform_plan=TransformPlan(
                    derived_columns=[],
                    groupby=["weekpart"],
                    metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=2,
                ),
            ),
            result_df=pd.DataFrame({"weekpart": ["周末", "工作日"], "value": [280, 220]}),
            selection_plan=SelectionPlan(columns=["Date", "Amount"]),
            transform_plan=TransformPlan(
                groupby=["weekpart"],
                metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                order_by=Sort(col="value", dir="desc"),
                top_k=2,
            ),
        )
    )

    assert "周末" in result.answer
    assert "280" in result.answer
    assert "工作日" in result.analysis_text
    assert result.meta["summary_kind"] == "weekpart_compare"


def test_rule_answer_generator_summarizes_period_compare_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="period_compare",
                selection_plan=SelectionPlan(columns=["Month", "Amount"]),
                transform_plan=TransformPlan(
                    derived_columns=[],
                    groupby=["month"],
                    metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                    pivot=PivotSpec(index=[], columns="month", values="value"),
                    post_pivot_formula_metrics=[
                        FormulaMetric(as_name="change_value", op="sub", left="2025-02", right="2025-01"),
                        FormulaMetric(as_name="change_pct", op="div", left="change_value", right="2025-01"),
                    ],
                ),
                planner_meta={
                    "current_period": "2025-02",
                    "previous_period": "2025-01",
                    "compare_metric_column": "Amount",
                },
            ),
            result_df=pd.DataFrame({"2025-01": [40.0], "2025-02": [60.0], "change_value": [20.0], "change_pct": [0.5]}),
            selection_plan=SelectionPlan(columns=["Month", "Amount"]),
            transform_plan=TransformPlan(
                groupby=["month"],
                metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                pivot=PivotSpec(index=[], columns="month", values="value"),
                post_pivot_formula_metrics=[
                    FormulaMetric(as_name="change_value", op="sub", left="2025-02", right="2025-01"),
                    FormulaMetric(as_name="change_pct", op="div", left="change_value", right="2025-01"),
                ],
            ),
        )
    )

    assert "2025-02" in result.answer
    assert "50.0%" in result.answer
    assert "2025-01=40" in result.analysis_text
    assert result.meta["summary_kind"] == "period_compare"


def test_rule_answer_generator_summarizes_explain_breakdown_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="explain_breakdown",
                selection_plan=SelectionPlan(columns=["Region", "Amount"]),
                transform_plan=TransformPlan(
                    groupby=["Region"],
                    metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=5,
                ),
                planner_meta={
                    "breakdown_target_dimension": "Billing Item Name",
                    "breakdown_target_value": "Instance",
                    "breakdown_dimension": "Region",
                },
            ),
            result_df=pd.DataFrame({"Region": ["cn-sh", "cn-bj"], "value": [180.0, 40.0]}),
            selection_plan=SelectionPlan(columns=["Region", "Amount"]),
            transform_plan=TransformPlan(
                groupby=["Region"],
                metrics=[Metric(agg="sum", col="Amount", as_name="value")],
                order_by=Sort(col="value", dir="desc"),
                top_k=5,
            ),
        )
    )

    assert "Instance" in result.answer
    assert "cn-sh" in result.answer
    assert "cn-bj" in result.analysis_text
    assert result.meta["summary_kind"] == "explain_breakdown"


def test_rule_answer_generator_enriches_explain_ranked_item_summary() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="explain_ranked_item",
                selection_plan=SelectionPlan(
                    columns=["Billing Item Name", "Region", "Date", "Amount"],
                    filters=[],
                    sort=Sort(col="Amount", dir="desc"),
                    limit=5,
                ),
                transform_plan=TransformPlan(return_rows=True),
                planner_meta={
                    "explain_target_value": "Instance",
                    "explain_dimension_column": "Billing Item Name",
                },
            ),
            result_df=pd.DataFrame(
                {
                    "Billing Item Name": ["Instance", "Instance"],
                    "Region": ["cn-sh", "cn-bj"],
                    "Date": ["2025-01-03", "2025-02-03"],
                    "Amount": [120.0, 60.0],
                }
            ),
            selection_plan=SelectionPlan(
                columns=["Billing Item Name", "Region", "Date", "Amount"],
                sort=Sort(col="Amount", dir="desc"),
                limit=5,
            ),
            transform_plan=TransformPlan(return_rows=True),
        )
    )

    assert "合计 180" in result.analysis_text
    assert "时间范围覆盖 2025-01-03 到 2025-02-03" in result.analysis_text
    assert "按 Region 看，cn-sh 贡献最高" in result.analysis_text
    assert result.meta["summary_kind"] == "explain_item"


def test_rule_answer_generator_summarizes_what_if_reduction_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="zh-CN",
            draft=PlanDraft(
                mode="text",
                intent="what_if_reduction",
                selection_plan=SelectionPlan(columns=["Billing Item Name", "Amount"]),
                transform_plan=TransformPlan(
                    metrics=[Metric(agg="sum", col="Amount", as_name="matched_amount")],
                    formula_metrics=[FormulaMetric(as_name="reduction_amount", op="mul", left="matched_amount", right="0.2")],
                ),
                planner_meta={
                    "what_if_target_column": "Billing Item Name",
                    "what_if_target_value": "Instance",
                    "what_if_percent": 20,
                },
            ),
            result_df=pd.DataFrame({"matched_amount": [180.0], "reduction_amount": [36.0]}),
            selection_plan=SelectionPlan(columns=["Billing Item Name", "Amount"]),
            transform_plan=TransformPlan(
                metrics=[Metric(agg="sum", col="Amount", as_name="matched_amount")],
                formula_metrics=[FormulaMetric(as_name="reduction_amount", op="mul", left="matched_amount", right="0.2")],
            ),
        )
    )

    assert "Instance" in result.answer
    assert "36" in result.answer
    assert "180" in result.analysis_text
    assert result.meta["summary_kind"] == "what_if_reduction"


def test_rule_answer_generator_summarizes_pivot_result() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="en",
            draft=PlanDraft(
                mode="text",
                intent="custom",
                selection_plan=SelectionPlan(columns=["Region", "Month", "Revenue", "Cost"]),
                transform_plan=TransformPlan(
                    groupby=["Region", "Month"],
                    metrics=[
                        Metric(agg="sum", col="Revenue", as_name="revenue"),
                        Metric(agg="sum", col="Cost", as_name="cost"),
                    ],
                    pivot=PivotSpec(index=["Region"], columns="Month", values="profit"),
                    order_by=Sort(col="delta", dir="desc"),
                ),
            ),
            result_df=pd.DataFrame(
                {
                    "Region": ["West"],
                    "2025-01": [40.0],
                    "2025-02": [60.0],
                    "delta": [20.0],
                }
            ),
            selection_plan=SelectionPlan(columns=["Region", "Month", "Revenue", "Cost"]),
            transform_plan=TransformPlan(
                groupby=["Region", "Month"],
                metrics=[
                    Metric(agg="sum", col="Revenue", as_name="revenue"),
                    Metric(agg="sum", col="Cost", as_name="cost"),
                ],
                pivot=PivotSpec(index=["Region"], columns="Month", values="profit"),
                order_by=Sort(col="delta", dir="desc"),
            ),
        )
    )

    assert "pivoted rows" in result.answer
    assert "profit" in result.analysis_text
    assert "West" in result.analysis_text
    assert result.meta["summary_kind"] == "pivot"


def test_analyze_adds_answer_generation_pipeline_metadata() -> None:
    result = analyze(
        pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-10", "2025-02-03", "2025-02-15"],
                "Category": ["A", "B", "A", "C"],
                "Amount": [100, 50, 80, 20],
            }
        ),
        chat_text="Show the top 2 categories by amount as a bar chart.",
        requested_mode="chart",
        locale="en",
        rows_loaded=4,
    )

    assert result.pipeline["answer_generation"]["provider_used"] == "rule-v1"
    assert result.pipeline["answer_generation"]["summary_kind"] == "ranking"
    assert "A" in result.answer
    assert result.analysis_text != result.answer


class _FakeAnswerClient:
    def __init__(self) -> None:
        self.enabled = True
        self.model = "fake-answer-model"
        self.base_url = "https://example.test/v1"
        self.calls: list[dict[str, str]] = []

    def generate_json(self, schema_model, *, system_prompt: str, user_prompt: str):
        self.calls.append(
            {
                "schema": schema_model.__name__,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
            }
        )
        return schema_model(
            conclusion="A leads with 180.",
            evidence="A leads the category ranking, followed by B. A bar chart was generated.",
            risk_note="This answer is based on the current active sheet result.",
            key_points=["A=180", "B=50"],
        )


class _FailingAnswerClient:
    def __init__(self) -> None:
        self.enabled = True
        self.model = "fake-answer-model"
        self.base_url = ""

    def generate_json(self, schema_model, *, system_prompt: str, user_prompt: str):
        raise OpenAICompatibleError("answer generation failed")


def test_llm_answer_generator_uses_openai_compatible_json(monkeypatch) -> None:
    import app.services.spreadsheet.conversation.answer_generator as answer_module

    fake_client = _FakeAnswerClient()
    monkeypatch.setattr(answer_module, "build_default_llm_client", lambda: fake_client)

    generator = LLMAnswerGenerator(provider_kind="openai")
    result = generator.generate(_ranking_context())

    assert result.answer == "A leads with 180."
    assert "B" in result.analysis_text
    assert "current active sheet result" in result.analysis_text
    assert result.meta["provider_used"] == "openai-answer-v1"
    assert result.meta["fallback_used"] is False
    assert fake_client.calls[0]["schema"] == "LLMGeneratedAnswerModel"
    assert "analysis_intent={'kind': 'ranking'" in fake_client.calls[0]["user_prompt"]
    assert "reference_conclusion=A ranks first at 180." in fake_client.calls[0]["user_prompt"]
    assert "execution_disclosure=None" in fake_client.calls[0]["user_prompt"]
    assert "result_preview_csv=" in fake_client.calls[0]["user_prompt"]


def test_llm_answer_generator_falls_back_to_rule_on_error(monkeypatch) -> None:
    import app.services.spreadsheet.conversation.answer_generator as answer_module

    monkeypatch.setattr(answer_module, "build_default_llm_client", lambda: _FailingAnswerClient())

    generator = LLMAnswerGenerator(provider_kind="openai")
    result = generator.generate(_ranking_context())

    assert result.answer == "A ranks first at 180."
    assert result.meta["provider_used"] == "rule-v1"
    assert result.meta["fallback_used"] is True
    assert "answer generation failed" in result.meta["fallback_reason"]


def test_default_answer_generator_uses_llm_provider_when_requested(monkeypatch) -> None:
    import app.services.spreadsheet.conversation.answer_generator as answer_module

    monkeypatch.setenv("TALK2SHEET_ANSWER_PROVIDER", "openai")
    monkeypatch.setattr(answer_module, "build_default_llm_client", lambda: _FakeAnswerClient())
    get_settings.cache_clear()

    binding = get_default_answer_generator()

    assert binding.generator.name == "openai-answer-v1"
    assert binding.requested_provider == "openai"
    assert binding.fallback_reason == ""
    get_settings.cache_clear()


def test_analyze_uses_rule_fallback_when_llm_answer_provider_fails(monkeypatch) -> None:
    import app.services.spreadsheet.conversation.answer_generator as answer_module

    monkeypatch.setenv("TALK2SHEET_ANSWER_PROVIDER", "openai")
    monkeypatch.setattr(answer_module, "build_default_llm_client", lambda: _FailingAnswerClient())
    get_settings.cache_clear()

    result = analyze(
        pd.DataFrame(
            {
                "Date": ["2025-01-01", "2025-01-10", "2025-02-03", "2025-02-15"],
                "Category": ["A", "B", "A", "C"],
                "Amount": [100, 50, 80, 20],
            }
        ),
        chat_text="Show the top 2 categories by amount as a bar chart.",
        requested_mode="chart",
        locale="en",
        rows_loaded=4,
    )

    assert result.answer == "A ranks first at 180."
    assert result.pipeline["answer_generation"]["provider_requested"] == "openai"
    assert result.pipeline["answer_generation"]["provider_used"] == "rule-v1"
    assert result.pipeline["answer_generation"]["fallback_used"] is True
    assert "answer generation failed" in result.pipeline["answer_generation"]["fallback_reason"]
    get_settings.cache_clear()


def test_rule_answer_generator_adds_sample_scope_risk_note() -> None:
    result = RuleBasedAnswerGenerator().generate(
        AnswerGeneratorContext(
            locale="en",
            draft=PlanDraft(
                mode="text",
                intent="row_count",
                selection_plan=SelectionPlan(),
                transform_plan=TransformPlan(metrics=[Metric(agg="count_rows", as_name="row_count")]),
            ),
            result_df=pd.DataFrame({"row_count": [4]}),
            selection_plan=SelectionPlan(),
            transform_plan=TransformPlan(metrics=[Metric(agg="count_rows", as_name="row_count")]),
            execution_disclosure=ExecutionDisclosure(
                data_scope="sampled_first_n",
                exact_used=False,
                scope_text="Using the first 100 rows only.",
                fallback_reason="Exact execution was unavailable.",
                max_rows=100,
            ),
        )
    )

    assert result.answer == "There are 4 rows in the active sheet."
    assert result.segments["conclusion"] == result.answer
    assert "A direct row count was executed on the current sheet." in result.segments["evidence"]
    assert "Using the first 100 rows only." in result.segments["risk_note"]
    assert "Exact execution was unavailable." in result.analysis_text
