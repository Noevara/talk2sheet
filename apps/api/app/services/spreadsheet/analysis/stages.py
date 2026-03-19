from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import pandas as pd

from ..core.i18n import t
from ..core.schema import ChartSpec, SelectionPlan, TransformPlan
from ..execution.exact_executor import (
    exact_execution_source_context,
    exact_execution_support,
    execute_exact_plan,
    execute_exact_plan_from_source,
    execute_exact_plan_with_source_df,
)
from ..execution.executor import apply_selection, apply_transform
from ..execution.forecast_executor import forecast_time_series
from ..pipeline import HEADER_PLAN_ATTR
from ..pipeline.column_profile import attach_column_profiles
from ..quality.policy import ANALYSIS_FULL_POLICY, govern_plan
from ..quality.validator import build_clarification
from .types import (
    ChartStageResult,
    ExecutionStageResult,
    SelectionStageResult,
    TransformStageResult,
)
from .utils import has_error


def run_selection_stage(df: pd.DataFrame, draft: Any, *, chat_text: str) -> SelectionStageResult:
    governance = govern_plan(
        df,
        draft.selection_plan,
        question=chat_text,
        mode=draft.mode,
        policy=ANALYSIS_FULL_POLICY,
    )
    selection_plan = governance.plan
    assert isinstance(selection_plan, SelectionPlan)
    clarification = build_clarification(governance.issues)
    if has_error(governance.issues):
        return SelectionStageResult(
            selection_plan=selection_plan,
            selection_guardrail=governance.guardrail,
            selection_issues=governance.issues,
            selection_repair=governance.repair,
            clarification=clarification,
        )

    selected_df, selection_meta = apply_selection(df, selection_plan)
    return SelectionStageResult(
        selection_plan=selection_plan,
        selection_guardrail=governance.guardrail,
        selection_issues=governance.issues,
        selection_repair=governance.repair,
        clarification=clarification,
        selected_df=selected_df,
        selection_meta=selection_meta,
    )


def run_transform_stage(selected_df: pd.DataFrame, draft: Any, *, chat_text: str) -> TransformStageResult:
    governance = govern_plan(
        selected_df,
        draft.transform_plan,
        question=chat_text,
        mode=draft.mode,
        policy=ANALYSIS_FULL_POLICY,
    )
    transform_plan = governance.plan
    assert isinstance(transform_plan, TransformPlan)
    return TransformStageResult(
        transform_plan=transform_plan,
        transform_guardrail=governance.guardrail,
        transform_issues=governance.issues,
        transform_repair=governance.repair,
    )


def run_exact_execution_stage(
    df: pd.DataFrame,
    selected_df: pd.DataFrame,
    *,
    selection_plan: SelectionPlan,
    transform_plan: TransformPlan,
    source_path: Path | None,
    source_sheet_index: int | None,
    exact_source_df_loader: Callable[[], tuple[pd.DataFrame, str]] | None,
) -> ExecutionStageResult:
    exact_support = exact_execution_support(selection_plan, transform_plan)
    exact_source = exact_execution_source_context(df)

    if exact_support.get("eligible"):
        try:
            if source_path is not None and exact_source_df_loader is not None:
                source_df, source_sheet_name = exact_source_df_loader()
                result_df, exact_meta = execute_exact_plan_with_source_df(
                    source_df=source_df,
                    source_sheet_name=source_sheet_name,
                    source_path=source_path,
                    source_sheet_index=int(source_sheet_index or 1),
                    selection_plan=selection_plan,
                    transform_plan=transform_plan,
                )
            elif source_path is not None:
                result_df, exact_meta = execute_exact_plan_from_source(
                    source_path=source_path,
                    source_sheet_index=int(source_sheet_index or 1),
                    selection_plan=selection_plan,
                    transform_plan=transform_plan,
                    header_plan=df.attrs.get(HEADER_PLAN_ATTR) or None,
                )
            elif exact_source.get("available"):
                result_df, exact_meta = execute_exact_plan_from_source(
                    source_path=Path(str(exact_source["source_path"])),
                    source_sheet_index=int(exact_source.get("source_sheet_index") or 1),
                    selection_plan=selection_plan,
                    transform_plan=transform_plan,
                    header_plan=exact_source.get("header_plan") or None,
                )
            else:
                result_df, exact_meta = execute_exact_plan(df, selection_plan, transform_plan)
            return ExecutionStageResult(
                result_df=attach_column_profiles(result_df),
                selection_meta=exact_meta["selection_meta"],
                transform_meta=exact_meta["transform_meta"],
                exact_used=True,
                exact_support=exact_support,
                exact_source=exact_source,
            )
        except Exception as exc:
            result_df, transform_meta = apply_transform(selected_df, transform_plan)
            return ExecutionStageResult(
                result_df=attach_column_profiles(result_df),
                selection_meta={},
                transform_meta=transform_meta,
                exact_used=False,
                exact_support={**exact_support, "fallback_reason": str(exc), "source_context": exact_source},
                exact_source=exact_source,
            )

    result_df, transform_meta = apply_transform(selected_df, transform_plan)
    return ExecutionStageResult(
        result_df=attach_column_profiles(result_df),
        selection_meta={},
        transform_meta=transform_meta,
        exact_used=False,
        exact_support=exact_support,
        exact_source=exact_source,
    )


def run_forecast_stage(
    result_df: pd.DataFrame,
    transform_meta: dict[str, Any],
    draft: Any,
    *,
    locale: str,
) -> tuple[pd.DataFrame | None, dict[str, Any], str | None]:
    if draft.intent != "forecast_timeseries":
        return result_df, transform_meta, None

    period_column = str(draft.planner_meta.get("bucket_name") or "")
    target_period = str(draft.planner_meta.get("forecast_target_period") or "")
    target_periods = [str(item) for item in (draft.planner_meta.get("forecast_target_periods") or []) if str(item or "").strip()]
    forecast_grain = str(draft.planner_meta.get("forecast_grain") or "")
    if not period_column or not target_period or not forecast_grain:
        return None, transform_meta, t(locale, "forecast_unavailable", reason="missing forecast planning context")

    try:
        forecast_df, forecast_meta = forecast_time_series(
            result_df,
            period_column=period_column,
            value_column="value",
            target_period=target_period,
            target_periods=target_periods or None,
            grain=forecast_grain,
        )
    except Exception as exc:
        return None, transform_meta, t(locale, "forecast_unavailable", reason=str(exc))

    return attach_column_profiles(forecast_df), {**transform_meta, "forecast": forecast_meta}, None


def run_chart_stage(result_df: pd.DataFrame, draft: Any, *, chat_text: str) -> ChartStageResult:
    chart_spec: ChartSpec | None = draft.chart_spec if draft.mode == "chart" and draft.chart_spec else None
    chart_guardrail_meta: dict[str, Any] = {"changes": []}
    chart_validation: list[dict[str, Any]] = []
    chart_repair_meta: dict[str, Any] = {"changes": []}
    chart_data: list[dict[str, Any]] | None = None

    if chart_spec is None:
        return ChartStageResult(
            chart_spec=None,
            chart_guardrail_meta=chart_guardrail_meta,
            chart_validation=chart_validation,
            chart_repair_meta=chart_repair_meta,
            chart_data=None,
        )

    governance = govern_plan(
        result_df,
        chart_spec,
        question=chat_text,
        mode=draft.mode,
        policy=ANALYSIS_FULL_POLICY,
    )
    chart_spec = governance.plan
    assert isinstance(chart_spec, ChartSpec)
    chart_guardrail_meta = governance.guardrail
    chart_validation = governance.issues
    chart_repair_meta = governance.repair

    if not has_error(chart_validation):
        from ..core.serialization import dataframe_to_records

        chart_limit = chart_spec.top_k if chart_spec.top_k is not None else len(result_df.index)
        chart_data = dataframe_to_records(result_df.head(chart_limit))
    else:
        chart_spec = None

    return ChartStageResult(
        chart_spec=chart_spec,
        chart_guardrail_meta=chart_guardrail_meta,
        chart_validation=chart_validation,
        chart_repair_meta=chart_repair_meta,
        chart_data=chart_data,
    )
