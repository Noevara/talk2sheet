from __future__ import annotations

from time import perf_counter
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from ..conversation.answer_generator import AnswerGeneratorContext, get_default_answer_generator
from ..core.i18n import t
from ..pipeline.column_profile import attach_column_profiles
from ..pipeline.column_profile import get_column_profiles
from ..planning.planner import get_default_planner
from ..quality.capability_guard import (
    build_feature_disabled_message,
    detect_unsupported_request,
    is_spreadsheet_feature_enabled,
)
from .response import build_clarification_payload, build_planner_payload, build_success_pipeline
from .stages import (
    run_chart_stage,
    run_exact_execution_stage,
    run_forecast_stage,
    run_selection_stage,
    run_transform_stage,
)
from .types import AnalysisPayload
from .utils import build_execution_disclosure, fallback_pipeline, has_error


def analyze(
    df: pd.DataFrame,
    *,
    chat_text: str,
    requested_mode: str,
    locale: str,
    rows_loaded: int | None = None,
    followup_context: dict[str, Any] | None = None,
    source_path: Path | None = None,
    source_sheet_index: int | None = None,
    exact_source_df_loader: Callable[[], tuple[pd.DataFrame, str]] | None = None,
) -> AnalysisPayload:
    analysis_started_at = perf_counter()
    stage_timings_ms: dict[str, float] = {}

    def record_stage(stage_name: str, started_at: float) -> None:
        stage_timings_ms[stage_name] = round((perf_counter() - started_at) * 1000, 3)

    def finalize_pipeline(
        pipeline: dict[str, Any],
        *,
        planner_provider: str = "",
        answer_provider: str = "",
        exact_used: bool | None = None,
    ) -> dict[str, Any]:
        finalized = dict(pipeline)
        finalized["observability"] = {
            "stage_timings_ms": dict(stage_timings_ms),
            "total_ms": round((perf_counter() - analysis_started_at) * 1000, 3),
            "planner_provider": planner_provider,
            "answer_provider": answer_provider,
            "exact_used": exact_used,
        }
        return finalized

    df = attach_column_profiles(df)
    rows_loaded = int(rows_loaded or len(df))

    if not is_spreadsheet_feature_enabled():
        message = build_feature_disabled_message(locale)
        return AnalysisPayload(
            mode="text",
            answer=message,
            pipeline=finalize_pipeline(fallback_pipeline(df, reason=message, code="feature_disabled")),
            execution_disclosure=build_execution_disclosure(locale, rows_loaded=rows_loaded, exact_used=False, fallback_reason=message),
        )

    unsupported = detect_unsupported_request(chat_text, locale=locale)
    if unsupported is not None:
        message = str(unsupported["message"])
        return AnalysisPayload(
            mode="text",
            answer=message,
            pipeline=finalize_pipeline(fallback_pipeline(df, reason=message, code="unsupported_capability")),
            execution_disclosure=build_execution_disclosure(locale, rows_loaded=rows_loaded, exact_used=False, fallback_reason=message),
        )

    planner_started_at = perf_counter()
    planner = get_default_planner()
    draft = planner.plan(df, chat_text=chat_text, requested_mode=requested_mode, followup_context=followup_context)
    record_stage("planner_ms", planner_started_at)
    if draft.analysis_intent is not None and draft.analysis_intent.clarification is not None:
        message = t(locale, "clarification", reason=draft.analysis_intent.clarification.reason)
        return build_clarification_payload(
            locale=locale,
            rows_loaded=rows_loaded,
            exact_used=False,
            message=message,
            pipeline=finalize_pipeline(
                {
                    "status": "clarification",
                    "clarification_stage": "intent",
                    "planner": build_planner_payload(planner=planner, draft=draft),
                    "clarification": draft.analysis_intent.clarification.model_dump(),
                    "column_profiles": list(get_column_profiles(df).values()),
                },
                planner_provider=planner.name,
                exact_used=False,
            ),
        )

    selection_started_at = perf_counter()
    selection_stage = run_selection_stage(df, draft, chat_text=chat_text)
    record_stage("selection_ms", selection_started_at)
    if has_error(selection_stage.selection_issues):
        message = t(
            locale,
            "clarification",
            reason=selection_stage.clarification.reason if selection_stage.clarification else "",
        )
        if not selection_stage.clarification:
            from ..quality.validator import summarize_issues

            message = t(locale, "clarification", reason=summarize_issues(selection_stage.selection_issues))
        return build_clarification_payload(
            locale=locale,
            rows_loaded=rows_loaded,
            exact_used=False,
            message=message,
            pipeline=finalize_pipeline(
                {
                    "status": "clarification",
                    "planner": build_planner_payload(planner=planner, draft=draft),
                    "selection_plan": selection_stage.selection_plan.model_dump(),
                    "selection_validation": selection_stage.selection_issues,
                    "selection_repair": selection_stage.selection_repair or {"changes": []},
                    "clarification": selection_stage.clarification.model_dump() if selection_stage.clarification else None,
                    "column_profiles": list(get_column_profiles(df).values()),
                },
                planner_provider=planner.name,
                exact_used=False,
            ),
        )

    assert selection_stage.selected_df is not None
    assert selection_stage.selection_meta is not None

    transform_started_at = perf_counter()
    transform_stage = run_transform_stage(selection_stage.selected_df, draft, chat_text=chat_text)
    record_stage("transform_ms", transform_started_at)
    if has_error(transform_stage.transform_issues):
        from ..quality.validator import summarize_issues

        message = t(locale, "clarification", reason=summarize_issues(transform_stage.transform_issues))
        return build_clarification_payload(
            locale=locale,
            rows_loaded=rows_loaded,
            exact_used=False,
            message=message,
            pipeline=finalize_pipeline(
                {
                    "status": "clarification",
                    "planner": build_planner_payload(planner=planner, draft=draft),
                    "selection_plan": selection_stage.selection_plan.model_dump(),
                    "transform_plan": transform_stage.transform_plan.model_dump(),
                    "selection_meta": selection_stage.selection_meta,
                    "transform_validation": transform_stage.transform_issues,
                    "transform_repair": transform_stage.transform_repair or {"changes": []},
                    "column_profiles": list(get_column_profiles(df).values()),
                },
                planner_provider=planner.name,
                exact_used=False,
            ),
        )

    execution_started_at = perf_counter()
    execution_stage = run_exact_execution_stage(
        df,
        selection_stage.selected_df,
        selection_plan=selection_stage.selection_plan,
        transform_plan=transform_stage.transform_plan,
        source_path=source_path,
        source_sheet_index=source_sheet_index,
        exact_source_df_loader=exact_source_df_loader,
    )
    record_stage("execution_ms", execution_started_at)
    if not execution_stage.exact_used:
        execution_stage.selection_meta = selection_stage.selection_meta

    forecast_started_at = perf_counter()
    result_df, transform_meta, forecast_error = run_forecast_stage(
        execution_stage.result_df,
        execution_stage.transform_meta,
        draft,
        locale=locale,
    )
    record_stage("forecast_ms", forecast_started_at)
    if forecast_error is not None or result_df is None:
        message = str(forecast_error or t(locale, "forecast_unavailable", reason="unknown"))
        return AnalysisPayload(
            mode="text",
            answer=message,
            pipeline=finalize_pipeline(
                fallback_pipeline(df, reason=message, code="forecast_unavailable"),
                planner_provider=planner.name,
                exact_used=execution_stage.exact_used,
            ),
            execution_disclosure=build_execution_disclosure(
                locale,
                rows_loaded=rows_loaded,
                exact_used=execution_stage.exact_used,
                fallback_reason=message,
            ),
        )
    execution_stage.result_df = result_df
    execution_stage.transform_meta = transform_meta

    chart_started_at = perf_counter()
    chart_stage = run_chart_stage(execution_stage.result_df, draft, chat_text=chat_text)
    record_stage("chart_ms", chart_started_at)

    execution_stage.transform_meta = {
        **execution_stage.transform_meta,
        **({"chart_runtime": chart_stage.chart_context} if chart_stage.chart_context else {}),
    }

    fallback_reasons: list[str] = [
        *[str(item) for item in (execution_stage.exact_support.get("reasons") or [])],
        *([str(execution_stage.exact_support.get("fallback_reason"))] if execution_stage.exact_support.get("fallback_reason") else []),
    ]
    if draft.mode == "chart" and chart_stage.chart_spec is None:
        chart_reason = str(chart_stage.chart_context.get("fallback_reason") or "chart rendering failed")
        fallback_reasons.append(t(locale, "chart_unavailable", reason=chart_reason))

    execution_disclosure = build_execution_disclosure(
        locale,
        rows_loaded=rows_loaded,
        exact_used=execution_stage.exact_used,
        fallback_reason=", ".join([reason for reason in fallback_reasons if reason]),
    )

    answer_started_at = perf_counter()
    answer_binding = get_default_answer_generator()
    answer_output = answer_binding.generator.generate(
        AnswerGeneratorContext(
            locale=locale,
            draft=draft,
            result_df=execution_stage.result_df,
            selection_plan=selection_stage.selection_plan,
            transform_plan=transform_stage.transform_plan,
            selection_meta=execution_stage.selection_meta,
            transform_meta=execution_stage.transform_meta,
            chart_spec=chart_stage.chart_spec,
            followup_context=followup_context,
            execution_disclosure=execution_disclosure,
            chat_text=chat_text,
        )
    )
    record_stage("answer_generation_ms", answer_started_at)
    answer = answer_output.answer
    analysis_text = answer_output.analysis_text

    answer_generation_meta = {
        "provider_requested": answer_binding.requested_provider,
        **({"binding_fallback_reason": answer_binding.fallback_reason} if answer_binding.fallback_reason else {}),
        **answer_output.meta,
    }
    answer_generation_meta.setdefault("provider_used", answer_binding.generator.name)

    pipeline = build_success_pipeline(
        df,
        execution_stage.result_df,
        planner=planner,
        draft=draft,
        selection_stage=selection_stage,
        transform_stage=transform_stage,
        execution_stage=execution_stage,
        chart_stage=chart_stage,
        answer_generation_meta=answer_generation_meta,
        followup_context=followup_context,
    )
    pipeline = finalize_pipeline(
        pipeline,
        planner_provider=planner.name,
        answer_provider=str(answer_generation_meta.get("provider_used") or answer_binding.generator.name),
        exact_used=execution_stage.exact_used,
    )

    return AnalysisPayload(
        mode=draft.mode,
        answer=answer,
        analysis_text=analysis_text or answer,
        pipeline=pipeline,
        execution_disclosure=execution_disclosure,
        chart_spec=chart_stage.chart_spec.model_dump() if chart_stage.chart_spec is not None else None,
        chart_data=chart_stage.chart_data,
    )


__all__ = [
    "AnalysisPayload",
    "analyze",
    "get_default_planner",
]
