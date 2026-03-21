from __future__ import annotations

from typing import Any

import pandas as pd

from ..core.serialization import dataframe_to_rows
from ..execution.executor import detect_unique_key_candidates
from ..pipeline import HEADER_HEALTH_ATTR, HEADER_PLAN_ATTR, SHEET_NAME_ATTR
from ..pipeline.column_profile import get_column_profiles
from .types import AnalysisPayload, ChartStageResult, ExecutionStageResult, SelectionStageResult, TransformStageResult
from .utils import build_execution_disclosure


def build_planner_payload(*, planner: Any, draft: Any) -> dict[str, Any]:
    payload = {
        "provider": planner.name,
        "intent": draft.intent,
        **draft.planner_meta,
    }
    if getattr(draft, "analysis_intent", None) is not None:
        payload["analysis_intent"] = draft.analysis_intent.model_dump()
    return payload


def build_clarification_payload(
    *,
    locale: str,
    rows_loaded: int,
    exact_used: bool,
    message: str,
    pipeline: dict[str, Any],
) -> AnalysisPayload:
    return AnalysisPayload(
        mode="text",
        answer=message,
        pipeline=pipeline,
        execution_disclosure=build_execution_disclosure(locale, rows_loaded=rows_loaded, exact_used=exact_used, fallback_reason=message),
    )


def build_success_pipeline(
    df: pd.DataFrame,
    result_df: pd.DataFrame,
    *,
    planner: Any,
    draft: Any,
    selection_stage: SelectionStageResult,
    transform_stage: TransformStageResult,
    execution_stage: ExecutionStageResult,
    chart_stage: ChartStageResult,
    answer_generation_meta: dict[str, Any],
    followup_context: dict[str, Any] | None,
) -> dict[str, Any]:
    pipeline: dict[str, Any] = {
        "status": "ok",
        "planner": build_planner_payload(planner=planner, draft=draft),
        "source_sheet_name": str(df.attrs.get(SHEET_NAME_ATTR) or ""),
        "source_header_plan": df.attrs.get(HEADER_PLAN_ATTR) or {},
        "source_header_health": df.attrs.get(HEADER_HEALTH_ATTR) or {},
        "selection_plan": selection_stage.selection_plan.model_dump(),
        "selection_guardrail": selection_stage.selection_guardrail,
        "selection_validation": selection_stage.selection_issues,
        "selection_repair": selection_stage.selection_repair or {"changes": []},
        "selection_meta": selection_stage.selection_meta,
        "transform_plan": transform_stage.transform_plan.model_dump(),
        "transform_guardrail": transform_stage.transform_guardrail,
        "transform_validation": transform_stage.transform_issues,
        "transform_repair": transform_stage.transform_repair or {"changes": []},
        "transform_meta": execution_stage.transform_meta,
        "exact_execution": execution_stage.exact_support | {"used": execution_stage.exact_used, "source_context": execution_stage.exact_source},
        "column_profiles": list(get_column_profiles(df).values()),
        "unique_key_candidates": detect_unique_key_candidates(df),
        "result_columns": [str(column) for column in result_df.columns],
        "result_row_count": int(len(result_df.index)),
        "answer_generation": answer_generation_meta,
    }
    if followup_context:
        pipeline["followup_context"] = {
            "conversation_id": followup_context.get("conversation_id"),
            "turn_count": int(followup_context.get("turn_count") or 0),
            "is_followup": bool(followup_context.get("is_followup")),
            "last_mode": str(followup_context.get("last_mode") or ""),
            "last_intent": str(((followup_context.get("last_turn") or {}) if isinstance(followup_context.get("last_turn"), dict) else {}).get("intent") or ""),
            "last_pipeline_summary": followup_context.get("last_pipeline_summary") if isinstance(followup_context.get("last_pipeline_summary"), dict) else {},
        }

    if selection_stage.clarification is not None:
        pipeline["clarification"] = selection_stage.clarification.model_dump()
    if transform_stage.transform_plan.return_rows:
        pipeline["preview_rows"] = dataframe_to_rows(result_df)
    elif chart_stage.chart_spec is None and len(result_df.index) > 1:
        pipeline["preview_rows"] = dataframe_to_rows(result_df)
    if chart_stage.chart_spec is not None:
        pipeline["chart_spec"] = chart_stage.chart_spec.model_dump()
        pipeline["chart_guardrail"] = chart_stage.chart_guardrail_meta
        pipeline["chart_validation"] = chart_stage.chart_validation
        pipeline["chart_repair"] = chart_stage.chart_repair_meta
    if chart_stage.chart_context:
        pipeline["chart_context"] = chart_stage.chart_context
    return pipeline
