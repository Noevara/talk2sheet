from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from app.config import get_settings
from ..core.schema import ChartSpec, SelectionPlan, TransformPlan
from ..openai_compatible import OpenAICompatibleError, build_default_llm_client
from ..pipeline.column_profile import get_column_profiles
from .repair_chart_rules import repair_chart_spec
from .repair_selection_rules import repair_selection_plan
from .repair_transform_rules import repair_transform_plan
from .validator import summarize_issues


def repair_plan(
    df: Any,
    plan: BaseModel,
    *,
    question: str,
    mode: str,
) -> tuple[BaseModel | None, dict[str, Any]]:
    if isinstance(plan, SelectionPlan):
        repaired, meta = repair_selection_plan(df, plan, question=question, mode=mode)
        return repaired, meta
    if isinstance(plan, TransformPlan):
        repaired, meta = repair_transform_plan(df, plan, question=question, mode=mode)
        return repaired, meta
    if isinstance(plan, ChartSpec):
        repaired, meta = repair_chart_spec(df, plan, question=question)
        return repaired, meta
    return None, {"changes": []}


def llm_repair_plan(
    df: Any,
    plan: BaseModel,
    *,
    question: str,
    mode: str,
    issues: list[dict[str, Any]],
) -> tuple[BaseModel | None, dict[str, Any]]:
    settings = get_settings()
    provider = str(settings.repair_provider or "auto").strip().lower()
    if provider not in {"auto", "openai", "ark"}:
        return None, {"used": False, "reason": "llm_repair_disabled"}

    client = build_default_llm_client()
    if not client.enabled:
        return None, {"used": False, "reason": "llm_repair_not_configured"}

    schema_model = type(plan)
    system_prompt = (
        "You are a spreadsheet plan repairer. "
        "Return JSON only and follow the target schema exactly. "
        "Repair invalid, ambiguous, or unreasonable parts of the plan. "
        "Do not invent columns that are not present in context."
    )
    user_prompt = (
        f"mode={mode}\n"
        f"question={question}\n"
        f"current_plan={plan.model_dump()}\n"
        f"issues=\n{summarize_issues(issues)}\n"
        f"columns={list(df.columns)}\n"
        f"column_profiles={list(get_column_profiles(df).values())}\n"
        f"sample_rows={df.head(5).fillna('').astype(str).to_dict(orient='records')}\n"
        "Return the repaired JSON object only."
    )

    try:
        repaired = client.generate_json(
            schema_model,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        return repaired, {"used": True, "provider": "openai-compatible"}
    except (OpenAICompatibleError, Exception) as exc:
        return None, {"used": False, "reason": str(exc)}


__all__ = [
    "llm_repair_plan",
    "repair_chart_spec",
    "repair_plan",
    "repair_selection_plan",
    "repair_transform_plan",
]
