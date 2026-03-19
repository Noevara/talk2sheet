from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from ..core.schema import ChartSpec, SelectionPlan, TransformPlan
from ..planning.planner_guardrails import (
    sanitize_chart_spec,
    sanitize_selection_plan,
    sanitize_transform_plan,
)
from .repair import llm_repair_plan, repair_plan
from .validator import validate_chart_spec, validate_selection_plan, validate_transform_plan


@dataclass(frozen=True)
class GovernancePolicy:
    name: str
    enable_rule_repair: bool = False
    enable_llm_repair: bool = False


@dataclass
class GovernanceResult:
    plan: BaseModel
    policy_name: str
    guardrail: dict[str, Any]
    issues: list[dict[str, Any]]
    repair: dict[str, Any]


PLANNER_LIGHT_POLICY = GovernancePolicy(name="planner_light", enable_rule_repair=False, enable_llm_repair=False)
ANALYSIS_FULL_POLICY = GovernancePolicy(name="analysis_full", enable_rule_repair=True, enable_llm_repair=True)


def has_error(issues: list[dict[str, Any]]) -> bool:
    return any(str(item.get("severity") or "error") == "error" for item in issues)


def build_governance_meta(result: GovernanceResult) -> dict[str, Any]:
    return {
        "policy": result.policy_name,
        "guardrail": result.guardrail,
        "validation": result.issues,
        "repair": result.repair,
    }


def govern_plan(
    df: Any,
    plan: BaseModel,
    *,
    question: str,
    mode: str,
    policy: GovernancePolicy,
) -> GovernanceResult:
    current_plan, guardrail_meta = _sanitize_plan(plan, question=question, mode=mode, df=df)
    issues = _validate_plan(df, current_plan, question=question, mode=mode)
    repair_meta = _default_repair_meta(policy)
    plan_type = type(current_plan)

    if has_error(issues) and policy.enable_rule_repair:
        repaired, rule_meta = repair_plan(df, current_plan, question=question, mode=mode)
        repair_meta["rule"] = {
            "used": bool((rule_meta or {}).get("changes")),
            **(rule_meta or {"changes": []}),
        }
        if isinstance(repaired, plan_type):
            current_plan, repaired_guardrail = _sanitize_plan(repaired, question=question, mode=mode, df=df)
            guardrail_meta = _merge_guardrail_meta(guardrail_meta, repaired_guardrail)
            issues = _validate_plan(df, current_plan, question=question, mode=mode)

            if has_error(issues) and policy.enable_llm_repair:
                llm_repaired, llm_meta = llm_repair_plan(
                    df,
                    current_plan,
                    question=question,
                    mode=mode,
                    issues=issues,
                )
                repair_meta["llm"] = llm_meta
                if isinstance(llm_repaired, plan_type):
                    current_plan, llm_guardrail = _sanitize_plan(llm_repaired, question=question, mode=mode, df=df)
                    guardrail_meta = _merge_guardrail_meta(guardrail_meta, llm_guardrail)
                    issues = _validate_plan(df, current_plan, question=question, mode=mode)

    return GovernanceResult(
        plan=current_plan,
        policy_name=policy.name,
        guardrail=guardrail_meta,
        issues=issues,
        repair=repair_meta,
    )


def _default_repair_meta(policy: GovernancePolicy) -> dict[str, Any]:
    return {
        "policy": policy.name,
        "rule": {
            "used": False,
            "changes": [],
            "reason": "" if policy.enable_rule_repair else "disabled_by_policy",
        },
        "llm": {
            "used": False,
            "reason": "" if policy.enable_llm_repair else "disabled_by_policy",
        },
    }


def _merge_guardrail_meta(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    return {
        **(left or {}),
        **(right or {}),
        "changes": [*((left or {}).get("changes") or []), *((right or {}).get("changes") or [])],
    }


def _validate_plan(
    df: Any,
    plan: BaseModel,
    *,
    question: str,
    mode: str,
) -> list[dict[str, Any]]:
    if isinstance(plan, SelectionPlan):
        return validate_selection_plan(df, plan, question=question, mode=mode)
    if isinstance(plan, TransformPlan):
        return validate_transform_plan(df, plan, question=question, mode=mode)
    if isinstance(plan, ChartSpec):
        return validate_chart_spec(df, plan)
    return []


def _sanitize_plan(
    plan: BaseModel,
    *,
    question: str,
    mode: str,
    df: Any,
) -> tuple[BaseModel, dict[str, Any]]:
    if isinstance(plan, SelectionPlan):
        sanitized, meta = sanitize_selection_plan(plan, question, mode=mode)
        return sanitized, meta
    if isinstance(plan, TransformPlan):
        sanitized, meta = sanitize_transform_plan(plan, question, df, mode=mode)
        return sanitized, meta
    if isinstance(plan, ChartSpec):
        sanitized, meta = sanitize_chart_spec(plan, question, df)
        return sanitized, meta
    return plan, {"changes": []}
