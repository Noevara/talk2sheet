from __future__ import annotations

import re
from typing import Any

from ..core.schema import Clarification, TransformPlan
from ..execution.executor import apply_transform, resolve_column_reference
from ..pipeline.column_profile import get_column_profiles


_NUMERIC_LITERAL_RE = re.compile(r"-?\d+(?:\.\d+)?")


def issue(
    kind: str,
    message: str,
    *,
    severity: str = "error",
    field: str | None = None,
    requested: str | None = None,
    resolved: str | None = None,
    candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload = {"kind": kind, "message": message, "severity": severity}
    if field is not None:
        payload["field"] = field
    if requested is not None:
        payload["requested"] = requested
    if resolved is not None:
        payload["resolved"] = resolved
    if candidates is not None:
        payload["candidates"] = candidates
    return payload


def summarize_issues(issues: list[dict[str, Any]]) -> str:
    if not issues:
        return ""
    return "\n".join(f"- {item.get('field') or item.get('kind')}: {item.get('message')}" for item in issues[:8])


def build_clarification(issues: list[dict[str, Any]]) -> Clarification | None:
    for issue_item in issues:
        if issue_item.get("kind") != "ambiguous_column":
            continue
        options = []
        for candidate in issue_item.get("candidates") or []:
            label = str(candidate.get("resolved") or "").strip()
            if not label:
                continue
            options.append(
                {
                    "label": label,
                    "value": label,
                    "description": f"match score {candidate.get('score')}",
                }
            )
        if options:
            requested = issue_item.get("requested") or "this field"
            return Clarification(
                reason=f"The requested field '{requested}' matches multiple columns.",
                field=str(requested),
                options=options[:3],
            )
    return None


def validate_ref_with_scope(
    requested: str,
    *,
    field: str,
    columns: list[str],
    profiles: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    result = resolve_column_reference(requested, columns, profiles=profiles)
    issues: list[dict[str, Any]] = []
    if not result.get("resolved"):
        issues.append(issue("unknown_column", f"Cannot resolve column: {requested}", field=field, requested=requested))
        return issues
    if result.get("confidence") == "low":
        issues.append(
            issue(
                "weak_column_match",
                f"Column match is too weak: {requested} -> {result.get('resolved')}",
                field=field,
                requested=requested,
                resolved=result.get("resolved"),
                candidates=result.get("candidates"),
            )
        )
    if result.get("ambiguous"):
        issues.append(
            issue(
                "ambiguous_column",
                f"Column match is ambiguous: {requested} -> {result.get('resolved')}",
                severity="warn",
                field=field,
                requested=requested,
                resolved=result.get("resolved"),
                candidates=result.get("candidates"),
            )
        )
    return issues


def validate_ref(df: Any, requested: str, *, field: str) -> list[dict[str, Any]]:
    columns = [str(column) for column in getattr(df, "columns", [])]
    profiles = get_column_profiles(df)
    return validate_ref_with_scope(requested, field=field, columns=columns, profiles=profiles)


def has_error(issues: list[dict[str, Any]]) -> bool:
    return any(str(item.get("severity") or "error") == "error" for item in issues)


def resolve_ref_name(requested: str, *, columns: list[str], profiles: dict[str, dict[str, Any]]) -> str | None:
    result = resolve_column_reference(str(requested), columns, profiles=profiles)
    resolved = str(result.get("resolved") or "")
    if not resolved or result.get("confidence") == "low":
        return None
    return resolved


def is_numeric_literal(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    return _NUMERIC_LITERAL_RE.fullmatch(str(value or "").strip().replace(",", "")) is not None


def plain_row_count_intent(question: str) -> bool:
    q = str(question or "")
    if not q.strip():
        return False
    lowered = q.lower()
    negative_tokens = [
        "distinct",
        "unique",
        "top",
        "rank",
        "trend",
        "share",
        "chart",
        "pie",
        "line",
        "bar",
        "按",
        "每",
        "排行",
        "排名",
        "趋势",
        "占比",
        "人数",
        "多少人",
        "明细",
        "records",
        "detail",
    ]
    if any(token in lowered for token in negative_tokens):
        return False
    return bool(
        re.search(r"how many rows", q, re.I)
        or re.search(r"row count", q, re.I)
        or any(token in q for token in ["多少行", "多少条", "总记录数", "件数", "几行", "几条"])
    )


def virtual_profiles(columns: list[str], base_profiles: dict[str, dict[str, Any]], semantic_types: dict[str, str]) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    for column in columns:
        profile = dict(base_profiles.get(column) or {})
        profile["name"] = column
        profile["semantic_type"] = semantic_types.get(column, str(profile.get("semantic_type") or "unknown"))
        aliases = [str(item) for item in profile.get("aliases") or [] if str(item or "").strip()]
        if column not in aliases:
            aliases.insert(0, column)
        profile["aliases"] = aliases or [column]
        profiles[column] = profile
    return profiles


def semantic_type(name: str | None, *, semantic_types: dict[str, str], base_profiles: dict[str, dict[str, Any]]) -> str:
    if not name:
        return "unknown"
    if name in semantic_types:
        return str(semantic_types.get(name) or "unknown")
    return str((base_profiles.get(name) or {}).get("semantic_type") or "unknown")


def validate_operand(
    operand: Any,
    *,
    field: str,
    columns: list[str],
    profiles: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if operand is None or str(operand).strip() == "":
        return [issue("missing_operand", "Missing operand.", field=field)]
    if is_numeric_literal(operand):
        return []
    return validate_ref_with_scope(str(operand), field=field, columns=columns, profiles=profiles)


def register_output_alias(
    alias: str,
    *,
    field: str,
    columns: list[str],
    semantic_types: dict[str, str],
    semantic_type_name: str,
) -> list[dict[str, Any]]:
    if not alias:
        return [issue("missing_alias", "Missing output alias.", field=field)]
    if alias in columns:
        return [issue("duplicate_alias", f"Duplicate output alias: {alias}", field=field, requested=alias)]
    columns.append(alias)
    semantic_types[alias] = semantic_type_name
    return []


def derived_semantic_type(kind: str, grain: str | None) -> str:
    if kind == "arithmetic":
        return "numeric"
    if grain in {"day", "week", "month", "quarter"}:
        return "date"
    return "categorical"


def try_validate_transform_runtime(df: Any, plan: TransformPlan) -> list[dict[str, Any]]:
    try:
        preview_df = df.head(500) if hasattr(df, "head") else df
        apply_transform(preview_df, plan)
        return []
    except Exception as exc:
        return [issue("transform_runtime_error", f"TransformPlan preview execution failed: {exc}", field="transform")]
