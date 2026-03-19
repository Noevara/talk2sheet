from __future__ import annotations

from typing import Any


def _safe_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def analysis_intent_payload(source: Any) -> dict[str, Any]:
    if source is None:
        return {}
    if isinstance(source, dict):
        payload = source.get("analysis_intent")
        return _safe_dict(payload)
    payload = getattr(source, "analysis_intent", None)
    if payload is None:
        return {}
    if hasattr(payload, "model_dump"):
        return _safe_dict(payload.model_dump())
    return _safe_dict(payload)


def analysis_intent_kind(source: Any, *, fallback: str = "") -> str:
    payload = analysis_intent_payload(source)
    kind = str(payload.get("kind") or payload.get("legacy_intent") or "").strip()
    if kind:
        return kind
    if isinstance(source, dict):
        return str(source.get("intent") or fallback).strip()
    return str(getattr(source, "intent", "") or fallback).strip()


def analysis_intent_target_dimension(source: Any) -> str:
    payload = analysis_intent_payload(source)
    return str(payload.get("target_dimension") or "").strip()


def analysis_intent_target_metric(source: Any) -> str:
    payload = analysis_intent_payload(source)
    return str(payload.get("target_metric") or "").strip()


def analysis_intent_time_scope(source: Any) -> dict[str, Any]:
    payload = analysis_intent_payload(source)
    return _safe_dict(payload.get("time_scope"))
