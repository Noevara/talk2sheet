from __future__ import annotations

from typing import Any

from ..pipeline.column_profile import get_column_profiles, infer_semantic_hints, normalize_text


def _norm(text: str) -> str:
    return normalize_text(text)


def _overlap_score(left: str, right: str) -> int:
    a = _norm(left)
    b = _norm(right)
    if not a or not b:
        return 0
    return sum(1 for ch in set(a) if ch in b)


def resolve_column_reference(
    column: str,
    columns: list[str],
    *,
    profiles: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    profiles = profiles or {}
    target = _norm(column)
    if not target:
        return {
            "requested": column,
            "resolved": None,
            "score": 0,
            "confidence": "low",
            "match_type": "empty",
            "ambiguous": False,
            "candidates": [],
        }

    scored: list[dict[str, Any]] = []
    target_hints = set(infer_semantic_hints(column))
    for candidate in [str(item) for item in columns]:
        profile = profiles.get(candidate) or {}
        aliases = [str(item) for item in profile.get("aliases") or [] if str(item or "").strip()]
        alias_norms = {_norm(alias) for alias in aliases}
        candidate_norm = _norm(candidate)
        score = 0
        match_type = "fuzzy"

        if column == candidate:
            score = 100
            match_type = "exact"
        elif target == candidate_norm:
            score = 96
            match_type = "normalized"
        elif target in alias_norms:
            score = 92
            match_type = "alias"
        else:
            score = min(88, _overlap_score(target, candidate_norm) * 10)
            if target and (target in candidate_norm or candidate_norm in target):
                score += 28
            candidate_hints = set(str(item) for item in profile.get("semantic_hints") or [])
            if target_hints & candidate_hints:
                score += 16
            score = min(score, 90)

        scored.append(
            {
                "requested": column,
                "resolved": candidate,
                "score": score,
                "match_type": match_type,
                "semantic_type": profile.get("semantic_type"),
            }
        )

    if not scored:
        return {
            "requested": column,
            "resolved": None,
            "score": 0,
            "confidence": "low",
            "match_type": "missing",
            "ambiguous": False,
            "candidates": [],
        }

    scored.sort(key=lambda item: (int(item["score"]), str(item["resolved"])), reverse=True)
    best = scored[0]
    best_score = int(best["score"])
    confidence = "high" if best_score >= 90 else "medium" if best_score >= 60 else "low"
    ambiguous = len(scored) > 1 and best_score >= 60 and (best_score - int(scored[1]["score"])) <= 4
    return {
        **best,
        "confidence": confidence,
        "ambiguous": ambiguous,
        "candidates": scored[:5],
    }


def pick_close_column(column: str, columns: list[str], *, profiles: dict[str, dict[str, Any]] | None = None) -> str:
    result = resolve_column_reference(column, columns, profiles=profiles)
    resolved = result.get("resolved")
    if not resolved or result.get("confidence") == "low":
        raise ValueError(f"Unknown column: {column}")
    return str(resolved)


def ensure_columns_exist(df: Any, columns: list[str]) -> list[str]:
    profiles = get_column_profiles(df)
    return [pick_close_column(column, list(df.columns), profiles=profiles) for column in columns]


def detect_unique_key_candidates(df: Any, max_candidates: int = 5) -> list[dict[str, Any]]:
    total_rows = len(df) if len(df) else 1
    candidates: list[dict[str, Any]] = []
    for column in list(df.columns):
        name = str(column)
        hint = any(token in name.lower() for token in ["id", "编号", "工号", "邮箱", "email", "单号"])
        try:
            unique_count = int(df[column].nunique(dropna=True))
        except Exception:
            continue
        ratio = unique_count / total_rows
        score = (2.0 if hint else 0.0) + ratio
        candidates.append({"col": name, "nunique": unique_count, "ratio": round(ratio, 4), "score": round(score, 4)})
    candidates.sort(key=lambda item: item["score"], reverse=True)
    return candidates[:max_candidates]
