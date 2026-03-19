from __future__ import annotations

import re
from typing import Any

from ..pipeline.column_profile import normalize_text
from ..execution.executor import detect_unique_key_candidates
from .planner_text_utils import _contains_any


def _find_amount_column(profiles: dict[str, dict[str, Any]]) -> str | None:
    ambiguous_tokens = ("type", "category", "label", "desc", "description", "note", "类型", "分类", "类别", "说明", "备注", "名称")
    weighted_name_tokens = (
        ("amountdue", 8),
        ("payable", 8),
        ("actualamount", 8),
        ("应付金额", 10),
        ("实付金额", 10),
        ("支付金额", 8),
        ("优惠后金额", 7),
        ("含税", 3),
        ("总金额", 6),
        ("金额", 4),
        ("总额", 4),
        ("合计", 4),
        ("totalamount", 5),
        ("subtotal", 3),
        ("amount", 3),
        ("payment", 3),
        ("charge", 3),
        ("price", 2),
        ("cost", 2),
        ("总价", 1),
        ("目录总价", -4),
        ("目录价", -4),
        ("官网目录价", -6),
        ("优惠金额", -5),
        ("抵扣金额", -4),
        ("抵扣后金额", -3),
    )

    def score(column: str, profile: dict[str, Any]) -> tuple[int, float]:
        hints = {str(item) for item in (profile.get("semantic_hints") or [])}
        semantic_type = str(profile.get("semantic_type") or "")
        numeric_ratio = float(profile.get("numeric_ratio") or 0.0)
        normalized_name = normalize_text(column)

        evidence = 0
        if semantic_type == "date":
            evidence -= 8
        if semantic_type == "numeric":
            evidence += 4
        if numeric_ratio >= 0.9:
            evidence += 4
        elif numeric_ratio >= 0.6:
            evidence += 3
        elif numeric_ratio >= 0.3:
            evidence += 2
        elif numeric_ratio > 0:
            evidence += 1

        if "amount" in hints:
            evidence += 4
        for token, weight in weighted_name_tokens:
            if token in normalized_name:
                evidence += weight
        if any(token in normalized_name for token in ambiguous_tokens):
            evidence -= 5

        return evidence, numeric_ratio

    candidates: list[tuple[int, float, str]] = []
    numeric_fallbacks: list[tuple[int, float, str]] = []
    for column, profile in profiles.items():
        evidence, numeric_ratio = score(column, profile)
        if evidence > 0 and ("amount" in (profile.get("semantic_hints") or []) or numeric_ratio > 0):
            candidates.append((evidence, numeric_ratio, column))
        if str(profile.get("semantic_type") or "") == "numeric" or numeric_ratio > 0:
            numeric_fallbacks.append((evidence, numeric_ratio, column))

    if candidates:
        candidates.sort(key=lambda item: (item[0], item[1], -len(item[2])), reverse=True)
        return candidates[0][2]
    if numeric_fallbacks:
        numeric_fallbacks.sort(key=lambda item: (item[1], item[0], -len(item[2])), reverse=True)
        return numeric_fallbacks[0][2]
    return None


def _find_date_column(profiles: dict[str, dict[str, Any]]) -> str | None:
    candidates: list[tuple[int, int, str]] = []
    for column, profile in profiles.items():
        hints = {str(item) for item in (profile.get("semantic_hints") or [])}
        semantic_type = str(profile.get("semantic_type") or "")
        if "date" not in hints and semantic_type != "date":
            continue

        normalized = normalize_text(column)
        samples = [str(item or "").strip() for item in (profile.get("sample_values") or []) if str(item or "").strip()]
        score = 0

        if semantic_type == "date":
            score += 20
        if "date" in hints:
            score += 12

        if any(token in normalized for token in ("日期", "date", "day", "时间", "time", "发生", "开始", "结束")):
            score += 36
        if any(token in normalized for token in ("月份", "month", "年月", "monthbucket")):
            score -= 28

        if any(re.fullmatch(r"\d{8}(\d{4}(\d{2})?)?", re.sub(r"\.0+$", "", sample).replace(" ", "")) for sample in samples):
            score += 24
        if any(re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", sample) for sample in samples):
            score += 24
        if any(re.fullmatch(r"\d{6}", re.sub(r"\.0+$", "", sample).replace(" ", "")) for sample in samples):
            score -= 16
        if any(re.fullmatch(r"\d{4}[-/]\d{1,2}", sample) for sample in samples):
            score -= 16

        candidates.append((score, -len(column), column))

    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2]


def _find_category_column(
    profiles: dict[str, dict[str, Any]],
    *,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()
    for column, profile in profiles.items():
        if column in exclude:
            continue
        if profile.get("semantic_type") in {"categorical", "text"}:
            return column
    for column in profiles:
        if column not in exclude:
            return column
    return None


def _find_single_transaction_group_column(
    df: Any,
    profiles: dict[str, dict[str, Any]],
    *,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()
    unique_candidates = detect_unique_key_candidates(df)
    for candidate in unique_candidates:
        column = str(candidate.get("col") or "")
        ratio = float(candidate.get("ratio") or 0.0)
        if column and column not in exclude and ratio >= 0.95:
            return column

    preferred_name_tokens = ("resource", "instance", "order", "bill", "name", "title", "资源", "实例", "订单", "账单", "名称")
    preferred_date_tokens = ("date", "time", "日期", "时间")
    for column, profile in profiles.items():
        if column in exclude:
            continue
        normalized = normalize_text(column)
        if any(token in normalized for token in preferred_name_tokens):
            return column
        if profile.get("semantic_type") in {"id", "categorical", "text"}:
            return column
    for column, profile in profiles.items():
        if column in exclude:
            continue
        normalized = normalize_text(column)
        if any(token in normalized for token in preferred_date_tokens):
            return column
        if profile.get("semantic_type") == "date":
            return column
    for column in profiles:
        if column not in exclude:
            return column
    return None


def _find_item_column(
    profiles: dict[str, dict[str, Any]],
    *,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()
    weighted_name_tokens = (
        ("itemname", 13),
        ("item", 11),
        ("项名称", 12),
        ("消费项", 12),
        ("计费项名称", 12),
        ("billingitemname", 12),
        ("商品名称", 10),
        ("productname", 9),
        ("产品名称", 8),
        ("资源名称", 7),
        ("resourcename", 7),
        ("费用类型", 5),
        ("category", 4),
        ("类别", 4),
        ("类型", 3),
        ("名称", 2),
    )

    candidates: list[tuple[int, float, str]] = []
    for column, profile in profiles.items():
        if column in exclude:
            continue
        semantic_type = str(profile.get("semantic_type") or "")
        if semantic_type not in {"categorical", "text"}:
            continue
        unique_ratio = float(profile.get("unique_ratio") or 0.0)
        normalized = normalize_text(column)
        score = 0
        for token, weight in weighted_name_tokens:
            if token in normalized:
                score += weight
        if 0.0001 < unique_ratio <= 0.5:
            score += 2
        if score > 0:
            candidates.append((score, -unique_ratio, column))

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][2]
    return _find_category_column(profiles, exclude=exclude)


def _find_service_column(
    profiles: dict[str, dict[str, Any]],
    *,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()
    weighted_name_tokens = (
        ("servicename", 14),
        ("service", 12),
        ("服务名称", 14),
        ("服务主体", 12),
        ("产品名称", 12),
        ("商品名称", 10),
        ("productname", 10),
        ("product", 8),
        ("产品", 7),
        ("服务", 7),
    )

    candidates: list[tuple[int, float, str]] = []
    for column, profile in profiles.items():
        if column in exclude:
            continue
        semantic_type = str(profile.get("semantic_type") or "")
        if semantic_type not in {"categorical", "text", "id"}:
            continue
        unique_ratio = float(profile.get("unique_ratio") or 0.0)
        normalized = normalize_text(column)
        score = 0
        for token, weight in weighted_name_tokens:
            if token in normalized:
                score += weight
        if 0.0001 < unique_ratio <= 0.7:
            score += 2
        if score > 0:
            candidates.append((score, -unique_ratio, column))

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][2]
    return _find_category_column(profiles, exclude=exclude)


def _find_region_column(
    profiles: dict[str, dict[str, Any]],
    *,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()
    weighted_name_tokens = (
        ("region", 14),
        ("location", 10),
        ("地域", 15),
        ("地区", 13),
        ("区域", 12),
        ("城市", 10),
        ("可用区", 9),
    )

    candidates: list[tuple[int, float, str]] = []
    for column, profile in profiles.items():
        if column in exclude:
            continue
        semantic_type = str(profile.get("semantic_type") or "")
        if semantic_type not in {"categorical", "text", "id"}:
            continue
        unique_ratio = float(profile.get("unique_ratio") or 0.0)
        normalized = normalize_text(column)
        score = 0
        for token, weight in weighted_name_tokens:
            if token in normalized:
                score += weight
        if 0.0001 < unique_ratio <= 0.8:
            score += 2
        if score > 0:
            candidates.append((score, -unique_ratio, column))

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][2]
    return None


def _question_overlap_score(question: str, candidate: str, aliases: list[str] | None = None) -> int:
    normalized_question = normalize_text(question)
    if not normalized_question:
        return 0
    values = [candidate, *(aliases or [])]
    best = 0
    for value in values:
        normalized_value = normalize_text(str(value or ""))
        if not normalized_value:
            continue
        score = 0
        if normalized_value in normalized_question:
            score += 100
        if normalized_question in normalized_value:
            score += 35
        score += sum(1 for ch in set(normalized_value) if ch in normalized_question)
        best = max(best, score)
    return best


def _find_question_dimension_column(
    profiles: dict[str, dict[str, Any]],
    question: str,
    *,
    exclude: set[str] | None = None,
    item_column: str | None = None,
    service_column: str | None = None,
    region_column: str | None = None,
    category_column: str | None = None,
) -> str | None:
    exclude = exclude or set()
    text = str(question or "")
    if _contains_any(text, ("billing item", "计费项")) and item_column and item_column not in exclude:
        return item_column
    if _contains_any(text, ("service", "product", "商品", "产品", "服务")) and service_column and service_column not in exclude:
        return service_column
    if _contains_any(text, ("region", "location", "地域", "地区", "区域", "城市")) and region_column and region_column not in exclude:
        return region_column

    best_score = 0
    best_column: str | None = None
    for column, profile in profiles.items():
        if column in exclude:
            continue
        aliases = [str(item) for item in (profile.get("aliases") or []) if str(item or "").strip()]
        score = _question_overlap_score(text, column, aliases=aliases)
        if profile.get("semantic_type") in {"categorical", "text", "id"}:
            score += 3
        if score > best_score:
            best_score = score
            best_column = column

    if best_column and best_score >= 20:
        return best_column
    if category_column and category_column not in exclude:
        return category_column
    return _find_category_column(profiles, exclude=exclude)
