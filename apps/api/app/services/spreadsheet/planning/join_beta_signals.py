from __future__ import annotations

import re
from typing import Any


_JOIN_ACTION_PATTERNS = (
    re.compile(r"\bjoin\b", re.I),
    re.compile(r"\bmerge\b", re.I),
    re.compile(r"关联", re.I),
    re.compile(r"匹配", re.I),
    re.compile(r"合并", re.I),
    re.compile(r"联合分析", re.I),
    re.compile(r"跨\s*(?:sheet|表|工作表)", re.I),
)

_JOIN_TYPE_INNER_PATTERNS = (
    re.compile(r"\binner\s+join\b", re.I),
    re.compile(r"内连接", re.I),
)

_JOIN_TYPE_LEFT_PATTERNS = (
    re.compile(r"\bleft\s+join\b", re.I),
    re.compile(r"左连接", re.I),
)

_JOIN_TYPE_UNSUPPORTED_PATTERNS = (
    re.compile(r"\bright\s+join\b", re.I),
    re.compile(r"右连接", re.I),
    re.compile(r"\bfull(?:\s+outer)?\s+join\b", re.I),
    re.compile(r"全连接", re.I),
    re.compile(r"\bouter\s+join\b", re.I),
    re.compile(r"\bcross\s+join\b", re.I),
    re.compile(r"\bunion\b", re.I),
)

_JOIN_KEY_PATTERNS = (
    re.compile(
        r"\b(?:by|on|using)\s+([A-Za-z0-9_\-\.\u4e00-\u9fff ]{1,64}?)(?:\s+(?:for|to|with|then|and)|[,\.;，。；]|$)",
        re.I,
    ),
    re.compile(
        r"(?:按|根据|基于|通过)\s*([A-Za-z0-9_\-\.\u4e00-\u9fff]{1,48})\s*(?:字段|列|key|键)?\s*(?:进行|做)?(?:关联|匹配|连接|join|合并)",
        re.I,
    ),
    re.compile(
        r"(?:用|使用)\s*([A-Za-z0-9_\-\.\u4e00-\u9fff]{1,48})\s*(?:作为|做)?\s*(?:关联键|连接键|key|键)",
        re.I,
    ),
)

_MULTI_KEY_PATTERNS = (
    re.compile(r"\bmulti(?:ple)?\s*key", re.I),
    re.compile(r"\bcomposite\s*key", re.I),
    re.compile(r"多键", re.I),
    re.compile(r"多个键", re.I),
    re.compile(r"复合键", re.I),
)

_MORE_THAN_TWO_TABLE_PATTERNS = (
    re.compile(r"\b(?:three|3)\s+(?:sheets?|tables?)\b", re.I),
    re.compile(r"\b(?:4|four|5|five)\s+(?:sheets?|tables?)\b", re.I),
    re.compile(r"(?:三个|3个|三张|3张|四个|4个)\s*(?:sheet|工作表|表)", re.I),
    re.compile(r"多张(?:sheet|工作表|表)", re.I),
)

_AGGREGATION_PATTERNS = (
    re.compile(r"\bsum\b", re.I),
    re.compile(r"\bcount\b", re.I),
    re.compile(r"\bavg\b", re.I),
    re.compile(r"\baverage\b", re.I),
    re.compile(r"\btop\s*\d*\b", re.I),
    re.compile(r"\btrend\b", re.I),
    re.compile(r"\bgroup\s+by\b", re.I),
    re.compile(r"总(?:和|计|量)?", re.I),
    re.compile(r"汇总", re.I),
    re.compile(r"平均", re.I),
    re.compile(r"计数", re.I),
    re.compile(r"排名", re.I),
    re.compile(r"趋势", re.I),
    re.compile(r"占比", re.I),
    re.compile(r"转化率", re.I),
)

_DETAIL_QUERY_PATTERNS = (
    re.compile(r"\bdetail\b", re.I),
    re.compile(r"\blist\b", re.I),
    re.compile(r"\braw\b", re.I),
    re.compile(r"明细", re.I),
    re.compile(r"逐行", re.I),
    re.compile(r"原始", re.I),
)


def _contains_pattern(patterns: tuple[re.Pattern[str], ...], text: str) -> bool:
    return any(pattern.search(text) is not None for pattern in patterns)


def _normalize_join_key(value: str) -> str:
    cleaned = str(value or "").strip().strip("`'\"“”‘’")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > 48:
        return cleaned[:48].rstrip()
    return cleaned


def _extract_join_key(text: str) -> str:
    for pattern in _JOIN_KEY_PATTERNS:
        match = pattern.search(text)
        if match is None:
            continue
        key = _normalize_join_key(match.group(1))
        if key:
            return key
    return ""


def _infer_join_type(text: str) -> str:
    if _contains_pattern(_JOIN_TYPE_UNSUPPORTED_PATTERNS, text):
        return "unsupported"
    if _contains_pattern(_JOIN_TYPE_LEFT_PATTERNS, text):
        return "left"
    if _contains_pattern(_JOIN_TYPE_INNER_PATTERNS, text):
        return "inner"
    return "unspecified"


def _contains_multi_key_signal(text: str, join_key: str) -> bool:
    if _contains_pattern(_MULTI_KEY_PATTERNS, text):
        return True
    lowered_key = join_key.lower()
    if " and " in lowered_key or "与" in join_key or "和" in join_key or "," in join_key:
        return True
    return False


def _contains_aggregate_signal(text: str) -> bool:
    if _contains_pattern(_AGGREGATION_PATTERNS, text):
        return True
    if _contains_pattern(_DETAIL_QUERY_PATTERNS, text):
        return False
    return False


def evaluate_join_beta_request(text: Any) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if not normalized:
        return {
            "is_join_request": False,
            "join_key": "",
            "join_type": "",
            "is_aggregate_query": False,
            "eligible": False,
            "reasons": [],
        }

    is_join_request = _contains_pattern(_JOIN_ACTION_PATTERNS, normalized)
    if not is_join_request:
        return {
            "is_join_request": False,
            "join_key": "",
            "join_type": "",
            "is_aggregate_query": False,
            "eligible": False,
            "reasons": [],
        }

    join_key = _extract_join_key(normalized)
    join_type = _infer_join_type(normalized)
    is_aggregate_query = _contains_aggregate_signal(normalized)

    reasons: list[str] = []
    if _contains_pattern(_MORE_THAN_TWO_TABLE_PATTERNS, normalized):
        reasons.append("join_more_than_two_tables")
    if join_type == "unsupported":
        reasons.append("join_type_not_allowed")
    if not join_key:
        reasons.append("join_key_missing")
    if join_key and _contains_multi_key_signal(normalized, join_key):
        reasons.append("join_multi_key_not_allowed")
    if not is_aggregate_query:
        reasons.append("join_non_aggregate_query")

    return {
        "is_join_request": True,
        "join_key": join_key,
        "join_type": join_type,
        "is_aggregate_query": is_aggregate_query,
        "eligible": len(reasons) == 0,
        "reasons": reasons,
    }
