from __future__ import annotations

import re
from typing import Any

from ...conversation.context_interpreter import build_analysis_anchor_payload, get_default_context_interpreter
from ..intent_accessors import analysis_intent_kind
from ..planner_intent_signals import (
    _detail_question,
    _explicit_analysis_request,
    _followup_question,
    _share_question,
    _text_mode_followup,
    _trend_question,
)
from ..planner_text_utils import _contains_any, _extract_top_k
from ..planner_time import (
    _extract_date_literal,
    _extract_month_literal,
)


CHART_KEYWORDS = (
    "chart",
    "plot",
    "graph",
    "visual",
    "图表",
    "柱状图",
    "折线图",
    "饼图",
    "趋势图",
    "可视化",
)

SHEET_SWITCH_KEYWORDS = (
    "another sheet",
    "other sheet",
    "next sheet",
    "switch sheet",
    "switch to another sheet",
    "switch to other sheet",
    "另一个sheet",
    "另外一个sheet",
    "另一个工作表",
    "另外一个工作表",
    "换个sheet",
    "换一个sheet",
    "再看另一个",
    "其他sheet",
    "别的sheet",
    "別のシート",
    "他のシート",
    "別シート",
)


def infer_mode(chat_text: str, requested_mode: str) -> str:
    if requested_mode in {"text", "chart"}:
        return requested_mode
    lowered = str(chat_text or "").lower()
    return "chart" if any(keyword in lowered for keyword in CHART_KEYWORDS) else "text"


def _followup_interpretation(followup_context: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(followup_context, dict):
        return {}
    payload = followup_context.get("_interpreted")
    return payload if isinstance(payload, dict) else {}


def _interpreted_confidence(followup_context: dict[str, Any] | None) -> float:
    try:
        return float(_followup_interpretation(followup_context).get("confidence") or 0.0)
    except Exception:
        return 0.0


def _interpreted_value(followup_context: dict[str, Any] | None, key: str, default: Any = None) -> Any:
    payload = _followup_interpretation(followup_context)
    value = payload.get(key, default)
    return default if value is None else value


def _with_interpreted_followup(
    df: Any,
    *,
    chat_text: str,
    requested_mode: str,
    followup_context: dict[str, Any] | None,
    context_interpreter_factory: Any = get_default_context_interpreter,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not isinstance(followup_context, dict) or not followup_context:
        return followup_context, {"provider": "noop", "used": False, "reason": "no_followup_context"}
    if _interpreted_confidence(followup_context) >= 0.55:
        return dict(followup_context), {"provider": "cached", "used": False, "reason": "already_interpreted"}
    interpreter = context_interpreter_factory()
    try:
        result = interpreter.interpret(
            df,
            chat_text=chat_text,
            requested_mode=requested_mode,
            followup_context=followup_context,
        )
    except Exception as exc:
        return dict(followup_context), {"provider": getattr(interpreter, "name", "unknown"), "used": False, "reason": str(exc)}

    enhanced_context = dict(followup_context)
    interpretation = result.interpretation
    anchor_payload = build_analysis_anchor_payload(followup_context=followup_context)
    if interpretation is not None and interpretation.analysis_anchor is not None:
        anchor_payload = interpretation.analysis_anchor.model_dump()
    if interpretation is not None and float(interpretation.confidence or 0.0) >= 0.55:
        enhanced_context["_interpreted"] = interpretation.model_dump()
    if isinstance(anchor_payload, dict) and anchor_payload:
        enhanced_context["analysis_anchor"] = anchor_payload
    return enhanced_context, result.meta


def _parse_top_k(chat_text: str) -> int | None:
    top_k = _extract_top_k(chat_text, default=0)
    return top_k or None


def _detail_to_ranking_followup(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if not isinstance(followup_context, dict):
        return False
    last_intent = _followup_last_intent(followup_context)
    if last_intent != "detail_rows":
        return False
    text = str(chat_text or "").strip()
    if not text:
        return False
    rejects_detail = _contains_any(
        text,
        (
            "不要返回表格",
            "不要返回5行",
            "不要返回行",
            "不要返回明细",
            "不要明细",
            "别返回表格",
            "别返回行",
            "not rows",
            "not table rows",
            "instead of rows",
        ),
    )
    asks_summary = _contains_any(
        text,
        (
            "最高项",
            "消费最高项",
            "对应的费用",
            "按项",
            "每项",
            "汇总",
            "聚合",
            "grouped",
            "group by item",
            "top items",
        ),
    )
    return rejects_detail and asks_summary


def _top_k_followup(chat_text: str, followup_context: dict[str, Any] | None) -> int | None:
    interpreted_top_k = _interpreted_value(followup_context, "top_k")
    interpreted_preserve = bool(_interpreted_value(followup_context, "preserve_previous_analysis", False))
    if interpreted_top_k is not None and interpreted_preserve:
        try:
            return max(1, min(int(interpreted_top_k), 50))
        except Exception:
            pass
    if not _contextual_followup(chat_text, followup_context):
        return None
    current = str(chat_text or "").strip()
    if not current:
        return None
    top_k = _parse_top_k(current)
    if top_k is None:
        return None
    if _contains_any(
        current,
        (
            "总金额",
            "sum",
            "total amount",
            "平均",
            "average",
            "avg",
            "趋势",
            "trend",
            "按月",
            "每月",
            "多少行",
            "row count",
            "占比",
            "share",
            "饼图",
            "pie",
        ),
    ):
        return None
    if _followup_last_intent(followup_context) not in {"ranking", "share", "detail_rows"}:
        return None
    return top_k


def _rank_position_from_text(chat_text: str) -> int | None:
    text = str(chat_text or "").strip().lower()
    if not text:
        return None
    patterns: list[tuple[str, int]] = [
        (r"第\s*1\s*名|第一名|first|1st", 1),
        (r"第\s*2\s*名|第二名|second|2nd", 2),
        (r"第\s*3\s*名|第三名|third|3rd", 3),
        (r"第\s*4\s*名|第四名|fourth|4th", 4),
        (r"第\s*5\s*名|第五名|fifth|5th", 5),
        (r"倒数第?\s*1\s*名|最后一名|最后一个|last", -1),
    ]
    for pattern, rank in patterns:
        if re.search(pattern, text, flags=re.I):
            return rank
    return None


def _rank_lookup_followup(chat_text: str, followup_context: dict[str, Any] | None) -> int | None:
    interpreted_rank = _interpreted_value(followup_context, "target_rank")
    interpreted_kind = str(_interpreted_value(followup_context, "kind", "") or "")
    if interpreted_rank is not None and interpreted_kind in {"followup_lookup", "followup_refine"}:
        try:
            return int(interpreted_rank)
        except Exception:
            pass
    if not _contextual_followup(chat_text, followup_context):
        return None
    if _followup_last_intent(followup_context) != "ranking":
        return None
    current = str(chat_text or "").strip()
    if not current:
        return None
    rank = _rank_position_from_text(current)
    if rank is None:
        return None
    if _contains_any(
        current,
        (
            "是什么",
            "什么东西",
            "什么意思",
            "怎么理解",
            "解释一下",
            "结合表格",
            "what is",
            "explain",
        ),
    ):
        return None
    return rank


def _share_switch_followup(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if str(_interpreted_value(followup_context, "view_intent", "") or "") == "share":
        return True
    if not _contextual_followup(chat_text, followup_context):
        return False
    if _followup_last_intent(followup_context) not in {"ranking", "share"}:
        return False
    return _share_question(chat_text)


def _trend_switch_followup(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if str(_interpreted_value(followup_context, "view_intent", "") or "") == "trend":
        return True
    if not _contextual_followup(chat_text, followup_context):
        return False
    if _followup_last_intent(followup_context) not in {"ranking", "share", "detail_rows", "trend"}:
        return False
    return _trend_question(chat_text) or _contains_any(chat_text, ("改成趋势", "看趋势", "按月看", "monthly trend"))


def _detail_switch_followup(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if str(_interpreted_value(followup_context, "view_intent", "") or "") == "detail_rows":
        return True
    if not _contextual_followup(chat_text, followup_context):
        return False
    if _followup_last_intent(followup_context) not in {"ranking", "share", "trend", "detail_rows"}:
        return False
    return _detail_question(chat_text) or _contains_any(chat_text, ("改成明细", "看明细", "原始记录", "raw rows", "detail rows"))


def _dimension_switch_followup(chat_text: str, *, new_dimension: str | None, followup_context: dict[str, Any] | None) -> bool:
    if _breakdown_followup(chat_text, followup_context):
        return False
    interpreted_dimension = str(_interpreted_value(followup_context, "new_dimension", "") or "").strip()
    if interpreted_dimension and new_dimension and interpreted_dimension == new_dimension:
        return True
    if not new_dimension:
        return False
    if not _contextual_followup(chat_text, followup_context):
        return False
    if _followup_last_intent(followup_context) not in {"ranking", "share"}:
        return False
    return _contains_any(chat_text, ("按", "by", "换成", "改成", "看", "展示", "统计"))


def _time_filter_followup(chat_text: str, followup_context: dict[str, Any] | None) -> tuple[str, str] | None:
    interpreted_value = str(_interpreted_value(followup_context, "time_value", "") or "").strip()
    interpreted_operator = str(_interpreted_value(followup_context, "time_operator", "") or "").strip()
    if interpreted_value and interpreted_operator in {"=", "contains"}:
        return interpreted_operator, interpreted_value
    if not _contextual_followup(chat_text, followup_context):
        return None
    if _followup_last_intent(followup_context) not in {"ranking", "share", "trend", "detail_rows"}:
        return None
    day = _extract_date_literal(chat_text)
    if day:
        return "=", day
    month = _extract_month_literal(chat_text)
    if month:
        return "contains", month
    return None


def _mode_switch_followup(chat_text: str, *, mode: str, followup_context: dict[str, Any] | None) -> bool:
    interpreted_output_mode = str(_interpreted_value(followup_context, "output_mode", "") or "").strip()
    if interpreted_output_mode in {"text", "chart"}:
        last_mode = str(followup_context.get("last_mode") or "") if isinstance(followup_context, dict) else ""
        if interpreted_output_mode == mode and last_mode in {"text", "chart"} and interpreted_output_mode != last_mode:
            return True
    if not _contextual_followup(chat_text, followup_context):
        return False
    last_mode = str(followup_context.get("last_mode") or "") if isinstance(followup_context, dict) else ""
    if mode not in {"text", "chart"} or last_mode not in {"text", "chart"}:
        return False
    if mode == last_mode:
        return False
    current = str(chat_text or "").strip()
    if not current:
        return False
    if _contains_any(
        current,
        (
            "是什么",
            "什么东西",
            "什么意思",
            "怎么理解",
            "解释一下",
            "结合表格",
            "具体是什么",
            "what is",
            "explain",
        ),
    ):
        return False
    if mode == "chart":
        return any(keyword in current.lower() for keyword in CHART_KEYWORDS)
    return _text_mode_followup(current)


def _ranking_explain_followup(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if not isinstance(followup_context, dict):
        return False
    if _followup_last_intent(followup_context) != "ranking":
        return False
    text = str(chat_text or "").strip()
    if not text:
        return False
    asks_explain = _explain_request(text)
    references_previous = _contains_any(
        text,
        (
            "上面",
            "第一个",
            "第一名",
            "排第一个",
            "top1",
            "top 1",
            "leader",
            "这个",
        ),
    )
    return asks_explain and references_previous


def _explain_request(chat_text: str) -> bool:
    return _contains_any(
        str(chat_text or "").strip(),
        (
            "为什么",
            "原因",
            "分析一下",
            "分析",
            "为何",
            "why",
            "reason",
            "analyze",
            "是什么",
            "什么东西",
            "什么意思",
            "怎么理解",
            "解释一下",
            "结合表格",
            "具体是什么",
            "what is",
            "explain",
        ),
    )


def _compare_basis(chat_text: str, followup_context: dict[str, Any] | None = None) -> str:
    interpreted = str(_interpreted_value(followup_context, "compare_basis", "") or "").strip()
    if interpreted in {"previous_period", "year_over_year"}:
        return interpreted
    text = str(chat_text or "").lower()
    if _contains_any(text, ("同比", "去年同期", "year over year", "yoy")):
        return "year_over_year"
    return "previous_period"


def _breakdown_followup(chat_text: str, followup_context: dict[str, Any] | None = None) -> bool:
    interpreted_view = str(_interpreted_value(followup_context, "view_intent", "") or "").strip()
    if interpreted_view == "ranking" and _explain_request(chat_text):
        return True
    return _contains_any(
        chat_text,
        (
            "拆解",
            "拆分",
            "细分",
            "分布",
            "构成",
            "breakdown",
            "split by",
            "group by",
        ),
    ) and _explain_request(chat_text)


def _followup_last_turn(followup_context: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(followup_context, dict):
        return None
    last_turn = followup_context.get("last_turn")
    return last_turn if isinstance(last_turn, dict) else None


def _followup_last_intent(followup_context: dict[str, Any] | None) -> str:
    last_turn = _followup_last_turn(followup_context)
    if isinstance(last_turn, dict):
        intent = analysis_intent_kind(last_turn, fallback=str(last_turn.get("intent") or ""))
        if intent:
            return intent
    summary = followup_context.get("last_pipeline_summary") if isinstance(followup_context, dict) else None
    if isinstance(summary, dict):
        return analysis_intent_kind(summary, fallback=str(summary.get("intent") or ""))
    return ""


def _followup_last_question(followup_context: dict[str, Any] | None) -> str:
    last_turn = _followup_last_turn(followup_context)
    if not isinstance(last_turn, dict):
        return ""
    return str(last_turn.get("question") or "").strip()


def _analysis_anchor_summary(followup_context: dict[str, Any] | None) -> str:
    if not isinstance(followup_context, dict):
        return ""
    anchor = followup_context.get("analysis_anchor")
    if not isinstance(anchor, dict):
        return ""
    parts: list[str] = []
    metric = str(anchor.get("metric_column") or anchor.get("metric_alias") or "").strip()
    if metric:
        parts.append(f"metric={metric}")
    dimension = str(anchor.get("dimension_column") or "").strip()
    if dimension:
        parts.append(f"dimension={dimension}")
    time_column = str(anchor.get("time_column") or "").strip()
    time_grain = str(anchor.get("time_grain") or "").strip()
    if time_column and time_grain:
        parts.append(f"time={time_column}({time_grain})")
    elif time_column:
        parts.append(f"time={time_column}")
    filters = anchor.get("filters")
    if isinstance(filters, list):
        rendered_filters: list[str] = []
        for item in filters[:2]:
            if not isinstance(item, dict):
                continue
            col = str(item.get("column") or "").strip()
            op = str(item.get("op") or "=").strip() or "="
            value = str(item.get("value") or "").strip()
            if not col or not value:
                continue
            rendered_filters.append(f"{col}{op}{value}")
        if rendered_filters:
            parts.append(f"filters={', '.join(rendered_filters)}")
    return "; ".join(parts)


def _contextual_followup(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    interpreted_kind = str(_interpreted_value(followup_context, "kind", "") or "")
    if interpreted_kind.startswith("followup"):
        return True
    if bool(_interpreted_value(followup_context, "requires_previous_context", False)):
        return True
    if isinstance(followup_context, dict) and bool(followup_context.get("is_followup")):
        return True
    return _followup_question(chat_text)


def _sheet_switch_followup(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if bool(_interpreted_value(followup_context, "switch_sheet", False)):
        return True
    if str(_interpreted_value(followup_context, "sheet_reference", "") or "") in {"another", "previous"}:
        return True
    if isinstance(followup_context, dict) and str(followup_context.get("followup_action") or "") == "continue_next_step":
        return True
    if isinstance(followup_context, dict) and bool(followup_context.get("wants_sheet_switch")):
        return True
    if isinstance(followup_context, dict) and str(followup_context.get("sheet_reference_hint") or "") in {"another", "previous"}:
        return True
    if not _contextual_followup(chat_text, followup_context):
        return False
    return _contains_any(chat_text, SHEET_SWITCH_KEYWORDS)


def _preserve_previous_analysis(chat_text: str, followup_context: dict[str, Any] | None) -> bool:
    if bool(_interpreted_value(followup_context, "preserve_previous_analysis", False)):
        return True
    if _sheet_switch_followup(chat_text, followup_context):
        return True
    if not _contextual_followup(chat_text, followup_context):
        return False
    current = str(chat_text or "").strip()
    if not current:
        return True
    if _explicit_analysis_request(current):
        return False
    return True


def _effective_chat_text(chat_text: str, followup_context: dict[str, Any] | None) -> str:
    interpreted_question = str(_interpreted_value(followup_context, "standalone_question", "") or "").strip()
    if interpreted_question and _interpreted_confidence(followup_context) >= 0.55:
        return interpreted_question
    if not followup_context:
        return chat_text
    last_turn = _followup_last_turn(followup_context)
    if not isinstance(last_turn, dict):
        return chat_text
    previous_question = _followup_last_question(followup_context)
    previous_intent = _followup_last_intent(followup_context)
    if not previous_question:
        return chat_text
    current = str(chat_text or "").strip()
    if not current:
        return previous_question
    if _contextual_followup(current, followup_context):
        last_mode = str(followup_context.get("last_mode") or "").strip() if isinstance(followup_context, dict) else ""
        pipeline_summary = followup_context.get("last_pipeline_summary") if isinstance(followup_context, dict) else None
        anchor_summary = _analysis_anchor_summary(followup_context)
        lines = [f"Previous question: {previous_question}", f"Previous intent: {previous_intent}"]
        if last_mode:
            lines.append(f"Previous mode: {last_mode}")
        if isinstance(pipeline_summary, dict) and pipeline_summary:
            lines.append(f"Previous pipeline summary: {pipeline_summary}")
        if anchor_summary:
            lines.append(f"Previous analysis anchor: {anchor_summary}")
        lines.append(f"Follow-up request: {current}")
        return "\n".join(lines)
    return current


def _resolve_mode(chat_text: str, requested_mode: str, followup_context: dict[str, Any] | None = None) -> str:
    if requested_mode in {"text", "chart"}:
        return requested_mode
    interpreted_output_mode = str(_interpreted_value(followup_context, "output_mode", "") or "").strip()
    if interpreted_output_mode in {"text", "chart"}:
        return interpreted_output_mode
    current = str(chat_text or "").strip()
    if any(keyword in current.lower() for keyword in CHART_KEYWORDS):
        return "chart"
    if _text_mode_followup(current):
        return "text"
    if isinstance(followup_context, dict) and _contextual_followup(current, followup_context):
        last_mode = str(followup_context.get("last_mode") or "").strip()
        if last_mode in {"text", "chart"}:
            return last_mode
    return infer_mode(chat_text, requested_mode)
