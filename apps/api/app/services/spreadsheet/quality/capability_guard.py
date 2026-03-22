from __future__ import annotations

import os
import re
from typing import Any

from ..planning.join_beta_signals import evaluate_join_beta_request


_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}

_ADVANCED_STAT_PATTERNS = (
    re.compile(r"显著性(?:检验|分析)?", re.I),
    re.compile(r"p\s*[- ]?value", re.I),
    re.compile(r"p\s*值", re.I),
    re.compile(r"t\s*检验", re.I),
    re.compile(r"t\s*test", re.I),
    re.compile(r"卡方检验", re.I),
    re.compile(r"chi\s*[- ]?square", re.I),
    re.compile(r"方差分析", re.I),
    re.compile(r"\banova\b", re.I),
    re.compile(r"相关系数", re.I),
    re.compile(r"相关性分析", re.I),
    re.compile(r"\bpearson\b", re.I),
    re.compile(r"\bspearman\b", re.I),
    re.compile(r"回归分析", re.I),
    re.compile(r"线性回归", re.I),
    re.compile(r"多元回归", re.I),
    re.compile(r"logistic\s*regression", re.I),
    re.compile(r"regression", re.I),
)

_FORECAST_PATTERNS = (
    re.compile(r"预测", re.I),
    re.compile(r"预估", re.I),
    re.compile(r"预计", re.I),
    re.compile(r"forecast", re.I),
    re.compile(r"predict", re.I),
    re.compile(r"projection", re.I),
    re.compile(r"projected", re.I),
    re.compile(r"extrapolat", re.I),
)

_CROSS_SHEET_DIRECT_PATTERNS = (
    re.compile(r"跨\s*(?:sheet|表|工作表)", re.I),
    re.compile(r"多\s*(?:sheet|工作表)", re.I),
    re.compile(r"多sheet", re.I),
    re.compile(r"sheet\s*\d+\s*(?:和|与|跟|及|,|，)\s*sheet\s*\d+", re.I),
)

_CROSS_SHEET_REF_PATTERNS = (
    re.compile(r"sheet\s*\d+", re.I),
    re.compile(r"(?:多个|两张|两个|多张)\s*(?:sheet|工作表|表)", re.I),
    re.compile(r"(?:工作表|sheet)(?:之间|间)", re.I),
)

_CROSS_SHEET_ACTION_PATTERNS = (
    re.compile(r"关联", re.I),
    re.compile(r"合并", re.I),
    re.compile(r"匹配", re.I),
    re.compile(r"\bjoin\b", re.I),
    re.compile(r"联动", re.I),
    re.compile(r"拼接", re.I),
    re.compile(r"联合(?:分析|统计)?", re.I),
    re.compile(r"一起(?:分析|统计)?", re.I),
    re.compile(r"对照", re.I),
)

_MESSAGES = {
    "en": {
        "feature_disabled": "Spreadsheet conversation is currently disabled.",
        "advanced_statistics": (
            "Advanced statistics such as significance tests, p-values, correlations, regression, and ANOVA are not supported yet. "
            "This release focuses on single-sheet totals, averages, Top N, trends, detail rows, and basic charts."
        ),
        "cross_sheet": (
            "Workbook-aware routing to one sheet is supported, but cross-sheet analysis is not supported yet. "
            "Please stay within one sheet and ask for totals, averages, Top N, trends, detail rows, or charts."
        ),
        "forecast": (
            "Forecasting or predicting future values is not supported yet. "
            "This release only answers based on existing spreadsheet data, such as totals, averages, Top N, trends, detail rows, and charts."
        ),
        "both": (
            "Workbook-aware routing to one sheet is supported, but cross-sheet analysis and advanced statistics are not supported yet. "
            "This release focuses on single-sheet totals, averages, Top N, trends, detail rows, and basic charts."
        ),
        "join_beta_candidate": (
            "Workbook-aware routing to one sheet is supported, and cross-sheet analysis is still controlled. "
            "Join Beta currently only supports two-sheet, single-key, inner/left joins for aggregate questions (sum/count/avg/top/trend). "
            "This request looks like a Join Beta candidate, but it is not enabled in this path yet. "
            "Please continue with one sheet first, then ask the next step on another sheet."
        ),
        "join_beta_out_of_scope": (
            "Workbook-aware routing to one sheet is supported, but cross-sheet analysis is not supported yet for arbitrary joins. "
            "Join Beta currently only supports two-sheet, single-key, inner/left joins for aggregate questions (sum/count/avg/top/trend). "
            "Detected unsupported join condition: {join_hint}"
        ),
    },
    "zh-CN": {
        "feature_disabled": "表格对话分析功能当前已暂停服务，请稍后再试。",
        "advanced_statistics": (
            "当前暂不支持显著性检验、p 值、相关系数、回归分析、ANOVA 等高级统计分析。"
            "当前版本更适合单个工作表内的总量、均值、TopN、趋势、明细和基础图表分析。"
        ),
        "cross_sheet": (
            "当前已支持 workbook 内单 sheet 智能路由，但暂不支持跨 sheet 联动或多工作表联合分析。"
            "请先限定在单个工作表内，再进行总量、均值、TopN、趋势、明细或基础图表分析。"
        ),
        "forecast": (
            "当前暂不支持对未来数据做预测、预估或外推。"
            "当前版本只基于表格中已有数据回答总量、均值、TopN、趋势、明细和基础图表问题。"
        ),
        "both": (
            "当前已支持 workbook 内单 sheet 智能路由，但暂不支持跨 sheet 联动分析，也不支持显著性检验、p 值、相关系数、回归分析、ANOVA 等高级统计分析。"
            "当前版本更适合单个工作表内的总量、均值、TopN、趋势、明细和基础图表分析。"
        ),
        "join_beta_candidate": (
            "当前已支持 workbook 内单 sheet 智能路由，跨 sheet 能力仍处于受控阶段。"
            "Join Beta 仅支持两表、单键、inner/left、聚合类问题（sum/count/avg/top/trend）。"
            "你当前的问题已接近 Join Beta 范围，但该路径暂未开放执行。"
            "建议先分析一个 sheet，再按下一步继续到另一个 sheet。"
        ),
        "join_beta_out_of_scope": (
            "当前已支持 workbook 内单 sheet 智能路由，但暂不支持任意跨 sheet join。"
            "Join Beta 仅支持两表、单键、inner/left、聚合类问题（sum/count/avg/top/trend）。"
            "检测到超出范围的 join 条件：{join_hint}"
        ),
    },
    "ja-JP": {
        "feature_disabled": "スプレッドシート対話分析は現在無効です。",
        "advanced_statistics": (
            "有意差検定、p 値、相関、回帰、ANOVA などの高度な統計分析はまだ未対応です。"
            "現在のリリースは単一シートの合計、平均、Top N、トレンド、詳細行、基本チャートに集中しています。"
        ),
        "cross_sheet": (
            "ワークブック内で 1 つのシートへルーティングすることはできますが、複数シートをまたぐ分析はまだ未対応です。"
            "1 つのシートに限定して、合計、平均、Top N、トレンド、詳細行、チャートを試してください。"
        ),
        "forecast": (
            "将来値の予測や外挿はまだ未対応です。"
            "現在のリリースは、シート内に存在するデータに基づく合計、平均、Top N、トレンド、詳細行、基本チャートに対応しています。"
        ),
        "both": (
            "ワークブック内で 1 つのシートへルーティングすることはできますが、複数シート分析と高度な統計分析はまだ未対応です。"
            "現在のリリースは単一シートの合計、平均、Top N、トレンド、詳細行、基本チャートに集中しています。"
        ),
        "join_beta_candidate": (
            "ワークブック内の 1 シートルーティングは利用できますが、シート横断分析はまだ制御付き段階です。"
            "Join Beta は 2 シート・単一キー・inner/left・集計系質問（sum/count/avg/top/trend）のみ対応予定です。"
            "この質問は Join Beta 候補ですが、この経路ではまだ有効化されていません。"
            "まず 1 シートを分析し、次に別シートへ進めてください。"
        ),
        "join_beta_out_of_scope": (
            "ワークブック内の 1 シートルーティングは利用できますが、任意のシート横断 join はまだ未対応です。"
            "Join Beta は 2 シート・単一キー・inner/left・集計系質問（sum/count/avg/top/trend）のみ対象です。"
            "範囲外の join 条件を検出しました: {join_hint}"
        ),
    },
}

_JOIN_REASON_HINTS = {
    "en": {
        "join_more_than_two_tables": "more than two tables were requested",
        "join_type_not_allowed": "join type is not inner/left",
        "join_key_missing": "join key is missing",
        "join_multi_key_not_allowed": "multiple join keys were detected",
        "join_non_aggregate_query": "question is not aggregate-oriented",
    },
    "zh-CN": {
        "join_more_than_two_tables": "请求涉及超过两张表",
        "join_type_not_allowed": "join 类型不是 inner/left",
        "join_key_missing": "缺少明确 join key",
        "join_multi_key_not_allowed": "检测到多键 join",
        "join_non_aggregate_query": "问题不属于聚合类分析",
    },
    "ja-JP": {
        "join_more_than_two_tables": "2 シート超の結合が要求されています",
        "join_type_not_allowed": "join 種別が inner/left ではありません",
        "join_key_missing": "join キーが不足しています",
        "join_multi_key_not_allowed": "複数キー join が検出されました",
        "join_non_aggregate_query": "質問が集計系ではありません",
    },
}


def _env_flag(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUE_VALUES:
        return True
    if raw in _FALSE_VALUES:
        return False
    return default


def is_spreadsheet_feature_enabled() -> bool:
    return _env_flag("TALK2SHEET_SPREADSHEET_FEATURE_ENABLED", True)


def _pick_locale(locale: str) -> str:
    return locale if locale in _MESSAGES else "en"


def build_feature_disabled_message(locale: str) -> str:
    return _MESSAGES[_pick_locale(locale)]["feature_disabled"]


def _contains_advanced_stat_intent(text: str) -> bool:
    return any(pattern.search(text) for pattern in _ADVANCED_STAT_PATTERNS)


def _contains_cross_sheet_intent(text: str) -> bool:
    if any(pattern.search(text) for pattern in _CROSS_SHEET_DIRECT_PATTERNS):
        return True
    has_ref = any(pattern.search(text) for pattern in _CROSS_SHEET_REF_PATTERNS)
    has_action = any(pattern.search(text) for pattern in _CROSS_SHEET_ACTION_PATTERNS)
    return has_ref and has_action


def _contains_forecast_intent(text: str) -> bool:
    return any(pattern.search(text) for pattern in _FORECAST_PATTERNS)


def _is_sequential_sheet_followup_context(followup_context: dict[str, Any] | None) -> bool:
    if not isinstance(followup_context, dict):
        return False
    clarification_resolution = followup_context.get("clarification_resolution")
    if isinstance(clarification_resolution, dict):
        if str(clarification_resolution.get("kind") or "") == "sheet_resolution":
            return True
    if str(followup_context.get("last_current_step_id") or "").strip():
        return True
    last_task_steps = followup_context.get("last_task_steps")
    if isinstance(last_task_steps, list) and len(last_task_steps) > 0:
        return True
    return False


def _join_hint_text(locale: str, *, reasons: list[str]) -> str:
    normalized_locale = _pick_locale(locale)
    reason_map = _JOIN_REASON_HINTS.get(normalized_locale, _JOIN_REASON_HINTS["en"])
    hints = [str(reason_map.get(code) or code).strip() for code in reasons if str(code).strip()]
    if not hints:
        return ""
    if normalized_locale == "zh-CN":
        return "；".join(hints)
    if normalized_locale == "ja-JP":
        return "、".join(hints)
    return "; ".join(hints)


def detect_unsupported_request(
    text: Any,
    *,
    locale: str,
    followup_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None

    reason_codes: list[str] = []
    join_guard: dict[str, Any] | None = None
    if _contains_advanced_stat_intent(normalized):
        reason_codes.append("advanced_statistics")
    join_eval = evaluate_join_beta_request(normalized)
    if bool(join_eval.get("is_join_request")) and not _is_sequential_sheet_followup_context(followup_context):
        reason_codes.append("cross_sheet")
        if bool(join_eval.get("eligible")):
            reason_codes.append("join_beta_candidate")
        else:
            reason_codes.append("join_beta_out_of_scope")
        join_guard = {
            "requested": True,
            "eligible": bool(join_eval.get("eligible")),
            "join_key": str(join_eval.get("join_key") or ""),
            "join_type": str(join_eval.get("join_type") or ""),
            "reasons": [str(item) for item in list(join_eval.get("reasons") or []) if str(item).strip()],
        }
    elif _contains_cross_sheet_intent(normalized) and not _is_sequential_sheet_followup_context(followup_context):
        reason_codes.append("cross_sheet")
    if not reason_codes:
        return None

    messages = _MESSAGES[_pick_locale(locale)]
    reason_set = set(reason_codes)
    if "join_beta_out_of_scope" in reason_codes:
        join_hint = _join_hint_text(locale, reasons=list(join_guard.get("reasons") or [])) if isinstance(join_guard, dict) else ""
        message = messages["join_beta_out_of_scope"].format(join_hint=join_hint or "unknown reason")
        if "advanced_statistics" in reason_set:
            message = f"{message} {messages['advanced_statistics']}"
    elif "join_beta_candidate" in reason_codes:
        message = messages["join_beta_candidate"]
        if "advanced_statistics" in reason_set:
            message = f"{message} {messages['advanced_statistics']}"
    elif reason_set == {"advanced_statistics", "cross_sheet"}:
        message = messages["both"]
    elif "cross_sheet" in reason_codes:
        message = messages["cross_sheet"]
    else:
        message = messages["advanced_statistics"]

    payload: dict[str, Any] = {"reason_codes": reason_codes, "message": message}
    if isinstance(join_guard, dict):
        payload["join_guard"] = join_guard
    return payload
