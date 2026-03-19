from __future__ import annotations

import os
import re
from typing import Any


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


def detect_unsupported_request(text: Any, *, locale: str) -> dict[str, Any] | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None

    reason_codes: list[str] = []
    if _contains_advanced_stat_intent(normalized):
        reason_codes.append("advanced_statistics")
    if _contains_cross_sheet_intent(normalized):
        reason_codes.append("cross_sheet")
    if not reason_codes:
        return None

    messages = _MESSAGES[_pick_locale(locale)]
    reason_set = set(reason_codes)
    if reason_set == {"advanced_statistics", "cross_sheet"}:
        message = messages["both"]
    elif "cross_sheet" in reason_codes:
        message = messages["cross_sheet"]
    else:
        message = messages["advanced_statistics"]

    return {"reason_codes": reason_codes, "message": message}
