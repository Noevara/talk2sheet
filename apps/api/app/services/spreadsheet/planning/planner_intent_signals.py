from __future__ import annotations

import re
from typing import Any

from .planner_text_utils import _contains_any
from .planner_time import (
    _extract_month_literals,
    _extract_month_numbers,
    _resolve_requested_month_buckets,
)


def _extract_chart_type(chat_text: str, default: str = "bar") -> str:
    text = str(chat_text or "").lower()

    def has_word(token: str) -> bool:
        return re.search(rf"\b{re.escape(token)}\b", text, flags=re.I) is not None

    if "饼图" in text or "占比" in text or has_word("pie") or has_word("share"):
        return "pie"
    if "折线" in text or "趋势" in text or has_word("line") or has_word("trend"):
        return "line"
    if "散点" in text or has_word("scatter"):
        return "scatter"
    return default


def _row_count_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("how many rows", "row count", "多少行", "多少条", "总记录数", "件数"))


def _total_amount_question(chat_text: str) -> bool:
    text = str(chat_text or "")
    if _contains_any(text, ("如果", "假如", "若", "if")) and _contains_any(text, ("降低", "减少", "reduce", "drop")):
        return False
    return _contains_any(text, ("total amount", "sum", "总金额", "总费用", "总应付", "总消费"))


def _multi_period_amount_question(df: Any, *, chat_text: str, date_column: str | None) -> list[str]:
    text = str(chat_text or "")
    if not _total_amount_question(text):
        return []
    if not _contains_any(text, ("看一下", "分别", "各自", "各月份", "each", "respectively", "breakdown", "按月")) and len(_extract_month_literals(text)) < 2 and len(_extract_month_numbers(text)) < 2:
        return []
    return _resolve_requested_month_buckets(df, date_column=date_column, chat_text=text)


def _average_amount_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("average", "avg", "平均金额", "平均费用", "平均值"))


def _day_count_question(chat_text: str) -> bool:
    text = str(chat_text or "")
    if re.search(r"(多少|几)\s*天", text):
        return True
    return _contains_any(text, ("多少个自然日", "几天的数据", "多少天的数据", "有数据的天数", "活跃天数"))


def _forecast_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("预测", "预估", "预计", "forecast", "predict", "projection", "projected"))


def _forecast_series_question(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        ("每天", "每日", "逐日", "每一天", "each day", "per day", "daily breakdown", "daily forecast"),
    )


def _remaining_period_question(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        ("剩余", "剩下", "余下", "后续", "remaining", "rest of", "the rest of"),
    )


def _ranking_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("top", "rank", "排行", "排名", "前", "最多", "最大", "最高", "largest", "biggest"))


def _detail_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("detail", "明细", "top rows", "records", "记录", "前10条", "前5条"))


def _single_transaction_question(chat_text: str) -> bool:
    text = str(chat_text or "")
    has_single = _contains_any(text, ("single", "单笔", "单次", "每笔"))
    has_amount_like = _contains_any(text, ("transaction", "spend", "expense", "amount", "消费", "费用", "金额", "支出"))
    has_top = _ranking_question(text)
    return (has_single and has_amount_like) or (has_top and _contains_any(text, ("单笔", "消费", "支出")))


def _item_ranking_question(chat_text: str) -> bool:
    text = str(chat_text or "")
    has_item = _contains_any(text, ("item", "items", "消费项", "计费项", "billing item", "项目"))
    has_amount = _contains_any(text, ("amount", "fee", "cost", "费用", "金额", "消费"))
    has_top = _ranking_question(text)
    explicit_other_dimension = _contains_any(text, ("service", "product", "region", "地域", "地区", "区域", "服务", "产品", "商品"))
    return has_item and has_amount and has_top and not explicit_other_dimension


def _trend_question(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        ("trend", "monthly", "daily", "weekly", "quarterly", "按月", "每月", "按天", "每天", "按周", "每周", "按季度", "每季度", "趋势", "月份"),
    )


def _compare_question(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        (
            "对比",
            "比较",
            "相比",
            "环比",
            "同比",
            "和上个月",
            "和去年同期",
            "vs",
            "versus",
            "compare",
            "comparison",
            "compared with",
            "last month",
            "previous month",
            "previous period",
            "year over year",
            "yoy",
            "mom",
            "difference",
            "变化了多少",
            "涨了多少",
            "降了多少",
        ),
    )


def _year_over_year_compare(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        (
            "同比",
            "去年同期",
            "year over year",
            "yoy",
        ),
    )


def _ratio_compare(chat_text: str) -> bool:
    text = str(chat_text or "")
    return _contains_any(
        text,
        (
            "占比",
            "比例",
            "比率",
            "占多少",
            "占到",
            "ratio",
            "percentage",
            "percent",
        ),
    ) or "%" in text


def _delta_compare(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        (
            "差值",
            "差额",
            "变化",
            "变动",
            "变化了多少",
            "涨了多少",
            "降了多少",
            "difference",
            "delta",
            "increase",
            "decrease",
        ),
    )


def _distinct_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("distinct", "去重", "不重复", "唯一"))


def _share_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("share", "占比", "构成", "份额", "pie", "饼图"))


def _count_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("count", "数量", "次数", "人数", "多少个", "多少条", "记录数"))


def _amount_question(chat_text: str) -> bool:
    return _contains_any(chat_text, ("amount", "price", "cost", "fee", "金额", "费用", "消费", "应付", "花费"))


def _weekday_weekend_question(chat_text: str) -> bool:
    text = str(chat_text or "")
    return "工作日" in text and "周末" in text and _amount_question(text)


def _text_mode_followup(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        (
            "text",
            "words",
            "written",
            "table",
            "list",
            "文本",
            "文字",
            "文字说明",
            "文字回答",
            "文本回答",
            "表格",
            "列表",
            "明细",
        ),
    )


def _followup_question(chat_text: str) -> bool:
    return _contains_any(
        chat_text,
        (
            "continue",
            "same",
            "again",
            "also",
            "instead",
            "switch to",
            "change to",
            "keep the same",
            "继续",
            "接着",
            "再",
            "同样",
            "换成",
            "改成",
            "改为",
            "只看",
            "仅看",
            "还是这个",
            "基于上一个",
        ),
    )


def _explicit_analysis_request(chat_text: str) -> bool:
    return any(
        (
            _row_count_question(chat_text),
            _count_question(chat_text),
            _day_count_question(chat_text),
            _forecast_question(chat_text),
            _total_amount_question(chat_text),
            _average_amount_question(chat_text),
            _distinct_question(chat_text),
            _single_transaction_question(chat_text),
            _item_ranking_question(chat_text),
            _trend_question(chat_text),
            _share_question(chat_text),
            _ranking_question(chat_text),
            _detail_question(chat_text),
        )
    )
