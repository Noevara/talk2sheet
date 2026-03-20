from __future__ import annotations

import re
from typing import Any

from ..core.schema import ChartSpec, Metric, SelectionPlan, TransformPlan


def _contains_any(text: str, tokens: list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(token.lower() in lowered for token in tokens)


def _detail_intent(question: str) -> bool:
    return _contains_any(question, ["detail", "明细", "records", "原始记录", "逐条记录", "前10条", "前 10 条", "top rows"])


def _dedup_intent(question: str) -> bool:
    return _contains_any(question, ["去重", "distinct", "不重复", "唯一"])


def _plain_row_count_intent(question: str) -> bool:
    q = str(question or "")
    if not q.strip():
        return False
    if _detail_intent(q):
        return False
    return bool(
        re.search(r"how many rows", q, re.I)
        or _contains_any(q, ["row count", "多少行", "多少条", "总记录数", "件数", "几行", "几条"])
    )


def _aggregate_intent(question: str) -> bool:
    return _contains_any(
        question,
        [
            "total",
            "sum",
            "average",
            "avg",
            "top",
            "rank",
            "趋势",
            "trend",
            "按月",
            "每月",
            "total amount",
            "总金额",
            "平均",
            "排行",
            "排名",
            "图表",
            "chart",
        ],
    )


def sanitize_selection_plan(plan: SelectionPlan, question: str, *, mode: str) -> tuple[SelectionPlan, dict[str, Any]]:
    data = plan.model_dump()
    changes: list[dict[str, Any]] = []

    if mode == "chart":
        if data.get("limit") is not None:
            changes.append({"field": "limit", "from": data["limit"], "to": None, "reason": "chart_mode_full_aggregation"})
            data["limit"] = None
        if data.get("distinct_by") and not _dedup_intent(question):
            changes.append({"field": "distinct_by", "from": data["distinct_by"], "to": None, "reason": "chart_mode_no_distinct"})
            data["distinct_by"] = None
        if data.get("sort") is not None:
            changes.append({"field": "sort", "from": data["sort"], "to": None, "reason": "chart_mode_post_aggregation_sort"})
            data["sort"] = None
    elif _aggregate_intent(question) and not _detail_intent(question):
        if data.get("limit") is not None:
            changes.append({"field": "limit", "from": data["limit"], "to": None, "reason": "aggregate_question_no_pre_limit"})
            data["limit"] = None
        if data.get("distinct_by") and not _dedup_intent(question):
            changes.append({"field": "distinct_by", "from": data["distinct_by"], "to": None, "reason": "aggregate_question_no_distinct"})
            data["distinct_by"] = None

    if _plain_row_count_intent(question):
        for field_name, replacement in (("filters", []), ("distinct_by", None), ("sort", None), ("limit", None)):
            if data.get(field_name):
                changes.append({"field": field_name, "from": data[field_name], "to": replacement, "reason": "plain_row_count_simplify"})
                data[field_name] = replacement

    return SelectionPlan.model_validate(data), {"changes": changes}


def sanitize_transform_plan(plan: TransformPlan, question: str, df: Any, *, mode: str) -> tuple[TransformPlan, dict[str, Any]]:
    data = plan.model_dump()
    changes: list[dict[str, Any]] = []

    if data.get("return_rows"):
        if data.get("groupby"):
            changes.append({"field": "groupby", "from": data["groupby"], "to": [], "reason": "detail_mode_no_groupby"})
            data["groupby"] = []
        if data.get("metrics"):
            changes.append({"field": "metrics", "from": data["metrics"], "to": [], "reason": "detail_mode_no_metrics"})
            data["metrics"] = []

    if not data.get("return_rows") and not data.get("metrics"):
        changes.append({"field": "metrics", "from": [], "to": [{"agg": "count_rows", "col": None, "as_name": "count"}], "reason": "default_metric"})
        data["metrics"] = [Metric(agg="count_rows", col=None, as_name="count").model_dump()]

    if _plain_row_count_intent(question):
        canonical = TransformPlan(metrics=[Metric(agg="count_rows", col=None, as_name="row_count")])
        changes.append({"field": "transform", "from": data, "to": canonical.model_dump(), "reason": "plain_row_count_canonical"})
        data = canonical.model_dump()

    top_k = data.get("top_k")
    if isinstance(top_k, int):
        bounded = max(1, min(top_k, 50))
        if bounded != top_k:
            changes.append({"field": "top_k", "from": top_k, "to": bounded, "reason": "top_k_clamped"})
            data["top_k"] = bounded

    order_by = data.get("order_by")
    if order_by and not data.get("return_rows") and not data.get("groupby") and len(data.get("metrics") or []) == 1:
        metric_alias = (data["metrics"][0] or {}).get("as_name")
        if metric_alias and order_by.get("col") != metric_alias:
            changes.append({"field": "order_by", "from": order_by, "to": {"col": metric_alias, "dir": order_by.get("dir", "desc")}, "reason": "single_metric_order"})
            data["order_by"] = {"col": metric_alias, "dir": order_by.get("dir", "desc")}

    return TransformPlan.model_validate(data), {"changes": changes}


def sanitize_chart_spec(spec: ChartSpec, question: str, df: Any) -> tuple[ChartSpec, dict[str, Any]]:
    data = spec.model_dump()
    changes: list[dict[str, Any]] = []

    allowed_types = {"line", "bar", "pie", "scatter"}
    if data.get("type") not in allowed_types:
        changes.append({"field": "type", "from": data.get("type"), "to": "bar", "reason": "unsupported_chart_type"})
        data["type"] = "bar"

    top_k = data.get("top_k")
    if isinstance(top_k, int):
        bounded = max(1, min(top_k, 50))
        if bounded != top_k:
            changes.append({"field": "top_k", "from": top_k, "to": bounded, "reason": "chart_top_k_clamped"})
            data["top_k"] = bounded

    if not data.get("title"):
        data["title"] = str(question or "").strip() or "Chart"
        changes.append({"field": "title", "from": None, "to": data["title"], "reason": "default_title"})

    return ChartSpec.model_validate(data), {"changes": changes}
