from __future__ import annotations

import re
from typing import Any

from ..core.schema import ChartSpec, DerivedColumn, Filter, FormulaMetric, Metric, SelectionPlan, Sort, TransformPlan
from ..pipeline.column_profile import get_column_profiles, normalize_text


def _contains_any(text: str, tokens: tuple[str, ...] | list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(token.lower() in lowered for token in tokens)


def _extract_top_k(question: str, default: int = 10, upper: int = 50) -> int:
    text = str(question or "")
    match = re.search(
        r"top\s*(\d+)|前\s*(\d+)|最多\s*(\d+)|最大\s*(\d+)|最高\s*(\d+)|largest\s*(\d+)|biggest\s*(\d+)",
        text,
        flags=re.I,
    )
    if not match:
        return default
    for group in match.groups():
        if group:
            return max(1, min(int(group), upper))
    return default


def _extract_percent(question: str) -> float | None:
    text = str(question or "")
    match = re.search(r"(\d+(?:\.\d+)?)\s*%", text)
    if match:
        return float(match.group(1))
    match = re.search(r"降低\s*(\d+(?:\.\d+)?)", text)
    if match:
        return float(match.group(1))
    return None


def _extract_chart_type(question: str, default: str = "bar") -> str:
    text = str(question or "").lower()

    def has_word(token: str) -> bool:
        return re.search(rf"\b{re.escape(token)}\b", text, flags=re.I) is not None

    if "饼图" in text or "占比" in text or has_word("pie") or has_word("share"):
        return "pie"
    if "折线" in text or "趋势" in text or has_word("line") or has_word("trend"):
        return "line"
    if "散点" in text or has_word("scatter"):
        return "scatter"
    return default


def _pick_amount_column(df: Any) -> str | None:
    profiles = get_column_profiles(df)
    preferred = (
        "应付信息/应付金额（含税）",
        "应付信息/应付金额",
        "应付金额（含税）",
        "应付金额",
        "Amount",
    )
    columns = list(getattr(df, "columns", []))
    for column in preferred:
        if column in columns:
            return column
    for column, profile in profiles.items():
        hints = set(str(item) for item in (profile.get("semantic_hints") or []))
        if profile.get("semantic_type") == "numeric" and "amount" in hints:
            return column
    for column, profile in profiles.items():
        if profile.get("semantic_type") == "numeric":
            return column
    return None


def _pick_date_column(df: Any) -> str | None:
    profiles = get_column_profiles(df)
    preferred = ("账单信息/账单日期", "账单信息/消费时间", "Date", "日期", "时间")
    columns = list(getattr(df, "columns", []))
    for column in preferred:
        if column in columns:
            return column
    for column, profile in profiles.items():
        if profile.get("semantic_type") == "date":
            return column
    return None


def _pick_id_column(df: Any) -> str | None:
    profiles = get_column_profiles(df)
    for column, profile in profiles.items():
        if profile.get("semantic_type") == "id":
            return column
    return None


def _pick_named_dimension(
    df: Any,
    weighted_name_tokens: tuple[tuple[str, int], ...],
    *,
    exclude: set[str] | None = None,
) -> str | None:
    exclude = exclude or set()
    profiles = get_column_profiles(df)
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
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][2]


def _pick_service_column(df: Any, *, exclude: set[str] | None = None) -> str | None:
    return _pick_named_dimension(
        df,
        (
            ("servicename", 14),
            ("service", 12),
            ("服务名称", 14),
            ("服务主体", 12),
            ("产品名称", 12),
            ("productname", 10),
            ("product", 8),
            ("商品名称", 10),
        ),
        exclude=exclude,
    )


def _pick_billing_item_column(df: Any, *, exclude: set[str] | None = None) -> str | None:
    return _pick_named_dimension(
        df,
        (
            ("billingitemname", 14),
            ("itemname", 13),
            ("billingitem", 13),
            ("item", 11),
            ("计费项名称", 14),
            ("计费项", 13),
            ("消费项", 12),
        ),
        exclude=exclude,
    )


def _pick_region_column(df: Any, *, exclude: set[str] | None = None) -> str | None:
    return _pick_named_dimension(
        df,
        (
            ("region", 14),
            ("location", 10),
            ("地域", 15),
            ("地区", 13),
            ("区域", 12),
            ("城市", 10),
        ),
        exclude=exclude,
    )


def _pick_category_column(df: Any, *, exclude: set[str] | None = None) -> str | None:
    exclude = exclude or set()
    profiles = get_column_profiles(df)
    for column, profile in profiles.items():
        if column in exclude:
            continue
        if str(profile.get("semantic_type") or "") in {"categorical", "text"}:
            return column
    for column in [str(item) for item in getattr(df, "columns", [])]:
        if column not in exclude:
            return column
    return None


def _question_overlap_score(question: str, candidate: str, aliases: list[str] | None = None) -> int:
    normalized_question = normalize_text(question)
    if not normalized_question:
        return 0
    best = 0
    for value in [candidate, *(aliases or [])]:
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


def _pick_question_dimension_column(df: Any, question: str, *, exclude: set[str] | None = None) -> str | None:
    exclude = exclude or set()
    text = str(question or "")
    if _contains_any(text, ("billing item", "计费项")):
        column = _pick_billing_item_column(df, exclude=exclude)
        if column:
            return column
    if _contains_any(text, ("service", "product", "服务", "产品", "商品")):
        column = _pick_service_column(df, exclude=exclude)
        if column:
            return column
    if _contains_any(text, ("region", "location", "地域", "地区", "区域", "城市")):
        column = _pick_region_column(df, exclude=exclude)
        if column:
            return column

    profiles = get_column_profiles(df)
    ranked: list[tuple[int, int, str]] = []
    for column, profile in profiles.items():
        if column in exclude:
            continue
        semantic_type = str(profile.get("semantic_type") or "")
        if semantic_type not in {"categorical", "text", "date", "id", "unknown"}:
            continue
        aliases = [str(item) for item in (profile.get("aliases") or []) if str(item or "").strip()]
        score = _question_overlap_score(text, column, aliases=aliases)
        if score <= 0:
            continue
        semantic_bonus = 20 if semantic_type in {"categorical", "text"} else 12 if semantic_type == "date" else 0
        ranked.append((score + semantic_bonus, len(column), column))
    if ranked:
        ranked.sort(key=lambda item: (item[0], -item[1], item[2]), reverse=True)
        if ranked[0][0] >= 18:
            return ranked[0][2]
    return _pick_category_column(df, exclude=exclude)


def _extract_date_literal(question: str) -> str | None:
    text = str(question or "")
    for pattern in (
        r"(\d{4}-\d{1,2}-\d{1,2})",
        r"(\d{4}/\d{1,2}/\d{1,2})",
        r"(\d{4}年\d{1,2}月\d{1,2}日)",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        raw = match.group(1)
        normalized = raw.replace("/", "-").replace("年", "-").replace("月", "-").replace("日", "")
        parts = normalized.split("-")
        if len(parts) == 3:
            year, month, day = parts
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    return None


def _match_question_value(df: Any, column: str, question: str) -> str | None:
    if column not in getattr(df, "columns", []):
        return None
    normalized_question = normalize_text(question)
    candidates: list[tuple[int, str]] = []
    seen: set[str] = set()
    for value in list(df[column].dropna().astype(str).head(300)):
        clean = str(value).strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        normalized_value = normalize_text(clean)
        if not normalized_value:
            continue
        score = 0
        if normalized_value in normalized_question:
            score += 100
        score += sum(1 for ch in set(normalized_value) if ch in normalized_question)
        if score > 0:
            candidates.append((score, clean))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], len(item[1]), item[1]), reverse=True)
    return candidates[0][1]


def _is_row_count_question(question: str) -> bool:
    return _contains_any(question, ("how many rows", "row count", "多少行", "多少条", "总记录数", "件数"))


def _is_total_amount_question(question: str) -> bool:
    return _contains_any(question, ("total amount", "sum", "总金额", "总费用", "总应付", "总消费"))


def _is_average_amount_question(question: str) -> bool:
    return _contains_any(question, ("average", "avg", "平均金额", "平均费用", "平均值"))


def _is_share_question(question: str) -> bool:
    return _contains_any(question, ("share", "占比", "构成", "份额", "pie", "饼图"))


def _is_trend_question(question: str) -> bool:
    return _contains_any(question, ("trend", "monthly", "按月", "每月", "趋势", "月份"))


def _is_ranking_question(question: str) -> bool:
    return _contains_any(question, ("top", "rank", "排行", "排名", "前", "最多", "最大", "最高", "largest", "biggest"))


def _is_detail_question(question: str) -> bool:
    return _contains_any(question, ("detail", "明细", "top rows", "records", "记录", "前10条", "前5条"))


def _is_amount_question(question: str) -> bool:
    return _contains_any(question, ("amount", "price", "cost", "fee", "金额", "费用", "消费", "应付", "花费"))


def _is_count_question(question: str) -> bool:
    return _contains_any(question, ("count", "数量", "次数", "人数", "多少个", "多少条", "记录数"))


def _is_weekpart_question(question: str) -> bool:
    text = str(question or "")
    return "工作日" in text and "周末" in text and _is_amount_question(text)


def _time_grain_from_question(question: str) -> str | None:
    text = str(question or "")
    if "工作日" in text and "周末" in text:
        return "weekpart"
    if _contains_any(text, ("按天", "每天", "每日", "日度")):
        return "day"
    if _contains_any(text, ("星期", "周几")):
        return "weekday"
    if _contains_any(text, ("季度", "按季度", "每季度")):
        return "quarter"
    if _contains_any(text, ("按周", "每周", "周度")):
        return "week"
    if _contains_any(text, ("按月", "每月", "月度", "月份")):
        return "month"
    return None


def _bucket_alias(grain: str) -> str:
    return {
        "day": "date_bucket",
        "week": "week_bucket",
        "month": "month",
        "quarter": "quarter_bucket",
        "weekday": "weekday",
        "weekpart": "weekpart",
    }.get(grain, "date_bucket")


def _is_date_breakdown_question(question: str) -> bool:
    text = str(question or "")
    return _extract_date_literal(text) is not None and _contains_any(
        text,
        ("哪项", "什么费用", "主要", "来源", "构成", "最高项", "top", "排行", "ranking"),
    )


def _is_what_if_reduction_question(question: str) -> bool:
    text = str(question or "")
    return _contains_any(text, ("如果", "假如", "若", "if")) and _contains_any(text, ("降低", "减少", "reduce", "drop")) and _extract_percent(text) is not None


def _try_fast_path_impl(df: Any, *, question: str, mode: str) -> dict[str, Any] | None:
    amount_column = _pick_amount_column(df)
    date_column = _pick_date_column(df)
    id_column = _pick_id_column(df)
    exclude = {column for column in (amount_column, date_column, id_column) if column}
    dimension_column = _pick_question_dimension_column(df, question, exclude=exclude)
    service_column = _pick_service_column(df, exclude=exclude)
    billing_item_column = _pick_billing_item_column(df, exclude=exclude)
    region_column = _pick_region_column(df, exclude=exclude)

    if _is_row_count_question(question):
        return {
            "intent": "row_count",
            "mode": "text",
            "selection_plan": SelectionPlan(),
            "transform_plan": TransformPlan(metrics=[Metric(agg="count_rows", as_name="row_count")]),
            "planner_meta": {"fast_path": "row_count"},
        }

    if _is_what_if_reduction_question(question) and amount_column:
        target_column = billing_item_column or service_column or dimension_column
        target_value = _match_question_value(df, target_column, question) if target_column else None
        reduction_percent = _extract_percent(question)
        if target_column and target_value and reduction_percent is not None:
            ratio = reduction_percent / 100.0
            return {
                "intent": "what_if_reduction",
                "mode": "text",
                "selection_plan": SelectionPlan(
                    columns=[target_column, amount_column],
                    filters=[Filter(col=target_column, op="=", value=target_value)],
                ),
                "transform_plan": TransformPlan(
                    metrics=[Metric(agg="sum", col=amount_column, as_name="matched_amount")],
                    formula_metrics=[FormulaMetric(as_name="reduction_amount", op="mul", left="matched_amount", right=str(ratio))],
                ),
                "planner_meta": {
                    "fast_path": "what_if_reduction",
                    "what_if_target_column": target_column,
                    "what_if_target_value": target_value,
                    "what_if_percent": reduction_percent,
                },
            }

    if _is_total_amount_question(question) and amount_column:
        return {
            "intent": "total_amount",
            "mode": "text",
            "selection_plan": SelectionPlan(columns=[amount_column]),
            "transform_plan": TransformPlan(metrics=[Metric(agg="sum", col=amount_column, as_name="total_amount")]),
            "planner_meta": {"fast_path": "total_amount", "amount_column": amount_column},
        }

    if _is_average_amount_question(question) and amount_column:
        return {
            "intent": "average_amount",
            "mode": "text",
            "selection_plan": SelectionPlan(columns=[amount_column]),
            "transform_plan": TransformPlan(metrics=[Metric(agg="avg", col=amount_column, as_name="avg_amount")]),
            "planner_meta": {"fast_path": "average_amount", "amount_column": amount_column},
        }

    if _is_date_breakdown_question(question) and amount_column and date_column:
        target_date = _extract_date_literal(question)
        target_dimension = billing_item_column or dimension_column or service_column or region_column
        if target_date and target_dimension:
            return {
                "intent": "ranking",
                "mode": "chart" if mode == "chart" else "text",
                "selection_plan": SelectionPlan(
                    columns=[date_column, target_dimension, amount_column],
                    filters=[Filter(col=date_column, op="=", value=target_date)],
                ),
                "transform_plan": TransformPlan(
                    groupby=[target_dimension],
                    metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                    order_by=Sort(col="value", dir="desc"),
                    top_k=_extract_top_k(question, default=5),
                ),
                "chart_spec": ChartSpec(type="bar", title=question.strip() or "Date breakdown", x=target_dimension, y="value", top_k=_extract_top_k(question, default=5))
                if mode == "chart"
                else None,
                "planner_meta": {
                    "fast_path": "date_breakdown",
                    "date_filter_column": date_column,
                    "date_filter_value": target_date,
                    "ranking_column": target_dimension,
                },
            }

    if _is_weekpart_question(question) and amount_column and date_column:
        resolved_mode = "chart" if mode == "chart" else "text"
        chart_type = _extract_chart_type(question, default="bar")
        return {
            "intent": "weekpart_compare",
            "mode": resolved_mode,
            "selection_plan": SelectionPlan(columns=[date_column, amount_column]),
            "transform_plan": TransformPlan(
                derived_columns=[DerivedColumn(as_name="weekpart", kind="date_bucket", source_col=date_column, grain="weekpart")],
                groupby=["weekpart"],
                metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                order_by=Sort(col="value", dir="desc"),
                top_k=2,
            ),
            "chart_spec": ChartSpec(type=chart_type, title=question.strip() or "Weekday vs weekend", x="weekpart", y="value", top_k=2)
            if resolved_mode == "chart"
            else None,
            "planner_meta": {"fast_path": "weekpart_compare", "amount_column": amount_column, "date_column": date_column},
        }

    if _is_trend_question(question) and amount_column and date_column:
        grain = _time_grain_from_question(question) or "month"
        bucket_name = _bucket_alias(grain)
        resolved_mode = "chart" if mode == "chart" else "text"
        return {
            "intent": "trend",
            "mode": resolved_mode,
            "selection_plan": SelectionPlan(columns=[date_column, amount_column]),
            "transform_plan": TransformPlan(
                derived_columns=[DerivedColumn(as_name=bucket_name, kind="date_bucket", source_col=date_column, grain=grain)],
                groupby=[bucket_name],
                metrics=[Metric(agg="sum", col=amount_column, as_name="value")],
                order_by=Sort(col=bucket_name, dir="asc"),
                top_k=24,
            ),
            "chart_spec": ChartSpec(type="line", title=question.strip() or "Trend", x=bucket_name, y="value", top_k=24)
            if resolved_mode == "chart"
            else None,
            "planner_meta": {"fast_path": f"{grain}_trend", "bucket_name": bucket_name},
        }

    if _is_detail_question(question) and amount_column:
        return {
            "intent": "detail_rows",
            "mode": "text",
            "selection_plan": SelectionPlan(sort=Sort(col=amount_column, dir="desc"), limit=_extract_top_k(question, default=10)),
            "transform_plan": TransformPlan(return_rows=True),
            "planner_meta": {"fast_path": "detail_rows", "sort_column": amount_column},
        }

    if (_is_share_question(question) or _is_ranking_question(question)) and (dimension_column or service_column or billing_item_column or region_column):
        ranking_column = dimension_column or service_column or billing_item_column or region_column
        if _contains_any(question, ("计费项", "billing item")) and billing_item_column:
            ranking_column = billing_item_column
        elif _contains_any(question, ("服务", "产品", "商品", "service", "product")) and service_column:
            ranking_column = service_column
        elif _contains_any(question, ("地域", "地区", "区域", "城市", "region", "location")) and region_column:
            ranking_column = region_column
        top_k = _extract_top_k(question, default=5 if _is_ranking_question(question) else 8)
        if _is_count_question(question) and not _is_amount_question(question):
            metric = Metric(agg="count_distinct", col=id_column, as_name="value") if id_column else Metric(agg="count_rows", as_name="value")
        else:
            metric = Metric(agg="sum", col=amount_column, as_name="value") if amount_column else Metric(agg="count_rows", as_name="value")
        intent = "share" if _is_share_question(question) else "ranking"
        resolved_mode = "chart" if mode == "chart" else "text"
        default_chart = "pie" if intent == "share" else "bar"
        return {
            "intent": intent,
            "mode": resolved_mode,
            "selection_plan": SelectionPlan(columns=[ranking_column] + ([metric.col] if metric.col else [])),
            "transform_plan": TransformPlan(
                groupby=[ranking_column],
                metrics=[metric],
                order_by=Sort(col="value", dir="desc"),
                top_k=top_k,
            ),
            "chart_spec": ChartSpec(type=_extract_chart_type(question, default=default_chart), title=question.strip() or intent.title(), x=ranking_column, y="value", top_k=top_k)
            if resolved_mode == "chart"
            else None,
            "planner_meta": {
                "fast_path": intent,
                "ranking_column": ranking_column,
                "top_k": top_k,
            },
        }

    return None


def try_text_fast_path(df: Any, *, question: str) -> dict[str, Any] | None:
    payload = _try_fast_path_impl(df, question=question, mode="text")
    if not isinstance(payload, dict):
        return None
    payload["mode"] = "text"
    payload["chart_spec"] = None
    return payload


def try_chart_fast_path(df: Any, *, question: str) -> dict[str, Any] | None:
    payload = _try_fast_path_impl(df, question=question, mode="chart")
    if not isinstance(payload, dict):
        return None
    payload["mode"] = "chart"
    if payload.get("chart_spec") is None:
        return None
    return payload


def try_fast_path(df: Any, *, question: str, mode: str) -> dict[str, Any] | None:
    return try_chart_fast_path(df, question=question) if mode == "chart" else try_text_fast_path(df, question=question)
