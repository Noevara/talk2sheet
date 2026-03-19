from __future__ import annotations

from typing import Any

import math
import pandas as pd

from ..core.i18n import t
from ..planning.intent_accessors import analysis_intent_kind, analysis_intent_payload, analysis_intent_target_dimension
from .answer_models import AnswerGeneratorContext, GeneratedAnswer
from .formatters import (
    _chart_clause,
    _columns_summary,
    _dimension_column,
    _finalize_generated_answer,
    _forecast_grain_label,
    _forecast_model_label,
    _format_number,
    _format_value,
    _metric_column,
    _metric_label,
    _period_compare_direction,
    _rank_label,
    _risk_note_from_disclosure,
    _row_summary,
    _safe_scalar,
    _summary_sentence_for_explain,
    _top_items_summary,
    _trend_change_clause,
)
from .templates import _ta


class RuleBasedAnswerGenerator:
    name = "rule-v1"

    def generate(self, context: AnswerGeneratorContext) -> GeneratedAnswer:
        locale = context.locale
        intent = analysis_intent_kind(context.draft, fallback=context.draft.intent)
        result_df = context.result_df
        base_meta = {
            "provider": self.name,
            "intent": intent,
            "analysis_intent": analysis_intent_payload(context.draft),
            "result_row_count": int(len(result_df.index)),
            "result_column_count": int(len(result_df.columns)),
            "chart_enabled": context.chart_spec is not None,
            "empty_result": bool(result_df.empty),
        }

        if intent == "unsupported":
            message = t(locale, "unsupported")
            return self._finalize(
                context,
                GeneratedAnswer(answer=message, analysis_text=message, meta={**base_meta, "summary_kind": "unsupported"}),
            )

        if result_df.empty:
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "no_data"),
                    analysis_text=_ta(locale, "no_data_detail"),
                    meta={**base_meta, "summary_kind": "empty"},
                ),
            )

        if intent == "row_count":
            value = int(float(_safe_scalar(result_df, self._first_existing_metric(context)) or 0))
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "row_count_answer", value=value),
                    analysis_text=_ta(locale, "row_count_analysis"),
                    meta={**base_meta, "summary_kind": "scalar", "value": value},
                ),
            )

        if intent == "count_distinct":
            metric_column = self._first_existing_metric(context)
            value = int(float(_safe_scalar(result_df, metric_column) or 0))
            label = self._selection_label(context, fallback="value")
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "distinct_count_answer", column=label, value=value),
                    analysis_text=_ta(locale, "distinct_count_analysis", column=label),
                    meta={**base_meta, "summary_kind": "scalar", "value": value, "label": label},
                ),
            )

        if intent == "active_day_count":
            metric_column = self._first_existing_metric(context)
            value = int(float(_safe_scalar(result_df, metric_column) or 0))
            period = str(context.draft.planner_meta.get("requested_period") or "")
            label = str((context.draft.planner_meta.get("bucket_name") or "day")).strip()
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "active_day_count_answer", period=period or label, value=value),
                    analysis_text=_ta(locale, "active_day_count_analysis", period=period or label, column=label),
                    meta={**base_meta, "summary_kind": "active_day_count", "value": value, "period": period, "label": label},
                ),
            )

        if intent == "total_amount":
            metric_column = self._first_existing_metric(context)
            value = _format_number(_safe_scalar(result_df, metric_column) or 0)
            label = self._selection_label(context, fallback=_metric_label(context.transform_plan))
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "total_amount_answer", column=label, value=value),
                    analysis_text=_ta(locale, "total_amount_analysis", column=label),
                    meta={**base_meta, "summary_kind": "scalar", "value": value, "label": label},
                ),
            )

        if intent == "average_amount":
            metric_column = self._first_existing_metric(context)
            value = _format_number(_safe_scalar(result_df, metric_column) or 0)
            label = self._selection_label(context, fallback=_metric_label(context.transform_plan))
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "average_amount_answer", column=label, value=value),
                    analysis_text=_ta(locale, "average_amount_analysis", column=label),
                    meta={**base_meta, "summary_kind": "scalar", "value": value, "label": label},
                ),
            )

        if intent == "forecast_timeseries":
            forecast_meta = context.transform_meta.get("forecast") if isinstance(context.transform_meta, dict) else {}
            period_column = str(forecast_meta.get("period_column") or (result_df.columns[0] if len(result_df.columns) else "period"))
            forecast_period = str(_safe_scalar(result_df, period_column) or forecast_meta.get("target_period") or "")
            forecast_value = _format_number(_safe_scalar(result_df, "forecast_value") or forecast_meta.get("forecast_value") or 0)
            lower_bound = _format_number(_safe_scalar(result_df, "lower_bound") or forecast_meta.get("lower_bound") or 0)
            upper_bound = _format_number(_safe_scalar(result_df, "upper_bound") or forecast_meta.get("upper_bound") or 0)
            model_label = _forecast_model_label(locale, str(forecast_meta.get("model") or ""))
            grain_label = _forecast_grain_label(locale, str(forecast_meta.get("grain") or "month"))
            history_points = int(forecast_meta.get("history_points") or 0)
            history_start = str(forecast_meta.get("history_start") or "")
            history_end = str(forecast_meta.get("history_end") or "")
            target_count = int(forecast_meta.get("target_count") or len(result_df.index) or 0)
            is_multi_step = bool(forecast_meta.get("multi_step")) or target_count > 1 or len(result_df.index) > 1
            start_period = str(forecast_meta.get("target_start_period") or (result_df.iloc[0][period_column] if len(result_df.index) else forecast_period) or "")
            end_period = str(forecast_meta.get("target_end_period") or (result_df.iloc[-1][period_column] if len(result_df.index) else forecast_period) or "")
            total_value = _format_number(forecast_meta.get("forecast_value") or _safe_scalar(result_df, "forecast_value") or 0)
            average_value = _format_number(forecast_meta.get("average_forecast_value") or forecast_meta.get("forecast_value") or 0)
            if is_multi_step:
                display_period = start_period if start_period == end_period else f"{start_period} 至 {end_period}"
                return self._finalize(
                    context,
                    GeneratedAnswer(
                        answer=_ta(
                            locale,
                            "forecast_series_answer",
                            start_period=start_period,
                            end_period=end_period,
                            count=target_count,
                            grain=grain_label,
                        ),
                        analysis_text=_ta(
                            locale,
                            "forecast_series_analysis",
                            start_period=start_period,
                            end_period=end_period,
                            count=target_count,
                            total_value=total_value,
                            average_value=average_value,
                            model=model_label,
                            grain=grain_label,
                            history_points=history_points,
                            history_start=history_start,
                            history_end=history_end,
                        ),
                        meta={
                            **base_meta,
                            "summary_kind": "forecast_timeseries",
                            "period": display_period,
                            "forecast_value": total_value,
                            "model": str(forecast_meta.get("model") or ""),
                            "lower_bound": _format_number(forecast_meta.get("lower_bound") or 0),
                            "upper_bound": _format_number(forecast_meta.get("upper_bound") or 0),
                            "target_count": target_count,
                            "multi_step": True,
                        },
                        segments={
                            "conclusion": _ta(
                                locale,
                                "forecast_series_answer",
                                start_period=start_period,
                                end_period=end_period,
                                count=target_count,
                                grain=grain_label,
                            ),
                            "evidence": _ta(
                                locale,
                                "forecast_series_analysis",
                                start_period=start_period,
                                end_period=end_period,
                                count=target_count,
                                total_value=total_value,
                                average_value=average_value,
                                model=model_label,
                                grain=grain_label,
                                history_points=history_points,
                                history_start=history_start,
                                history_end=history_end,
                            ),
                            "risk_note": _ta(
                                locale,
                                "forecast_series_risk",
                                lower=_format_number(forecast_meta.get("lower_bound") or 0),
                                upper=_format_number(forecast_meta.get("upper_bound") or 0),
                            ),
                        },
                    ),
                )
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "forecast_answer", period=forecast_period, value=forecast_value),
                    analysis_text=_ta(
                        locale,
                        "forecast_analysis",
                        period=forecast_period,
                        value=forecast_value,
                        model=model_label,
                        grain=grain_label,
                        history_points=history_points,
                        history_start=history_start,
                        history_end=history_end,
                    ),
                    meta={
                        **base_meta,
                        "summary_kind": "forecast_timeseries",
                        "period": forecast_period,
                        "forecast_value": forecast_value,
                        "model": str(forecast_meta.get("model") or ""),
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound,
                    },
                    segments={
                        "conclusion": _ta(locale, "forecast_answer", period=forecast_period, value=forecast_value),
                        "evidence": _ta(
                            locale,
                            "forecast_analysis",
                            period=forecast_period,
                            value=forecast_value,
                            model=model_label,
                            grain=grain_label,
                            history_points=history_points,
                            history_start=history_start,
                            history_end=history_end,
                        ),
                        "risk_note": _ta(locale, "forecast_risk", lower=lower_bound, upper=upper_bound),
                    },
                ),
            )

        if intent == "what_if_reduction":
            matched_value = _format_number(_safe_scalar(result_df, "matched_amount") or 0)
            reduction_value = _format_number(_safe_scalar(result_df, "reduction_amount") or 0)
            target = str(context.draft.planner_meta.get("what_if_target_value") or "target")
            target_column = str(context.draft.planner_meta.get("what_if_target_column") or self._selection_label(context, fallback="target"))
            percent = _format_number(context.draft.planner_meta.get("what_if_percent") or 0)
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(locale, "what_if_reduction_answer", target=target, percent=percent, reduction_value=reduction_value),
                    analysis_text=_ta(
                        locale,
                        "what_if_reduction_analysis",
                        target_column=target_column,
                        target=target,
                        matched_value=matched_value,
                        percent=percent,
                        reduction_value=reduction_value,
                    ),
                    meta={
                        **base_meta,
                        "summary_kind": "what_if_reduction",
                        "target": target,
                        "target_column": target_column,
                        "percent": percent,
                        "matched_value": matched_value,
                        "reduction_value": reduction_value,
                    },
                ),
            )

        if intent == "ranking":
            summary = self._ranking_summary(context)
            if summary is not None:
                return self._finalize(context, GeneratedAnswer(answer=summary["answer"], analysis_text=summary["analysis_text"], meta={**base_meta, **summary["meta"]}))

        if intent == "share":
            summary = self._share_summary(context)
            if summary is not None:
                return self._finalize(context, GeneratedAnswer(answer=summary["answer"], analysis_text=summary["analysis_text"], meta={**base_meta, **summary["meta"]}))

        if intent == "weekpart_compare":
            summary = self._weekpart_compare_summary(context)
            if summary is not None:
                return self._finalize(context, GeneratedAnswer(answer=summary["answer"], analysis_text=summary["analysis_text"], meta={**base_meta, **summary["meta"]}))

        if intent == "period_compare":
            summary = self._period_compare_summary(context)
            if summary is not None:
                return self._finalize(context, GeneratedAnswer(answer=summary["answer"], analysis_text=summary["analysis_text"], meta={**base_meta, **summary["meta"]}))

        if intent == "period_breakdown":
            summary = self._period_breakdown_summary(context)
            if summary is not None:
                return self._finalize(context, GeneratedAnswer(answer=summary["answer"], analysis_text=summary["analysis_text"], meta={**base_meta, **summary["meta"]}))

        if intent == "trend":
            summary = self._trend_summary(context)
            if summary is not None:
                return self._finalize(context, GeneratedAnswer(answer=summary["answer"], analysis_text=summary["analysis_text"], meta={**base_meta, **summary["meta"]}))

        if intent == "explain_ranked_item":
            target = str(context.draft.planner_meta.get("explain_target_value") or "")
            dimension = str(context.draft.planner_meta.get("explain_dimension_column") or self._selection_label(context, fallback="item"))
            count = int(len(result_df.index))
            sort_column = context.selection_plan.sort.col if context.selection_plan.sort is not None else (
                context.selection_plan.columns[-1] if context.selection_plan.columns else _metric_label(context.transform_plan)
            )
            summary_sentence = _summary_sentence_for_explain(context, target=target or dimension, dimension=dimension)
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(context.locale, "explain_item_answer", target=target or dimension, count=count),
                    analysis_text=_ta(
                        context.locale,
                        "explain_item_analysis",
                        dimension=dimension,
                        target=target or dimension,
                        count=count,
                        summary_sentence=summary_sentence,
                        row_summary=_row_summary(context.result_df),
                    ),
                    meta={
                        **base_meta,
                        "summary_kind": "explain_item",
                        "target": target,
                        "dimension_column": dimension,
                        "sort_column": sort_column,
                        "summary_sentence": summary_sentence,
                    },
                ),
            )

        if intent == "explain_breakdown":
            summary = self._explain_breakdown_summary(context)
            if summary is not None:
                return self._finalize(context, GeneratedAnswer(answer=summary["answer"], analysis_text=summary["analysis_text"], meta={**base_meta, **summary["meta"]}))

        if intent == "ranked_item_lookup":
            dimension = str(context.draft.planner_meta.get("ranked_item_dimension_column") or self._selection_label(context, fallback="item"))
            target = str(context.draft.planner_meta.get("ranked_item_value") or _safe_scalar(result_df, dimension) or "")
            value_column = _metric_column(result_df, plan=context.transform_plan, chart_spec=context.chart_spec)
            target_value = _format_number(_safe_scalar(result_df, value_column)) if value_column else _format_value(context.draft.planner_meta.get("ranked_item_metric_value"))
            rank_position = int(context.draft.planner_meta.get("rank_position") or 1)
            rank_label = _rank_label(context.locale, rank_position)
            return self._finalize(
                context,
                GeneratedAnswer(
                    answer=_ta(context.locale, "ranked_item_answer", rank_position=rank_label, target=target or dimension, target_value=target_value, dimension=dimension),
                    analysis_text=_ta(context.locale, "ranked_item_analysis", rank_position=rank_label, target=target or dimension, target_value=target_value, dimension=dimension),
                    meta={
                        **base_meta,
                        "summary_kind": "ranked_item_lookup",
                        "rank_position": rank_position,
                        "target": target,
                        "dimension_column": dimension,
                        "target_value": target_value,
                    },
                ),
            )

        if context.transform_plan.return_rows or intent == "detail_rows":
            return self._finalize(context, self._detail_summary(context, base_meta))

        if context.transform_plan.pivot is not None:
            return self._finalize(context, self._pivot_summary(context, base_meta))

        return self._finalize(context, self._table_summary(context, base_meta))

    def _finalize(self, context: AnswerGeneratorContext, result: GeneratedAnswer) -> GeneratedAnswer:
        return _finalize_generated_answer(
            answer=result.answer,
            analysis_text=result.analysis_text,
            meta=result.meta,
            conclusion=result.segments.get("conclusion") if result.segments else result.answer,
            evidence=result.segments.get("evidence") if result.segments else result.analysis_text,
            risk_note=result.segments.get("risk_note") if result.segments else _risk_note_from_disclosure(context.execution_disclosure),
        )

    def _first_existing_metric(self, context: AnswerGeneratorContext) -> str | None:
        return _metric_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec)

    def _selection_label(self, context: AnswerGeneratorContext, *, fallback: str) -> str:
        structured_dimension = analysis_intent_target_dimension(context.draft)
        if structured_dimension:
            return structured_dimension
        if context.selection_plan.columns:
            return str(context.selection_plan.columns[0])
        return fallback

    def _ranking_summary(self, context: AnswerGeneratorContext) -> dict[str, Any] | None:
        value_column = _metric_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec)
        dimension_column = analysis_intent_target_dimension(context.draft) or _dimension_column(
            context.result_df,
            plan=context.transform_plan,
            chart_spec=context.chart_spec,
            value_column=value_column,
        )
        if value_column is None or dimension_column is None or value_column not in context.result_df.columns:
            return None
        values = pd.to_numeric(context.result_df[value_column], errors="coerce")
        valid = context.result_df.loc[values.notna()]
        if valid.empty:
            return None
        leader = valid.iloc[0]
        runner_clause = ""
        if len(valid.index) > 1:
            runner = valid.iloc[1]
            runner_clause = _ta(context.locale, "runner_clause", runner=_format_value(runner[dimension_column]), runner_value=_format_number(runner[value_column]))
        limit = int(context.transform_plan.top_k or (context.chart_spec.top_k if context.chart_spec and context.chart_spec.top_k else len(valid.index)))
        top_items = _top_items_summary(valid, dimension_column=dimension_column, value_column=value_column, limit=limit)
        analysis_text = _ta(
            context.locale,
            "ranking_analysis",
            limit=limit,
            dimension=dimension_column,
            leader=_format_value(leader[dimension_column]),
            leader_value=_format_number(leader[value_column]),
            runner_clause=runner_clause,
            chart_clause=_chart_clause(context.locale, context.chart_spec),
        )
        if top_items != "-":
            analysis_text = f"{analysis_text} {_ta(context.locale, 'ranking_top_list', limit=min(limit, len(valid.index)), items=top_items)}"
        return {
            "answer": _ta(context.locale, "ranking_answer", leader=_format_value(leader[dimension_column]), leader_value=_format_number(leader[value_column])),
            "analysis_text": analysis_text,
            "meta": {
                "summary_kind": "ranking",
                "dimension_column": dimension_column,
                "value_column": value_column,
                "leader": _format_value(leader[dimension_column]),
                "leader_value": _format_number(leader[value_column]),
                "top_items": top_items,
            },
        }

    def _share_summary(self, context: AnswerGeneratorContext) -> dict[str, Any] | None:
        value_column = _metric_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec)
        dimension_column = _dimension_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec, value_column=value_column)
        if value_column is None or dimension_column is None:
            return None
        values = pd.to_numeric(context.result_df[value_column], errors="coerce")
        valid = context.result_df.loc[values.notna()].copy()
        if valid.empty:
            return None
        total = float(values.loc[valid.index].sum())
        if abs(total) < 1e-9:
            return self._ranking_summary(context)
        leader = valid.iloc[0]
        leader_share = f"{(float(leader[value_column]) / total) * 100:.1f}%"
        runner_clause = ""
        if len(valid.index) > 1:
            runner = valid.iloc[1]
            runner_share = f"{(float(runner[value_column]) / total) * 100:.1f}%"
            runner_clause = _ta(
                context.locale,
                "runner_share_clause",
                runner=_format_value(runner[dimension_column]),
                runner_share=runner_share,
                runner_value=_format_number(runner[value_column]),
            )
        limit = int(context.transform_plan.top_k or (context.chart_spec.top_k if context.chart_spec and context.chart_spec.top_k else len(valid.index)))
        top_items = []
        for _, row in valid.head(limit).iterrows():
            share = f"{(float(row[value_column]) / total) * 100:.1f}%"
            top_items.append(f"{_format_value(row[dimension_column])}（{share}）")
        top_list_text = "、".join(top_items) if context.locale == "zh-CN" else ", ".join(top_items)
        analysis_text = _ta(
            context.locale,
            "share_analysis",
            dimension=dimension_column,
            leader=_format_value(leader[dimension_column]),
            leader_share=leader_share,
            leader_value=_format_number(leader[value_column]),
            runner_clause=runner_clause,
            chart_clause=_chart_clause(context.locale, context.chart_spec),
        )
        if top_list_text:
            analysis_text = f"{analysis_text} {_ta(context.locale, 'share_top_list', limit=min(limit, len(valid.index)), items=top_list_text)}"
        return {
            "answer": _ta(context.locale, "share_answer", leader=_format_value(leader[dimension_column]), leader_share=leader_share),
            "analysis_text": analysis_text,
            "meta": {
                "summary_kind": "share",
                "dimension_column": dimension_column,
                "value_column": value_column,
                "leader": _format_value(leader[dimension_column]),
                "leader_share": leader_share,
                "leader_value": _format_number(leader[value_column]),
                "top_items": top_items,
            },
        }

    def _trend_summary(self, context: AnswerGeneratorContext) -> dict[str, Any] | None:
        value_column = _metric_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec)
        dimension_column = _dimension_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec, value_column=value_column)
        if value_column is None or dimension_column is None:
            return None
        values = pd.to_numeric(context.result_df[value_column], errors="coerce")
        valid = context.result_df.loc[values.notna()].copy()
        if valid.empty:
            return None
        valid_values = values.loc[valid.index]
        peak_index = valid_values.idxmax()
        peak_row = valid.loc[peak_index]
        latest_row = valid.iloc[-1]
        metric_label = _metric_label(context.transform_plan, fallback=value_column)
        return {
            "answer": _ta(context.locale, "trend_answer", peak_period=_format_value(peak_row[dimension_column]), metric=metric_label, peak_value=_format_number(peak_row[value_column])),
            "analysis_text": _ta(
                context.locale,
                "trend_analysis",
                point_count=int(len(valid.index)),
                metric=metric_label,
                peak_period=_format_value(peak_row[dimension_column]),
                peak_value=_format_number(peak_row[value_column]),
                latest_period=_format_value(latest_row[dimension_column]),
                latest_value=_format_number(latest_row[value_column]),
                change_clause=_trend_change_clause(context.locale, valid_values),
                chart_clause=_chart_clause(context.locale, context.chart_spec),
            ),
            "meta": {
                "summary_kind": "trend",
                "dimension_column": dimension_column,
                "value_column": value_column,
                "peak_period": _format_value(peak_row[dimension_column]),
                "peak_value": _format_number(peak_row[value_column]),
                "latest_period": _format_value(latest_row[dimension_column]),
                "latest_value": _format_number(latest_row[value_column]),
            },
        }

    def _weekpart_compare_summary(self, context: AnswerGeneratorContext) -> dict[str, Any] | None:
        value_column = _metric_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec)
        dimension_column = _dimension_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec, value_column=value_column)
        if value_column is None or dimension_column is None:
            return None
        values = pd.to_numeric(context.result_df[value_column], errors="coerce")
        valid = context.result_df.loc[values.notna()].copy()
        if valid.empty:
            return None
        normalized_dimension = valid[dimension_column].astype(str)
        preferred_order = []
        for label in ("工作日", "Weekday", "平日", "周末", "Weekend"):
            matched = valid.loc[normalized_dimension == label]
            if not matched.empty:
                preferred_order.append(matched.iloc[0])
        if len(preferred_order) >= 2:
            leader_row = max(preferred_order, key=lambda row: float(row[value_column]))
            other_row = min(preferred_order, key=lambda row: float(row[value_column]))
        else:
            leader_row = valid.iloc[0]
            other_row = valid.iloc[1] if len(valid.index) > 1 else valid.iloc[0]
        metric_label = _metric_label(context.transform_plan, fallback=value_column)
        return {
            "answer": _ta(context.locale, "weekpart_compare_answer", leader_label=_format_value(leader_row[dimension_column]), leader_value=_format_number(leader_row[value_column]), other_label=_format_value(other_row[dimension_column]), other_value=_format_number(other_row[value_column])),
            "analysis_text": _ta(
                context.locale,
                "weekpart_compare_analysis",
                metric=metric_label,
                leader_label=_format_value(leader_row[dimension_column]),
                leader_value=_format_number(leader_row[value_column]),
                other_label=_format_value(other_row[dimension_column]),
                other_value=_format_number(other_row[value_column]),
                chart_clause=_chart_clause(context.locale, context.chart_spec),
            ),
            "meta": {
                "summary_kind": "weekpart_compare",
                "dimension_column": dimension_column,
                "value_column": value_column,
                "leader_label": _format_value(leader_row[dimension_column]),
                "leader_value": _format_number(leader_row[value_column]),
                "other_label": _format_value(other_row[dimension_column]),
                "other_value": _format_number(other_row[value_column]),
            },
        }

    def _period_compare_summary(self, context: AnswerGeneratorContext) -> dict[str, Any] | None:
        result_df = context.result_df
        current_period = str(context.draft.planner_meta.get("current_period") or "")
        previous_period = str(context.draft.planner_meta.get("previous_period") or "")
        if result_df.empty or not current_period or not previous_period:
            return None
        if current_period not in result_df.columns or previous_period not in result_df.columns:
            return None
        row = result_df.iloc[0]
        current_value_raw = row[current_period]
        previous_value_raw = row[previous_period]
        change_value_raw = row["change_value"] if "change_value" in result_df.columns else None
        change_pct_raw = row["change_pct"] if "change_pct" in result_df.columns else None
        try:
            delta = float(change_value_raw or 0)
        except Exception:
            delta = 0.0
        change_pct = "-"
        try:
            pct_value = float(change_pct_raw)
            if math.isfinite(pct_value):
                change_pct = f"{pct_value * 100:.1f}%"
        except Exception:
            pass
        metric_label = str(context.draft.planner_meta.get("compare_metric_column") or _metric_label(context.transform_plan, fallback="value"))
        return {
            "answer": _ta(
                context.locale,
                "period_compare_answer",
                previous_period=previous_period,
                current_period=current_period,
                previous_value=_format_number(previous_value_raw),
                current_value=_format_number(current_value_raw),
                change_value=_format_number(abs(delta)),
                change_pct=change_pct,
                direction=_period_compare_direction(context.locale, delta),
            ),
            "analysis_text": _ta(
                context.locale,
                "period_compare_analysis",
                metric=metric_label,
                previous_period=previous_period,
                current_period=current_period,
                previous_value=_format_number(previous_value_raw),
                current_value=_format_number(current_value_raw),
            ),
            "meta": {
                "summary_kind": "period_compare",
                "current_period": current_period,
                "previous_period": previous_period,
                "current_value": _format_number(current_value_raw),
                "previous_value": _format_number(previous_value_raw),
                "change_value": _format_number(change_value_raw),
                "change_pct": change_pct,
            },
        }

    def _period_breakdown_summary(self, context: AnswerGeneratorContext) -> dict[str, Any] | None:
        value_column = _metric_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec)
        dimension_column = _dimension_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec, value_column=value_column)
        if value_column is None or dimension_column is None or context.result_df.empty:
            return None
        valid = context.result_df.copy()
        valid[value_column] = pd.to_numeric(valid[value_column], errors="coerce")
        valid = valid.dropna(subset=[value_column])
        if valid.empty:
            return None
        if context.locale == "zh-CN":
            items = "；".join(f"{_format_value(row[dimension_column])}为 {_format_number(row[value_column])}" for _, row in valid.iterrows())
        else:
            items = ", ".join(f"{_format_value(row[dimension_column])} = {_format_number(row[value_column])}" for _, row in valid.iterrows())
        metric_label = _metric_label(context.transform_plan, fallback=value_column)
        return {
            "answer": _ta(context.locale, "period_breakdown_answer", items=items),
            "analysis_text": _ta(context.locale, "period_breakdown_analysis", metric=metric_label, items=items, chart_clause=_chart_clause(context.locale, context.chart_spec)),
            "meta": {
                "summary_kind": "period_breakdown",
                "dimension_column": dimension_column,
                "value_column": value_column,
                "items": items,
            },
        }

    def _explain_breakdown_summary(self, context: AnswerGeneratorContext) -> dict[str, Any] | None:
        value_column = _metric_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec)
        dimension_column = _dimension_column(context.result_df, plan=context.transform_plan, chart_spec=context.chart_spec, value_column=value_column)
        if value_column is None or dimension_column is None or context.result_df.empty:
            return None
        values = pd.to_numeric(context.result_df[value_column], errors="coerce")
        valid = context.result_df.loc[values.notna()]
        if valid.empty:
            return None
        leader = valid.iloc[0]
        runner_clause = ""
        if len(valid.index) > 1:
            runner = valid.iloc[1]
            runner_clause = _ta(context.locale, "runner_clause", runner=_format_value(runner[dimension_column]), runner_value=_format_number(runner[value_column]))
        target = str(context.draft.planner_meta.get("breakdown_target_value") or "")
        target_dimension = str(context.draft.planner_meta.get("breakdown_target_dimension") or "")
        return {
            "answer": _ta(context.locale, "explain_breakdown_answer", target=target or target_dimension, dimension=dimension_column, leader=_format_value(leader[dimension_column]), leader_value=_format_number(leader[value_column])),
            "analysis_text": _ta(
                context.locale,
                "explain_breakdown_analysis",
                target_dimension=target_dimension or "target",
                target=target or target_dimension,
                dimension=dimension_column,
                leader=_format_value(leader[dimension_column]),
                leader_value=_format_number(leader[value_column]),
                runner_clause=runner_clause,
            ),
            "meta": {
                "summary_kind": "explain_breakdown",
                "target": target,
                "target_dimension": target_dimension,
                "dimension_column": dimension_column,
                "value_column": value_column,
                "leader": _format_value(leader[dimension_column]),
                "leader_value": _format_number(leader[value_column]),
            },
        }

    def _detail_summary(self, context: AnswerGeneratorContext, base_meta: dict[str, Any]) -> GeneratedAnswer:
        sort_column = context.selection_plan.sort.col if context.selection_plan.sort is not None else (
            context.selection_plan.columns[-1] if context.selection_plan.columns else _metric_label(context.transform_plan)
        )
        count = int(len(context.result_df.index))
        return GeneratedAnswer(
            answer=_ta(context.locale, "detail_answer", count=count, column=sort_column),
            analysis_text=_ta(context.locale, "detail_analysis", count=count, column=sort_column, row_summary=_row_summary(context.result_df)),
            meta={**base_meta, "summary_kind": "detail", "sort_column": sort_column},
        )

    def _pivot_summary(self, context: AnswerGeneratorContext, base_meta: dict[str, Any]) -> GeneratedAnswer:
        pivot = context.transform_plan.pivot
        assert pivot is not None
        index_columns = [column for column in (pivot.index or []) if column in context.result_df.columns]
        value_columns = [str(column) for column in context.result_df.columns if column not in index_columns]
        index_label = ", ".join(index_columns) if index_columns else "rows"
        row_count = int(len(context.result_df.index))
        return GeneratedAnswer(
            answer=_ta(context.locale, "pivot_answer", row_count=row_count, value_column_count=int(len(value_columns))),
            analysis_text=_ta(
                context.locale,
                "pivot_analysis",
                value_label=pivot.values,
                pivot_column=pivot.columns,
                index_label=index_label,
                row_summary=_row_summary(context.result_df, preferred_columns=index_columns + value_columns),
            ),
            meta={**base_meta, "summary_kind": "pivot", "pivot_index_columns": index_columns, "pivot_value_columns": value_columns},
        )

    def _table_summary(self, context: AnswerGeneratorContext, base_meta: dict[str, Any]) -> GeneratedAnswer:
        row_count = int(len(context.result_df.index))
        column_count = int(len(context.result_df.columns))
        columns = _columns_summary(context.result_df)
        analysis_key = "table_analysis" if row_count else "table_analysis_no_row"
        analysis_kwargs = {"columns": columns}
        if row_count:
            analysis_kwargs["row_summary"] = _row_summary(context.result_df)
        return GeneratedAnswer(
            answer=_ta(context.locale, "table_answer", row_count=row_count, column_count=column_count),
            analysis_text=_ta(context.locale, analysis_key, **analysis_kwargs),
            meta={**base_meta, "summary_kind": "table", "columns": [str(column) for column in context.result_df.columns]},
        )
