from __future__ import annotations

from typing import Any

from app.config import get_settings

from ..planning.intent_accessors import analysis_intent_kind, analysis_intent_payload
from .memory_store import InMemoryDataframeCache, InMemorySessionStore
from .store_types import ConversationSession, DataframeCache, SessionStore


def _truncate_text(value: str, limit: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _contains_any(text: str, tokens: tuple[str, ...] | list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(token.lower() in lowered for token in tokens)


def _is_explanation_like_question(chat_text: str) -> bool:
    return _contains_any(
        str(chat_text or "").strip(),
        (
            "为什么",
            "原因",
            "分析一下",
            "分析",
            "为何",
            "怎么理解",
            "是什么意思",
            "什么东西",
            "是什么",
            "explain",
            "why",
            "reason",
            "analyze",
            "what is",
        ),
    )


def _is_sheet_switch_followup(chat_text: str) -> bool:
    return _sheet_reference_hint(chat_text) in {"another", "previous"}


def _sheet_reference_hint(chat_text: str) -> str:
    text = str(chat_text or "").strip()
    if not text:
        return "auto"
    if _contains_any(
        text,
        (
            "previous sheet",
            "last sheet",
            "back to previous sheet",
            "go back to previous sheet",
            "上一个sheet",
            "上一张sheet",
            "上一个工作表",
            "上一张工作表",
            "回到上一个sheet",
            "回到上一个工作表",
            "前一个sheet",
            "前一个工作表",
            "前のシート",
            "戻って前のシート",
        ),
    ):
        return "previous"
    if _contains_any(
        text,
        (
            "another sheet",
            "other sheet",
            "next sheet",
            "switch sheet",
            "different sheet",
            "另一个sheet",
            "另外一个sheet",
            "另一个工作表",
            "另外一个工作表",
            "换个sheet",
            "换一个sheet",
            "再看另一个",
            "别的sheet",
            "其他sheet",
            "別のシート",
            "他のシート",
            "別シート",
        ),
    ):
        return "another"
    if _contains_any(
        text,
        (
            "current sheet",
            "this sheet",
            "keep current sheet",
            "当前sheet",
            "这个sheet",
            "当前工作表",
            "这个工作表",
            "本sheet",
            "本工作表",
            "今のシート",
            "現在のシート",
        ),
    ):
        return "current"
    return "auto"


def _is_followup_question(chat_text: str) -> bool:
    text = str(chat_text or "").strip()
    if not text:
        return False
    explicit_analysis = _contains_any(
        text,
        (
            "how many rows",
            "row count",
            "total amount",
            "sum",
            "average",
            "avg",
            "trend",
            "monthly",
            "share",
            "distinct",
            "top",
            "rank",
            "排行",
            "排名",
            "多少行",
            "多少条",
            "总金额",
            "总费用",
            "总应付",
            "总消费",
            "平均金额",
            "平均费用",
            "平均值",
            "趋势",
            "按月",
            "每月",
            "占比",
            "构成",
            "份额",
            "去重",
            "不重复",
            "唯一",
            "明细",
            "记录",
            "前",
            "最多",
            "最大",
            "最高",
        ),
    )
    if _contains_any(
        text,
        (
            "continue",
            "same",
            "again",
            "also",
            "instead",
            "switch to",
            "change to",
            "keep the same",
            "what about",
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
            "上面",
            "刚才",
            "前一个",
            "这个呢",
            "图表呢",
            "文字呢",
        ),
    ):
        return True
    if _is_explanation_like_question(text):
        return True
    return len(text) <= 16 and not explicit_analysis


def _safe_dict(value: Any) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, dict) else None


def _safe_list_of_str(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item or "").strip()]


def _safe_list_of_dict(value: Any, *, limit: int = 8) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    output: list[dict[str, Any]] = []
    for item in value[: max(1, int(limit))]:
        if isinstance(item, dict):
            output.append(dict(item))
    return output


def _safe_clarification_resolution(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    kind = str(value.get("kind") or "").strip()
    selected_value = str(value.get("selected_value") or value.get("value") or "").strip()
    source_field = str(value.get("source_field") or value.get("field") or "").strip()
    if not selected_value:
        return None
    payload: dict[str, Any] = {
        "kind": kind or "column_resolution",
        "selected_value": selected_value,
    }
    if source_field:
        payload["source_field"] = source_field
    return payload


def _safe_anchor_filters(value: Any, *, limit: int = 3) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    filters: list[dict[str, str]] = []
    for item in value[: max(1, int(limit))]:
        if not isinstance(item, dict):
            continue
        column = str(item.get("column") or item.get("col") or "").strip()
        op = str(item.get("op") or "=").strip() or "="
        item_value = str(item.get("value") or "").strip()
        if not column or not item_value:
            continue
        filters.append(
            {
                "column": column,
                "op": op,
                "value": item_value,
            }
        )
    return filters


def _safe_analysis_anchor(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    payload: dict[str, Any] = {
        "intent": str(value.get("intent") or "").strip(),
        "metric_column": str(value.get("metric_column") or "").strip(),
        "metric_agg": str(value.get("metric_agg") or "").strip(),
        "metric_alias": str(value.get("metric_alias") or "").strip(),
        "dimension_column": str(value.get("dimension_column") or "").strip(),
        "time_column": str(value.get("time_column") or "").strip(),
        "time_grain": str(value.get("time_grain") or "").strip(),
        "filters": _safe_anchor_filters(value.get("filters")),
    }
    selection_plan = _safe_dict(value.get("selection_plan"))
    transform_plan = _safe_dict(value.get("transform_plan"))
    chart_spec = _safe_dict(value.get("chart_spec"))
    if selection_plan:
        payload["selection_plan"] = selection_plan
    if transform_plan:
        payload["transform_plan"] = transform_plan
    if chart_spec:
        payload["chart_spec"] = chart_spec
    normalized_payload: dict[str, Any] = {}
    for key, item in payload.items():
        if item is None:
            continue
        if isinstance(item, str) and not item.strip():
            continue
        if isinstance(item, list) and not item:
            continue
        normalized_payload[key] = item
    payload = normalized_payload
    if not payload:
        return None
    if not any(key in payload for key in {"metric_column", "dimension_column", "time_column", "filters", "intent"}):
        return None
    return payload


def _derive_analysis_anchor_from_turn(
    *,
    intent: str,
    pipeline: dict[str, Any],
    pipeline_summary: dict[str, Any],
    chart_spec: dict[str, Any] | None,
) -> dict[str, Any] | None:
    selection_plan = _safe_dict(pipeline.get("selection_plan")) or {}
    transform_plan = _safe_dict(pipeline.get("transform_plan")) or {}
    metrics = transform_plan.get("metrics") if isinstance(transform_plan.get("metrics"), list) else []
    first_metric = metrics[0] if metrics and isinstance(metrics[0], dict) else {}
    groupby = transform_plan.get("groupby") if isinstance(transform_plan.get("groupby"), list) else []
    derived_columns = transform_plan.get("derived_columns") if isinstance(transform_plan.get("derived_columns"), list) else []

    time_column = ""
    time_grain = ""
    for item in derived_columns:
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "") != "date_bucket":
            continue
        time_column = str(item.get("source_col") or "").strip()
        time_grain = str(item.get("grain") or "").strip()
        if time_column or time_grain:
            break

    anchor_payload: dict[str, Any] = {
        "intent": str(intent or pipeline_summary.get("intent") or "").strip(),
        "metric_column": str(first_metric.get("col") or "").strip(),
        "metric_agg": str(first_metric.get("agg") or "").strip(),
        "metric_alias": str(first_metric.get("as_name") or "").strip(),
        "dimension_column": str(groupby[0] or "").strip() if groupby else "",
        "time_column": time_column,
        "time_grain": time_grain,
        "filters": _safe_anchor_filters(selection_plan.get("filters")),
    }
    if selection_plan:
        anchor_payload["selection_plan"] = selection_plan
    if transform_plan:
        anchor_payload["transform_plan"] = transform_plan
    safe_chart_spec = _safe_dict(pipeline.get("chart_spec")) or chart_spec or {}
    if safe_chart_spec:
        anchor_payload["chart_spec"] = safe_chart_spec
    return _safe_analysis_anchor(anchor_payload)


def _build_visited_sheets(turns: list[dict[str, Any]], *, limit: int = 6) -> list[dict[str, Any]]:
    visited: list[dict[str, Any]] = []
    seen: set[int] = set()
    for turn in turns:
        sheet_index = int(turn.get("sheet_index") or 0)
        sheet_name = str(turn.get("sheet_name") or "").strip()
        if sheet_index <= 0 or sheet_index in seen:
            continue
        seen.add(sheet_index)
        visited.append({"sheet_index": sheet_index, "sheet_name": sheet_name})
    return visited[-max(1, int(limit)) :]


def _build_recent_sheet_trajectory(turns: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    trajectory: list[dict[str, Any]] = []
    for turn in turns:
        sheet_index = int(turn.get("sheet_index") or 0)
        sheet_name = str(turn.get("sheet_name") or "").strip()
        if sheet_index <= 0:
            continue
        if trajectory and int(trajectory[-1].get("sheet_index") or 0) == sheet_index:
            continue
        trajectory.append({"sheet_index": sheet_index, "sheet_name": sheet_name})
    return trajectory[-max(1, int(limit)) :]


def _previous_sheet_from_trajectory(trajectory: list[dict[str, Any]]) -> tuple[int, str]:
    if len(trajectory) < 2:
        return 0, ""
    previous = trajectory[-2]
    return int(previous.get("sheet_index") or 0), str(previous.get("sheet_name") or "")


def _build_pipeline_summary(
    *,
    requested_mode: str,
    result_mode: str,
    pipeline: dict[str, Any],
    chart_spec: dict[str, Any] | None,
) -> dict[str, Any]:
    planner = pipeline.get("planner") if isinstance(pipeline, dict) else {}
    transform_plan = _safe_dict(pipeline.get("transform_plan")) or {}
    selection_plan = _safe_dict(pipeline.get("selection_plan")) or {}
    chart_payload = _safe_dict(pipeline.get("chart_spec")) or chart_spec or {}
    metrics = transform_plan.get("metrics") if isinstance(transform_plan.get("metrics"), list) else []
    metric_aliases = [
        str(item.get("as_name") or item.get("col") or "")
        for item in metrics
        if isinstance(item, dict) and str(item.get("as_name") or item.get("col") or "").strip()
    ]
    sheet_routing = _safe_dict(pipeline.get("sheet_routing")) or {}
    sheet_sequence = _safe_dict(pipeline.get("sheet_sequence")) or {}
    return {
        "requested_mode": requested_mode,
        "mode": result_mode,
        "intent": analysis_intent_kind(planner, fallback=str((planner or {}).get("intent") or "")),
        "analysis_intent": analysis_intent_payload(planner),
        "source_sheet_index": int(pipeline.get("source_sheet_index") or 0),
        "source_sheet_name": str(pipeline.get("source_sheet_name") or ""),
        "selected_columns": _safe_list_of_str(selection_plan.get("columns")),
        "filter_count": len(selection_plan.get("filters") or []) if isinstance(selection_plan.get("filters"), list) else 0,
        "groupby": _safe_list_of_str(transform_plan.get("groupby")),
        "metric_aliases": metric_aliases,
        "return_rows": bool(transform_plan.get("return_rows")),
        "top_k": transform_plan.get("top_k"),
        "chart_type": str(chart_payload.get("type") or ""),
        "chart_x": str(chart_payload.get("x") or ""),
        "chart_y": str(chart_payload.get("y") or ""),
        "result_columns": _safe_list_of_str(pipeline.get("result_columns")),
        "result_row_count": int(pipeline.get("result_row_count") or 0),
        "sheet_switch_reason": str(
            sheet_sequence.get("last_sheet_switch_reason")
            or sheet_routing.get("reason")
            or ""
        ),
        "sheet_switched": bool(sheet_sequence.get("switched_from_previous")),
        "visited_sheets": _safe_list_of_str(
            [str((item or {}).get("sheet_name") or "") for item in (sheet_sequence.get("visited_sheets") or []) if isinstance(item, dict)]
        ),
        "status": str(pipeline.get("status") or "ok"),
    }


class ConversationStore:
    def __init__(
        self,
        *,
        max_sessions: int,
        max_turns: int,
        cache_ttl_seconds: int = 0,
        cache_max_entries: int = 1,
        session_store: SessionStore | None = None,
        dataframe_cache: DataframeCache | None = None,
    ) -> None:
        self.max_sessions = max(1, int(max_sessions))
        self.max_turns = max(1, int(max_turns))
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds))
        self.cache_max_entries = max(1, int(cache_max_entries))
        self._session_store = session_store or InMemorySessionStore(
            max_sessions=self.max_sessions,
            max_turns=self.max_turns,
        )
        self._dataframe_cache = dataframe_cache or InMemoryDataframeCache(
            ttl_seconds=self.cache_ttl_seconds,
            max_entries=self.cache_max_entries,
        )

    def ensure_session(
        self,
        *,
        conversation_id: str | None,
        file_id: str,
        sheet_index: int,
        locale: str,
    ) -> tuple[ConversationSession, bool]:
        result = self._session_store.ensure_session(
            conversation_id=conversation_id,
            file_id=file_id,
            sheet_index=sheet_index,
            locale=locale,
        )
        if result.reset:
            self._dataframe_cache.clear_conversation(result.session.conversation_id)
        for evicted_conversation_id in result.evicted_conversation_ids:
            self._dataframe_cache.clear_conversation(evicted_conversation_id)
        return result.session, result.reset

    def append_turn(self, session: ConversationSession, turn_summary: dict[str, Any]) -> None:
        self._session_store.append_turn(session, turn_summary)

    def get_cached_dataframe(
        self,
        session: ConversationSession,
        *,
        cache_key: str,
        cache_token: str,
    ) -> tuple[Any, str] | None:
        return self._dataframe_cache.get(
            session.conversation_id,
            cache_key=cache_key,
            cache_token=cache_token,
        )

    def set_cached_dataframe(
        self,
        session: ConversationSession,
        *,
        cache_key: str,
        cache_token: str,
        dataframe: Any,
        sheet_name: str,
    ) -> None:
        self._dataframe_cache.set(
            session.conversation_id,
            cache_key=cache_key,
            cache_token=cache_token,
            dataframe=dataframe,
            sheet_name=sheet_name,
        )

    def build_followup_context(
        self,
        session: ConversationSession | None,
        *,
        chat_text: str = "",
        clarification_resolution: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        safe_resolution = _safe_clarification_resolution(clarification_resolution)
        if session is None or not session.turns:
            return None
        recent_turns = [dict(turn) for turn in session.turns[-3:]]
        last_turn = dict(session.turns[-1])
        recent_pipeline_history: list[dict[str, Any]] = []
        for turn in recent_turns:
            recent_pipeline_history.append(
                {
                    "question": str(turn.get("question") or ""),
                    "mode": str(turn.get("mode") or ""),
                    "intent": str(turn.get("intent") or ""),
                    "pipeline_summary": _safe_dict(turn.get("pipeline_summary")) or {},
                    "chart_spec": _safe_dict(turn.get("chart_spec")),
                    "result_columns": _safe_list_of_str(turn.get("result_columns")),
                    "result_row_count": int(turn.get("result_row_count") or 0),
                    "answer_summary": str(turn.get("answer_summary") or ""),
                    "execution_disclosure": _safe_dict(turn.get("execution_disclosure")),
                }
            )
        visited_sheets = _build_visited_sheets(session.turns)
        recent_sheet_trajectory = _build_recent_sheet_trajectory(session.turns)
        previous_sheet_index, previous_sheet_name = _previous_sheet_from_trajectory(recent_sheet_trajectory)
        sheet_reference_hint = _sheet_reference_hint(chat_text)
        analysis_anchor = _safe_analysis_anchor(last_turn.get("analysis_anchor"))
        payload = {
            "conversation_id": session.conversation_id,
            "turn_count": len(session.turns),
            "file_id": session.file_id,
            "sheet_index": int(session.sheet_index),
            "sheet_name": str(session.sheet_name or ""),
            "locale": session.locale,
            "is_followup": _is_followup_question(chat_text) or safe_resolution is not None,
            "wants_sheet_switch": _is_sheet_switch_followup(chat_text),
            "wants_previous_sheet": sheet_reference_hint == "previous",
            "wants_current_sheet": sheet_reference_hint == "current",
            "sheet_reference_hint": sheet_reference_hint,
            "last_mode": str(last_turn.get("mode") or ""),
            "last_sheet_index": int(last_turn.get("sheet_index") or 0),
            "last_sheet_name": str(last_turn.get("sheet_name") or ""),
            "previous_sheet_index": previous_sheet_index,
            "previous_sheet_name": previous_sheet_name,
            "last_sheet_switch_reason": str(last_turn.get("sheet_switch_reason") or ""),
            "visited_sheets": visited_sheets,
            "recent_sheet_trajectory": recent_sheet_trajectory,
            "last_pipeline_summary": _safe_dict(last_turn.get("pipeline_summary")) or {},
            "last_chart_spec": _safe_dict(last_turn.get("chart_spec")),
            "last_result_columns": _safe_list_of_str(last_turn.get("result_columns")),
            "last_result_row_count": int(last_turn.get("result_row_count") or 0),
            "last_task_steps": _safe_list_of_dict(last_turn.get("task_steps")),
            "last_current_step_id": str(last_turn.get("current_step_id") or ""),
            "analysis_anchor": analysis_anchor,
            "last_turn": last_turn,
            "recent_turns": recent_turns,
            "recent_pipeline_history": recent_pipeline_history,
        }
        if safe_resolution is not None:
            payload["clarification_resolution"] = safe_resolution
        return payload

    def get_session(self, conversation_id: str) -> ConversationSession | None:
        return self._session_store.get(str(conversation_id))

    def clear(self) -> None:
        self._session_store.clear()
        self._dataframe_cache.clear()


def build_turn_summary(
    *,
    question: str,
    requested_mode: str,
    result_mode: str,
    pipeline: dict[str, Any],
    answer: str,
    analysis_text: str | None,
    chart_spec: dict[str, Any] | None,
    execution_disclosure: dict[str, Any] | None = None,
) -> dict[str, Any]:
    planner = pipeline.get("planner") if isinstance(pipeline, dict) else {}
    pipeline_summary = _build_pipeline_summary(
        requested_mode=requested_mode,
        result_mode=result_mode,
        pipeline=pipeline,
        chart_spec=chart_spec,
    )
    sheet_routing = (pipeline.get("sheet_routing") or {}) if isinstance(pipeline.get("sheet_routing"), dict) else {}
    sheet_sequence = (pipeline.get("sheet_sequence") or {}) if isinstance(pipeline.get("sheet_sequence"), dict) else {}
    sheet_switch_reason = str(sheet_sequence.get("last_sheet_switch_reason") or sheet_routing.get("reason") or "")
    turn_intent = analysis_intent_kind(planner, fallback=str((planner or {}).get("intent") or ""))
    analysis_anchor = _derive_analysis_anchor_from_turn(
        intent=turn_intent,
        pipeline=pipeline,
        pipeline_summary=pipeline_summary,
        chart_spec=chart_spec,
    ) or _safe_analysis_anchor(pipeline.get("analysis_anchor"))
    return {
        "question": _truncate_text(question, 300),
        "requested_mode": requested_mode,
        "mode": result_mode,
        "intent": turn_intent,
        "analysis_intent": analysis_intent_payload(planner),
        "sheet_index": int(pipeline.get("source_sheet_index") or ((pipeline.get("sheet_routing") or {}) if isinstance(pipeline.get("sheet_routing"), dict) else {}).get("resolved_sheet_index") or 1),
        "sheet_name": str(pipeline.get("source_sheet_name") or ((pipeline.get("sheet_routing") or {}) if isinstance(pipeline.get("sheet_routing"), dict) else {}).get("resolved_sheet_name") or ""),
        "sheet_switch_reason": sheet_switch_reason,
        "task_steps": _safe_list_of_dict(pipeline.get("task_steps")),
        "current_step_id": str(pipeline.get("current_step_id") or ""),
        "answer_summary": _truncate_text(analysis_text or answer, 240),
        "pipeline_summary": pipeline_summary,
        "selection_plan": pipeline.get("selection_plan"),
        "transform_plan": pipeline.get("transform_plan"),
        "chart_spec": pipeline.get("chart_spec") or chart_spec,
        "analysis_anchor": analysis_anchor or {},
        "result_columns": pipeline.get("result_columns") or [],
        "result_row_count": int(pipeline.get("result_row_count") or 0),
        "status": str(pipeline.get("status") or "ok"),
        "execution_disclosure": execution_disclosure or {},
    }


settings = get_settings()
conversation_store = ConversationStore(
    max_sessions=settings.conversation_max_sessions,
    max_turns=settings.conversation_max_turns,
    cache_ttl_seconds=settings.conversation_cache_ttl_seconds,
    cache_max_entries=settings.conversation_cache_max_entries,
)
