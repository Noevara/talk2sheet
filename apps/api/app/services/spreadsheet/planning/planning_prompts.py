from __future__ import annotations

import json
import math
from typing import Any

from app.config import get_settings
from ..pipeline.column_profile import get_column_profiles
from ..execution.executor import detect_unique_key_candidates


PROMPT_RESULT_MAX_ROWS = 200
PROMPT_RESULT_MAX_COLS = 40
PROMPT_CELL_MAX_CHARS = 200
PROMPT_BUDGET_CHARS = 32000


def _truncate_cells(df: Any, *, cell_max_chars: int = PROMPT_CELL_MAX_CHARS) -> Any:
    out = df.copy()
    for column in getattr(out, "columns", []):
        try:
            if str(out[column].dtype) == "object":
                out[column] = out[column].astype(str).map(
                    lambda value: (value[:cell_max_chars] + "...") if len(value) > cell_max_chars else value
                )
        except Exception:
            continue
    return out


def _cap_columns(df: Any, *, max_cols: int = PROMPT_RESULT_MAX_COLS) -> Any:
    columns = list(getattr(df, "columns", []))
    if len(columns) <= max_cols:
        return df
    return df[columns[:max_cols]]


def _cap_rows(df: Any, *, max_rows: int = PROMPT_RESULT_MAX_ROWS) -> Any:
    if len(df) <= max_rows:
        return df
    return df.head(max_rows)


def dataframe_to_small_csv(df: Any, *, budget_chars: int = PROMPT_BUDGET_CHARS) -> str:
    tmp = _cap_rows(_cap_columns(_truncate_cells(df)))
    csv = tmp.to_csv(index=False)
    if len(csv) <= budget_chars:
        return csv

    rows = min(len(tmp), PROMPT_RESULT_MAX_ROWS)
    while rows > 1:
        rows = max(1, rows // 2)
        csv = tmp.head(rows).to_csv(index=False)
        if len(csv) <= budget_chars:
            return csv
    return csv[:budget_chars]


def describe_dataframe_for_prompt(df: Any) -> dict[str, Any]:
    column_profiles = get_column_profiles(df)
    columns = [str(column) for column in getattr(df, "columns", [])]
    dtypes: dict[str, str] = {}
    for column in columns:
        try:
            dtypes[column] = str(df[column].dtype)
        except Exception:
            dtypes[column] = "unknown"
    ordered_profiles = [column_profiles[column] for column in columns if column in column_profiles]
    return {
        "rows": int(getattr(df, "shape", (0, 0))[0]),
        "cols": int(getattr(df, "shape", (0, 0))[1]),
        "columns": columns,
        "dtypes": dtypes,
        "column_profiles": ordered_profiles,
        "preview_csv": dataframe_to_small_csv(df),
    }


def build_pivot_value_samples(df: Any, *, max_values: int = 8, max_rows: int = 500) -> dict[str, list[str]]:
    preview_df = df.head(max_rows) if hasattr(df, "head") else df
    out: dict[str, list[str]] = {}
    for column in [str(item) for item in getattr(preview_df, "columns", [])]:
        try:
            series = preview_df[column]
        except Exception:
            continue
        values: list[str] = []
        seen: set[str] = set()
        for value in list(series):
            if value is None:
                continue
            if isinstance(value, float) and math.isnan(value):
                continue
            text = str(value).strip()
            if not text or text.lower() in {"nan", "none", "<na>"} or text in seen:
                continue
            seen.add(text)
            values.append(text)
            if len(values) >= max_values:
                break
        if len(values) >= 2:
            out[column] = values
    return out


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def build_selection_prompt(
    df: Any,
    *,
    question: str,
    requested_mode: str,
    followup_context: dict[str, Any] | None = None,
) -> tuple[str, str]:
    desc = describe_dataframe_for_prompt(df)
    settings = get_settings()
    system_prompt = (
        "You are a data selection planner / 你是数据查询规划器。"
        "Return JSON only and it must exactly match the SelectionPlan schema / 你必须只输出JSON，符合SelectionPlan schema。"
        "Goal: choose the necessary columns and filters for the user's question, and narrow the data scope when reasonable / "
        "目标：为用户问题选择必要的列，并给出过滤条件，尽量缩小数据范围。"
        "Use only the provided columns. Never invent column names / 不要编造列名，只能使用提供的columns。"
        "Unless the user explicitly asks for top-N detail rows or deduplication, do not use limit or distinct_by / "
        "除非用户明确要求“明细前N条”或“去重”，否则不要使用limit/distinct_by。"
        "For ranking or chart statistics, do not truncate rows in SelectionPlan. Put TopN in TransformPlan.top_k instead / "
        "对于统计排行问题，不要在Selection阶段截断数据，TopN应放到TransformPlan.top_k。"
    )
    user_prompt = (
        f"requested_mode={requested_mode}\n"
        f"columns={desc['columns']}\n"
        f"dtypes={_json(desc['dtypes'])}\n"
        f"column_profiles={_json(desc['column_profiles'])}\n"
        f"followup_context={_json(followup_context or {})}\n"
        f"sample_note=Only the first {settings.max_analysis_rows} rows are loaded on the server / 仅使用前{settings.max_analysis_rows}行（服务端硬限制）\n"
        f"preview_csv=\n{desc['preview_csv']}\n\n"
        f"question={question}\n\n"
        "Output SelectionPlan JSON only. If no filtering is needed, set filters to []. / 输出SelectionPlan JSON。若不需要筛选，filters为空数组。"
    )
    return system_prompt, user_prompt


def build_transform_prompt(
    df: Any,
    *,
    question: str,
    mode: str,
    followup_context: dict[str, Any] | None = None,
) -> tuple[str, str]:
    desc = describe_dataframe_for_prompt(df)
    settings = get_settings()
    system_prompt = (
        "You are a data statistics planner / 你是数据统计规划器。"
        "Return JSON only and it must exactly match the TransformPlan schema / 你必须只输出JSON，符合TransformPlan schema。"
        "Goal: build a result table that can answer the user's question from the already selected data / "
        "目标：根据用户问题对已筛选数据做分组、聚合、统计，得到可用于回答的结果表。"
        "Use only the provided columns. Never invent column names / 不要编造列名，只能使用提供的columns。"
        "If analysis columns are needed, use derived_columns. date_bucket supports day, week, month, quarter, weekday, weekpart. "
        "arithmetic supports add, sub, mul, div / 如需先构造分析列，可使用derived_columns：date_bucket支持day/week/month/quarter/weekday/weekpart，"
        "arithmetic支持add/sub/mul/div。"
        "Use formula_metrics for post-aggregation add/sub/mul/div. Use having for post-aggregation filtering. "
        "Use pivot to reshape a long table into a wide table. Use post_pivot_formula_metrics and post_pivot_having for wide-table calculations and filtering / "
        "formula_metrics用于聚合后再做add/sub/mul/div；having用于聚合后筛选；pivot用于把长表转成宽表；"
        "如需在pivot后继续比较宽表列，可使用post_pivot_formula_metrics和post_pivot_having。"
        "If pivot.columns is used, the generated wide columns usually come from actual values of that column / "
        "如果使用pivot.columns，则pivot后生成的宽表列名通常来自该列的实际取值。"
        "For raw detail or record-list questions, set return_rows=true and keep groupby=[] and metrics=[]. Use order_by and top_k to control order and size / "
        "如果用户要求返回原始明细、逐条记录、记录列表、前N条记录内容，应设置return_rows=true，且groupby=[]、metrics=[]，再用order_by/top_k控制输出顺序和数量。"
        "If mode=chart, keep return_rows=false / 图表模式不要使用return_rows，保持return_rows=false。"
        "For monthly, weekly, or quarterly trends, do date_bucket first and then group by the derived bucket / "
        "如果问题是按月、按周、按季度趋势，不要直接groupby原始日期，应先做date_bucket。"
        "When the question asks how many people, users, or unique items and unique-key candidates exist, prefer count_distinct with an explicit col. "
        "Otherwise use count_rows / 当用户问“多少人、人数、有多少个”且存在唯一键候选时，你可以选择count_distinct并指定col；否则可用count_rows。"
    )
    user_prompt = (
        f"mode={mode}\n"
        f"columns={desc['columns']}\n"
        f"dtypes={_json(desc['dtypes'])}\n"
        f"column_profiles={_json(desc['column_profiles'])}\n"
        f"unique_key_candidates={_json(detect_unique_key_candidates(df))}\n"
        f"pivot_value_samples={_json(build_pivot_value_samples(df))}\n"
        f"followup_context={_json(followup_context or {})}\n"
        "transform_capabilities=derived_columns(date_bucket/arithmetic), formula_metrics(add/sub/mul/div), having(post_agg_filter), "
        "pivot(index/columns/values), post_pivot_formula_metrics(add/sub/mul/div), post_pivot_having(post_pivot_filter), "
        "return_rows(detail_rows_or_raw_records)\n"
        "pivot_constraints=after pivot, post_pivot_formula_metrics/post_pivot_having/order_by may only reference pivot.index columns, "
        "generated columns from actual pivot.columns values, or aliases newly created by post_pivot_formula_metrics\n"
        f"sample_note=Only the first {settings.max_analysis_rows} rows are loaded on the server / 仅使用前{settings.max_analysis_rows}行（服务端硬限制）\n"
        f"preview_csv=\n{desc['preview_csv']}\n\n"
        f"question={question}\n\n"
        "Output TransformPlan JSON only. If deduplication grain is uncertain, prefer count_rows. / "
        "输出TransformPlan JSON。若不确定去重口径，可用count_rows并在回答中提示可能重复。"
    )
    return system_prompt, user_prompt


def build_chart_prompt(
    df: Any,
    *,
    question: str,
    followup_context: dict[str, Any] | None = None,
) -> tuple[str, str]:
    desc = describe_dataframe_for_prompt(df)
    system_prompt = (
        "You are a data visualization planner / 你是数据可视化规划器。"
        "Return JSON only and it must exactly match the ChartSpec schema / 你必须只输出JSON，符合ChartSpec schema。"
        "result_csv is already the final chart table. Do not assume another aggregation step is needed / result_csv已经是最终绘图表，不要假设还需要再次聚合。"
        "The chart must use only columns from result_csv. Never invent columns. Output one chart only / 图必须基于result_csv中的列，禁止编造列名。仅输出一张图。"
        "Supported chart types: line, bar, scatter, pie. Prefer line for time trends, bar for category comparisons, and avoid pie when there are too many categories / "
        "支持类型：line、bar、scatter、pie。时间趋势优先line，类别对比优先bar，类别过多不要用pie。"
    )
    user_prompt = (
        f"result_columns={desc['columns']}\n"
        f"result_dtypes={_json(desc['dtypes'])}\n"
        f"result_column_profiles={_json(desc['column_profiles'])}\n"
        f"followup_context={_json(followup_context or {})}\n"
        f"question={question}\n\n"
        f"result_csv=\n{desc['preview_csv']}\n\n"
        "Output ChartSpec JSON only. / 输出ChartSpec JSON。"
    )
    return system_prompt, user_prompt


def build_followup_interpreter_prompt(
    df: Any,
    *,
    question: str,
    requested_mode: str,
    followup_context: dict[str, Any] | None = None,
) -> tuple[str, str]:
    desc = describe_dataframe_for_prompt(df)
    system_prompt = (
        "You are a spreadsheet conversation context interpreter / 你是电子表格多轮对话上下文解释器。"
        "Return JSON only and it must exactly match the schema / 你必须只输出JSON，严格符合schema。"
        "Your job is to decide whether the latest user turn is a new analysis question or a follow-up to the previous result / "
        "你的任务是判断当前用户输入是一个全新的分析问题，还是对上一个结果的延续、修改、筛选、解释。"
        "If the user refers to previous items with pronouns or rank positions, resolve them into a standalone question when possible / "
        "如果用户用代词、排名、上一个结果中的对象来指代，请尽量改写成可独立理解的完整问题。"
        "Do not invent columns, filters, metrics, or business meaning that are not supported by the current question and conversation context / "
        "不要编造列名、筛选条件、指标或业务含义。"
        "Prefer preserving the previous analysis only when the user is clearly refining or reusing it / "
        "只有当用户明显是在延续上轮分析时，才保留上一轮分析口径。"
    )
    user_prompt = (
        f"requested_mode={requested_mode}\n"
        f"columns={desc['columns']}\n"
        f"dtypes={_json(desc['dtypes'])}\n"
        f"column_profiles={_json(desc['column_profiles'])}\n"
        f"followup_context={_json(followup_context or {})}\n"
        f"preview_csv=\n{desc['preview_csv']}\n\n"
        f"question={question}\n\n"
        "Output JSON only."
    )
    return system_prompt, user_prompt
