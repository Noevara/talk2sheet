from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

from app.schemas import JoinPreflightCheck, JoinPreflightResult, JoinPreflightSheetMetrics
from ..contracts.workbook_models import WorkbookContext, WorkbookSheetProfile
from ..planning.join_beta_signals import evaluate_join_beta_request
from ..routing.router_types import SheetRoutingDecision
from .dataframe_loader import load_dataframe


def _normalize_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s_\-\.]+", "", text)
    return text


def _pick_locale(locale: str) -> str:
    normalized = str(locale or "").lower()
    if normalized.startswith("zh"):
        return "zh-CN"
    if normalized.startswith("ja"):
        return "ja-JP"
    return "en"


def _text(locale: str, *, en: str, zh: str, ja: str) -> str:
    normalized = _pick_locale(locale)
    if normalized == "zh-CN":
        return zh
    if normalized == "ja-JP":
        return ja
    return en


def _sheet_by_index(workbook_context: WorkbookContext, sheet_index: int) -> WorkbookSheetProfile | None:
    return next((sheet for sheet in workbook_context.sheets if int(sheet.sheet_index) == int(sheet_index)), None)


def _resolve_sheet_indexes(workbook_context: WorkbookContext, routing_decision: SheetRoutingDecision) -> list[int]:
    picked: list[int] = []
    for item in list(routing_decision.mentioned_sheets or []):
        sheet_index = int(item.get("sheet_index") or 0) if isinstance(item, dict) else 0
        if sheet_index > 0 and sheet_index not in picked:
            picked.append(sheet_index)
        if len(picked) >= 2:
            return picked[:2]

    for sheet in workbook_context.sheets:
        sheet_index = int(sheet.sheet_index or 0)
        if sheet_index > 0 and sheet_index not in picked:
            picked.append(sheet_index)
        if len(picked) >= 2:
            return picked[:2]
    return picked[:2]


def _resolve_join_key_column(columns: list[str], join_key: str) -> str:
    key = str(join_key or "").strip()
    if not key:
        return ""
    normalized_key = _normalize_token(key)
    if not normalized_key:
        return ""

    for column in columns:
        if _normalize_token(column) == normalized_key:
            return str(column)

    if "." in key:
        suffix = key.rsplit(".", 1)[-1].strip()
        normalized_suffix = _normalize_token(suffix)
        for column in columns:
            if _normalize_token(column) == normalized_suffix:
                return str(column)

    for column in columns:
        normalized_column = _normalize_token(column)
        if normalized_key and (normalized_key in normalized_column or normalized_column in normalized_key):
            return str(column)

    return ""


def _safe_key_series(df: pd.DataFrame, column: str) -> pd.Series:
    series = df[column] if column in df.columns else pd.Series(dtype="object")
    normalized = series.astype("string").str.strip().str.lower()
    normalized = normalized.replace({"": pd.NA, "nan": pd.NA, "none": pd.NA})
    return normalized.dropna()


def _detect_key_dtype(series: pd.Series) -> str:
    if is_numeric_dtype(series):
        return "numeric"
    if is_datetime64_any_dtype(series):
        return "datetime"
    non_null = series.dropna()
    if non_null.empty:
        return "unknown"
    sample = non_null.astype("string")
    numeric_ratio = float(pd.to_numeric(sample, errors="coerce").notna().mean())
    if numeric_ratio >= 0.9:
        return "numeric"
    date_like_ratio = float(
        sample.str.contains(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{8}", regex=True, na=False).mean()
    )
    if date_like_ratio < 0.5:
        return "text"
    datetime_ratio = float(pd.to_datetime(sample, errors="coerce").notna().mean())
    if datetime_ratio >= 0.9:
        return "datetime"
    return "text"


def _status_rank(status: str) -> int:
    if status == "fail":
        return 2
    if status == "warn":
        return 1
    return 0


def _overall_status(checks: list[JoinPreflightCheck]) -> str:
    highest = 0
    for check in checks:
        highest = max(highest, _status_rank(check.status))
    if highest >= 2:
        return "fail"
    if highest >= 1:
        return "warn"
    return "pass"


def _compose_summary(*, locale: str, status: str, join_key: str, suggestions: list[str]) -> str:
    if status == "pass":
        return _text(
            locale,
            en=f"Join preflight passed for key '{join_key}'.",
            zh=f"Join 预检通过，可用键「{join_key}」进入后续流程。",
            ja=f"Join プリフライトはキー「{join_key}」で通過しました。",
        )
    suggestion_text = " ".join(suggestions[:2]).strip()
    if status == "warn":
        return _text(
            locale,
            en=f"Join preflight has risks for key '{join_key}'. {suggestion_text}",
            zh=f"Join 预检发现风险（键「{join_key}」）。{suggestion_text}",
            ja=f"Join プリフライトでリスクが検出されました（キー「{join_key}」）。{suggestion_text}",
        ).strip()
    return _text(
        locale,
        en=f"Join preflight failed for key '{join_key}'. {suggestion_text}",
        zh=f"Join 预检失败（键「{join_key}」）。{suggestion_text}",
        ja=f"Join プリフライト失敗（キー「{join_key}」）。{suggestion_text}",
    ).strip()


def run_join_preflight(
    *,
    path: Path,
    workbook_context: WorkbookContext,
    routing_decision: SheetRoutingDecision,
    question: str,
    locale: str,
    sample_limit: int = 2000,
) -> JoinPreflightResult | None:
    join_eval = evaluate_join_beta_request(question)
    if not bool(join_eval.get("is_join_request")):
        return None

    checks: list[JoinPreflightCheck] = []
    suggestions: list[str] = []
    sheet_indexes = _resolve_sheet_indexes(workbook_context, routing_decision)
    join_key = str(join_eval.get("join_key") or "")
    join_type = str(join_eval.get("join_type") or "")
    join_reasons = [str(item) for item in list(join_eval.get("reasons") or []) if str(item).strip()]

    if len(sheet_indexes) != 2:
        checks.append(
            JoinPreflightCheck(
                code="join_sheet_pair_invalid",
                status="fail",
                message=_text(
                    locale,
                    en="Join preflight requires exactly two sheets.",
                    zh="Join 预检需要明确两张工作表。",
                    ja="Join プリフライトには 2 つのシート指定が必要です。",
                ),
                suggestion=_text(
                    locale,
                    en="Specify exactly two sheets to continue.",
                    zh="请明确指定两张工作表后重试。",
                    ja="対象シートを 2 つに絞って再実行してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)
        return JoinPreflightResult(
            status="fail",
            is_join_request=True,
            join_key=join_key,
            join_type=join_type,
            sheet_indexes=sheet_indexes,
            checks=checks,
            repair_suggestions=suggestions,
            summary=_compose_summary(locale=locale, status="fail", join_key=join_key or "N/A", suggestions=suggestions),
        )

    left_sheet = _sheet_by_index(workbook_context, sheet_indexes[0])
    right_sheet = _sheet_by_index(workbook_context, sheet_indexes[1])
    if left_sheet is None or right_sheet is None:
        checks.append(
            JoinPreflightCheck(
                code="join_sheet_not_found",
                status="fail",
                message=_text(
                    locale,
                    en="Join preflight could not resolve selected sheets.",
                    zh="Join 预检无法定位所选工作表。",
                    ja="Join プリフライトで選択シートを解決できませんでした。",
                ),
                suggestion=_text(
                    locale,
                    en="Re-select the two sheets and retry.",
                    zh="请重新选择两张表后重试。",
                    ja="2 つのシートを再選択して再試行してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)
        return JoinPreflightResult(
            status="fail",
            is_join_request=True,
            join_key=join_key,
            join_type=join_type,
            sheet_indexes=sheet_indexes,
            checks=checks,
            repair_suggestions=suggestions,
            summary=_compose_summary(locale=locale, status="fail", join_key=join_key or "N/A", suggestions=suggestions),
        )

    if "join_more_than_two_tables" in join_reasons:
        checks.append(
            JoinPreflightCheck(
                code="join_more_than_two_tables",
                status="fail",
                message=_text(
                    locale,
                    en="Detected a request to join more than two sheets.",
                    zh="检测到超过两张表的 join 请求。",
                    ja="2 シートを超える join リクエストを検出しました。",
                ),
                suggestion=_text(
                    locale,
                    en="Limit the request to two sheets only.",
                    zh="请先限制为两张表。",
                    ja="まず 2 シート join に限定してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)
    if "join_type_not_allowed" in join_reasons:
        checks.append(
            JoinPreflightCheck(
                code="join_type_not_allowed",
                status="fail",
                message=_text(
                    locale,
                    en="Join type is not allowed in beta scope.",
                    zh="Join 类型不在 Beta 支持范围内。",
                    ja="Join 種別が Beta の対応範囲外です。",
                ),
                suggestion=_text(
                    locale,
                    en="Use inner join or left join only.",
                    zh="请使用 inner 或 left join。",
                    ja="inner join または left join を使用してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)
    if "join_multi_key_not_allowed" in join_reasons:
        checks.append(
            JoinPreflightCheck(
                code="join_multi_key_not_allowed",
                status="fail",
                message=_text(
                    locale,
                    en="Multiple join keys are not supported in beta.",
                    zh="Beta 阶段暂不支持多键 join。",
                    ja="Beta では複数キー join に未対応です。",
                ),
                suggestion=_text(
                    locale,
                    en="Reduce to a single join key.",
                    zh="请先收敛为单键 join。",
                    ja="単一キー join に絞ってください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)
    if "join_non_aggregate_query" in join_reasons:
        checks.append(
            JoinPreflightCheck(
                code="join_non_aggregate_query",
                status="warn",
                message=_text(
                    locale,
                    en="Question is not clearly aggregate-oriented.",
                    zh="问题不是明确的聚合分析口径。",
                    ja="質問が集計指向であることを確認できませんでした。",
                ),
                suggestion=_text(
                    locale,
                    en="Use aggregate wording such as sum/count/avg/top/trend.",
                    zh="建议使用 sum/count/avg/top/trend 等聚合口径描述。",
                    ja="sum/count/avg/top/trend など集計表現で質問してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)

    left_key_col = _resolve_join_key_column(list(left_sheet.columns or []), join_key)
    right_key_col = _resolve_join_key_column(list(right_sheet.columns or []), join_key)

    if not join_key:
        checks.append(
            JoinPreflightCheck(
                code="join_key_missing",
                status="fail",
                message=_text(
                    locale,
                    en="Join key is missing from the question.",
                    zh="问题中未识别到 join key。",
                    ja="質問内で join キーを特定できませんでした。",
                ),
                suggestion=_text(
                    locale,
                    en="Specify a concrete key, for example: by Email.",
                    zh="请明确指定键，例如：按 Email 关联。",
                    ja="例: by Email のように具体的なキーを指定してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)
    if join_key and not left_key_col:
        checks.append(
            JoinPreflightCheck(
                code="left_sheet_key_missing",
                status="fail",
                message=_text(
                    locale,
                    en=f"Join key '{join_key}' was not found in sheet '{left_sheet.sheet_name}'.",
                    zh=f"在工作表「{left_sheet.sheet_name}」中未找到 join 键「{join_key}」。",
                    ja=f"シート「{left_sheet.sheet_name}」に join キー「{join_key}」が見つかりません。",
                ),
                suggestion=_text(
                    locale,
                    en="Check column naming or choose an existing key column in the first sheet.",
                    zh="请检查字段命名，或改用第一张表中存在的键列。",
                    ja="列名を確認し、1枚目シートに存在するキー列を選択してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)
    if join_key and not right_key_col:
        checks.append(
            JoinPreflightCheck(
                code="right_sheet_key_missing",
                status="fail",
                message=_text(
                    locale,
                    en=f"Join key '{join_key}' was not found in sheet '{right_sheet.sheet_name}'.",
                    zh=f"在工作表「{right_sheet.sheet_name}」中未找到 join 键「{join_key}」。",
                    ja=f"シート「{right_sheet.sheet_name}」に join キー「{join_key}」が見つかりません。",
                ),
                suggestion=_text(
                    locale,
                    en="Check column naming or choose an existing key column in the second sheet.",
                    zh="请检查字段命名，或改用第二张表中存在的键列。",
                    ja="列名を確認し、2枚目シートに存在するキー列を選択してください。",
                ),
            )
        )
        suggestions.append(checks[-1].suggestion)

    sampled_limit = max(100, min(int(sample_limit or 0), 5000))
    left_df, _ = load_dataframe(path, sheet_index=int(left_sheet.sheet_index), limit=sampled_limit)
    right_df, _ = load_dataframe(path, sheet_index=int(right_sheet.sheet_index), limit=sampled_limit)
    runtime_left_key_col = left_key_col
    runtime_right_key_col = right_key_col
    if runtime_left_key_col and runtime_left_key_col not in left_df.columns:
        runtime_left_key_col = _resolve_join_key_column(list(left_df.columns), runtime_left_key_col or join_key)
    if runtime_right_key_col and runtime_right_key_col not in right_df.columns:
        runtime_right_key_col = _resolve_join_key_column(list(right_df.columns), runtime_right_key_col or join_key)

    left_metrics = JoinPreflightSheetMetrics(
        sheet_index=int(left_sheet.sheet_index),
        sheet_name=str(left_sheet.sheet_name),
        sampled_rows=int(len(left_df)),
        key_column=runtime_left_key_col or left_key_col,
    )
    right_metrics = JoinPreflightSheetMetrics(
        sheet_index=int(right_sheet.sheet_index),
        sheet_name=str(right_sheet.sheet_name),
        sampled_rows=int(len(right_df)),
        key_column=runtime_right_key_col or right_key_col,
    )

    estimated_match_rate: float | None = None
    estimated_left_unmatched_rate: float | None = None
    estimated_right_unmatched_rate: float | None = None

    if runtime_left_key_col and runtime_right_key_col and runtime_left_key_col in left_df.columns and runtime_right_key_col in right_df.columns:
        left_key_series = left_df[runtime_left_key_col]
        right_key_series = right_df[runtime_right_key_col]
        left_type = _detect_key_dtype(left_key_series)
        right_type = _detect_key_dtype(right_key_series)
        left_metrics.key_dtype = left_type
        right_metrics.key_dtype = right_type

        if left_type != "unknown" and right_type != "unknown" and left_type != right_type:
            checks.append(
                JoinPreflightCheck(
                    code="join_key_dtype_mismatch",
                    status="fail",
                    message=_text(
                        locale,
                        en=f"Join key types mismatch: {left_type} vs {right_type}.",
                        zh=f"Join 键类型不一致：{left_type} vs {right_type}。",
                        ja=f"Join キー型が不一致です: {left_type} vs {right_type}。",
                    ),
                    suggestion=_text(
                        locale,
                        en="Align both key columns to the same type before joining.",
                        zh="请先把两侧键列转换为相同类型。",
                        ja="join 前に両側キー列の型を揃えてください。",
                    ),
                )
            )
            suggestions.append(checks[-1].suggestion)

        left_keys = _safe_key_series(left_df, runtime_left_key_col)
        right_keys = _safe_key_series(right_df, runtime_right_key_col)
        left_null_rate = 1.0 - (float(len(left_keys)) / float(max(len(left_df), 1)))
        right_null_rate = 1.0 - (float(len(right_keys)) / float(max(len(right_df), 1)))
        left_dup_rate = float(left_keys.duplicated().mean()) if len(left_keys) > 0 else 0.0
        right_dup_rate = float(right_keys.duplicated().mean()) if len(right_keys) > 0 else 0.0
        left_metrics.key_null_rate = round(left_null_rate, 4)
        right_metrics.key_null_rate = round(right_null_rate, 4)
        left_metrics.key_duplicate_rate = round(left_dup_rate, 4)
        right_metrics.key_duplicate_rate = round(right_dup_rate, 4)

        if max(left_null_rate, right_null_rate) >= 0.5:
            checks.append(
                JoinPreflightCheck(
                    code="join_key_null_rate_high",
                    status="fail",
                    message=_text(
                        locale,
                        en="Join key has too many null values in one of the sheets.",
                        zh="至少一张表的 join 键空值率过高。",
                        ja="少なくとも一方のシートで join キー欠損率が高すぎます。",
                    ),
                    suggestion=_text(
                        locale,
                        en="Clean null keys or filter to valid key rows first.",
                        zh="建议先清洗空键，或只保留有效键后再关联。",
                        ja="欠損キーを補正するか、有効キー行のみで結合してください。",
                    ),
                )
            )
            suggestions.append(checks[-1].suggestion)
        elif max(left_null_rate, right_null_rate) >= 0.2:
            checks.append(
                JoinPreflightCheck(
                    code="join_key_null_rate_risky",
                    status="warn",
                    message=_text(
                        locale,
                        en="Join key null rate is elevated and may reduce match quality.",
                        zh="Join 键空值率偏高，可能影响匹配质量。",
                        ja="join キー欠損率が高く、マッチ品質低下の可能性があります。",
                    ),
                    suggestion=_text(
                        locale,
                        en="Consider cleaning missing keys before join.",
                        zh="建议先补齐或过滤缺失键。",
                        ja="欠損キーを補正または除外してから join してください。",
                    ),
                )
            )
            suggestions.append(checks[-1].suggestion)

        if max(left_dup_rate, right_dup_rate) >= 0.6:
            checks.append(
                JoinPreflightCheck(
                    code="join_key_duplicate_rate_high",
                    status="warn",
                    message=_text(
                        locale,
                        en="Join key duplicate rate is high and may inflate rows after join.",
                        zh="Join 键重复率较高，关联后可能导致结果膨胀。",
                        ja="join キー重複率が高く、結合後に行数が増幅する可能性があります。",
                    ),
                    suggestion=_text(
                        locale,
                        en="Deduplicate one side or aggregate before join.",
                        zh="建议先去重，或在 join 前先做聚合。",
                        ja="片側を重複排除、または join 前に集約してください。",
                    ),
                )
            )
            suggestions.append(checks[-1].suggestion)

        left_unique = set(left_keys.tolist())
        right_unique = set(right_keys.tolist())
        intersection = left_unique & right_unique
        left_match_rate = float(len(intersection)) / float(max(len(left_unique), 1))
        right_match_rate = float(len(intersection)) / float(max(len(right_unique), 1))
        estimated_match_rate = round((left_match_rate + right_match_rate) / 2.0, 4)
        estimated_left_unmatched_rate = round(1.0 - left_match_rate, 4)
        estimated_right_unmatched_rate = round(1.0 - right_match_rate, 4)

        if estimated_match_rate < 0.1:
            checks.append(
                JoinPreflightCheck(
                    code="join_match_rate_too_low",
                    status="fail",
                    message=_text(
                        locale,
                        en="Estimated key match rate is too low.",
                        zh="估算键匹配率过低。",
                        ja="推定キー一致率が低すぎます。",
                    ),
                    suggestion=_text(
                        locale,
                        en="Verify key format consistency and remove noisy values.",
                        zh="请统一键格式并清理噪声值后重试。",
                        ja="キー形式を統一し、ノイズ値を除去して再試行してください。",
                    ),
                )
            )
            suggestions.append(checks[-1].suggestion)
        elif estimated_match_rate < 0.4:
            checks.append(
                JoinPreflightCheck(
                    code="join_match_rate_risky",
                    status="warn",
                    message=_text(
                        locale,
                        en="Estimated key match rate is moderate and may affect reliability.",
                        zh="估算键匹配率一般，可能影响结果可靠性。",
                        ja="推定キー一致率が中程度で、結果信頼性に影響する可能性があります。",
                    ),
                    suggestion=_text(
                        locale,
                        en="Check key standardization (trim/case/format) before join.",
                        zh="建议先做键标准化（去空格/大小写/格式统一）。",
                        ja="キー標準化（空白/大文字小文字/形式）を行ってください。",
                    ),
                )
            )
            suggestions.append(checks[-1].suggestion)
    else:
        estimated_match_rate = None
        estimated_left_unmatched_rate = None
        estimated_right_unmatched_rate = None

    if not checks:
        checks.append(
            JoinPreflightCheck(
                code="join_preflight_ok",
                status="pass",
                message=_text(
                    locale,
                    en="Join preflight checks passed.",
                    zh="Join 预检通过。",
                    ja="Join プリフライトを通過しました。",
                ),
            )
        )

    status = _overall_status(checks)
    dedup_suggestions = [item for index, item in enumerate(suggestions) if item and item not in suggestions[:index]]
    summary = _compose_summary(locale=locale, status=status, join_key=join_key or "N/A", suggestions=dedup_suggestions)
    return JoinPreflightResult(
        status=status,
        is_join_request=True,
        join_key=join_key,
        join_type=join_type,
        sheet_indexes=[int(item) for item in sheet_indexes],
        estimated_match_rate=estimated_match_rate,
        estimated_left_unmatched_rate=estimated_left_unmatched_rate,
        estimated_right_unmatched_rate=estimated_right_unmatched_rate,
        left_sheet=left_metrics,
        right_sheet=right_metrics,
        checks=checks,
        repair_suggestions=dedup_suggestions,
        summary=summary,
    )
