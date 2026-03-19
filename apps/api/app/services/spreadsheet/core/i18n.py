from __future__ import annotations

MESSAGES = {
    "en": {
        "scope_exact": "Full-sheet exact computation",
        "scope_sampled": "Using the first {max_rows} rows only.",
        "unsupported": "This question is not supported yet. Try row count, total amount, average, Top N, trend, detail rows, or a basic chart.",
        "row_count": "Scope: full active sheet. Total rows: {value}.",
        "distinct_count": "Scope: full active sheet. Distinct {column}: {value}.",
        "total_amount": "Scope: full active sheet. Total {column}: {value:.4f}.",
        "average_amount": "Scope: full active sheet. Average {column}: {value:.4f}.",
        "detail_rows": "Scope: full active sheet. Top {limit} rows by {column}.",
        "ranking": "Scope: full active sheet. Top {limit} {dimension} by {metric}.",
        "trend": "Scope: full active sheet. Monthly trend for {metric}.",
        "share": "Scope: full active sheet. Share by {dimension}.",
        "forecast_unavailable": "Forecasting is unavailable: {reason}",
        "chart_ready": "Chart generated from the active sheet.",
        "clarification": "The plan is ambiguous and may need clarification: {reason}",
        "internal_error": "The analysis failed unexpectedly. Request ID: {request_id}",
    },
    "zh-CN": {
        "scope_exact": "全表精确计算",
        "scope_sampled": "仅使用前 {max_rows} 行数据。",
        "unsupported": "当前问题暂不支持。请尝试总行数、总金额、平均值、TopN、趋势、明细或基础图表。",
        "row_count": "口径：当前工作表整表。总行数：{value}。",
        "distinct_count": "口径：当前工作表整表。{column}去重后数量：{value}。",
        "total_amount": "口径：当前工作表整表。{column}总计：{value:.4f}。",
        "average_amount": "口径：当前工作表整表。{column}平均值：{value:.4f}。",
        "detail_rows": "口径：当前工作表整表。按{column}降序取前{limit}条明细。",
        "ranking": "口径：当前工作表整表。按{dimension}统计{metric}，返回前{limit}名。",
        "trend": "口径：当前工作表整表。按月统计{metric}趋势。",
        "share": "口径：当前工作表整表。按{dimension}统计占比。",
        "forecast_unavailable": "当前无法执行预测：{reason}",
        "chart_ready": "已基于当前工作表生成图表。",
        "clarification": "当前规划存在歧义，可能需要补充确认：{reason}",
        "internal_error": "分析过程发生异常。请求 ID：{request_id}",
    },
    "ja-JP": {
        "scope_exact": "シート全体の厳密計算",
        "scope_sampled": "先頭 {max_rows} 行のみを使用しています。",
        "unsupported": "この質問はまだ未対応です。件数、合計、平均、Top N、トレンド、詳細行、基本チャートを試してください。",
        "row_count": "対象: アクティブシート全体。総行数: {value}。",
        "distinct_count": "対象: アクティブシート全体。{column} の重複除外件数: {value}。",
        "total_amount": "対象: アクティブシート全体。{column} の合計: {value:.4f}。",
        "average_amount": "対象: アクティブシート全体。{column} の平均: {value:.4f}。",
        "detail_rows": "対象: アクティブシート全体。{column} の降順で上位 {limit} 行。",
        "ranking": "対象: アクティブシート全体。{dimension} 別の {metric} 上位 {limit} 件。",
        "trend": "対象: アクティブシート全体。{metric} の月次トレンド。",
        "share": "対象: アクティブシート全体。{dimension} 別の構成比。",
        "forecast_unavailable": "予測を実行できません: {reason}",
        "chart_ready": "アクティブシートからチャートを生成しました。",
        "clarification": "計画にあいまいさがあります。確認が必要な可能性があります: {reason}",
        "internal_error": "分析中に予期しないエラーが発生しました。リクエスト ID: {request_id}",
    },
}


def pick_locale(locale: str) -> str:
    return locale if locale in MESSAGES else "en"


def t(locale: str, key: str, **kwargs: object) -> str:
    selected = pick_locale(locale)
    template = MESSAGES[selected][key]
    return template.format(**kwargs)
