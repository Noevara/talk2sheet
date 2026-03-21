import type { Locale } from "../types";

export interface SuggestedPromptGroup {
  label: string;
  prompts: string[];
}

export interface UiMessages {
  eyebrow: string;
  brand: string;
  tagline: string;
  capabilityTitle: string;
  capabilityBody: string;
  outOfScopeTitle: string;
  outOfScopeBody: string;
  languageLabel: string;
  uploadTitle: string;
  uploadHint: string;
  uploadButton: string;
  uploading: string;
  workbookTitle: string;
  workbookOverviewTitle: string;
  workbookOverviewHint: string;
  workbookEmpty: string;
  fileIdLabel: string;
  fileTypeLabel: string;
  previewTitle: string;
  previewLoading: string;
  previewEmpty: string;
  chatTitle: string;
  chatHint: string;
  chatEmpty: string;
  examplesButtonLabel: string;
  guideButtonLabel: string;
  quickStartTitle: string;
  quickStartBody: string;
  quickStartPrimary: string;
  quickStartSecondary: string;
  questionPlaceholder: string;
  modeLabel: string;
  modeAutoLabel: string;
  modeTextLabel: string;
  modeChartLabel: string;
  clarificationSelectedLabel: string;
  followupLabel: string;
  followupSuggestionSwitchToChart: string;
  followupSuggestionSwitchToText: string;
  followupSuggestionRefineTop3: string;
  followupSuggestionAskTrend: string;
  followupSuggestionRecentThreePeriods: string;
  followupSuggestionComparePreviousPeriod: string;
  followupSuggestionAggregateByCategory: string;
  followupSuggestionForecastNextMonth: string;
  followupSuggestionSummarizeOneLine: string;
  send: string;
  stop: string;
  thinking: string;
  aborted: string;
  suggestionsLabel: string;
  totalRowsLabel: string;
  previewRowsLabel: string;
  colsLoaded: string;
  sheetFieldSummaryLabel: string;
  sheetFieldSummaryEmpty: string;
  sheetOverridePendingLabel: string;
  scopeLabel: string;
  metadataLabel: string;
  sheetRoutingLabel: string;
  requestedSheetLabel: string;
  resolvedSheetLabel: string;
  routingMethodLabel: string;
  routingWhyLabel: string;
  routingBoundaryLabel: string;
  routingMentionedSheetsLabel: string;
  routingChangedLabel: string;
  routingMethodSingleSheetLabel: string;
  routingMethodExplicitLabel: string;
  routingMethodClarificationLabel: string;
  routingMethodManualOverrideLabel: string;
  routingMethodFollowupLabel: string;
  routingMethodAutoLabel: string;
  routingMethodRequestedLabel: string;
  routingBoundarySingleSheetLabel: string;
  routingBoundaryDetectedLabel: string;
  routingBoundaryOutOfScopeLabel: string;
  routingBoundaryOutOfScopeHint: string;
  filterSummaryLabel: string;
  topKSummaryLabel: string;
  trendGrainLabel: string;
  conclusionLabel: string;
  evidenceLabel: string;
  riskNoteLabel: string;
  clarificationLabel: string;
  clarificationColumnLabel: string;
  clarificationSheetLabel: string;
  clarificationApplyLabel: string;
  clarificationReasonPrefix: string;
  clarificationSelectedMessageTemplate: string;
  pipelineLabel: string;
  selectionPlanLabel: string;
  transformPlanLabel: string;
  detailRowsLabel: string;
  evidenceTableLabel: string;
  resultTableLabel: string;
  forecastLabel: string;
  forecastBadgeLabel: string;
  forecastTargetLabel: string;
  forecastEstimateLabel: string;
  forecastRangeLabel: string;
  forecastModelLabel: string;
  forecastHistoryLabel: string;
  forecastHistoryPointsLabel: string;
  forecastGrainLabel: string;
  forecastHorizonLabel: string;
  forecastTableLabel: string;
  forecastModelLinearLabel: string;
  forecastModelSmoothingLabel: string;
  forecastGrainDayLabel: string;
  forecastGrainWeekLabel: string;
  forecastGrainMonthLabel: string;
  compareLabel: string;
  compareBasisLabel: string;
  compareCurrentLabel: string;
  compareBaseLabel: string;
  compareChangeValueLabel: string;
  compareChangePctLabel: string;
  compareRatioLabel: string;
  compareBasisMomLabel: string;
  compareBasisYoyLabel: string;
  chartLabel: string;
  chartPointCountLabel: string;
  noChartData: string;
  copyAnswerLabel: string;
  copyAnswerDoneLabel: string;
  exportCsvLabel: string;
  exportChartLabel: string;
  exportChartDoneLabel: string;
  userLabel: string;
  assistantLabel: string;
  streamingLabel: string;
  sourceSheetLabel: string;
  sheetSwitchFromLabel: string;
  sheetSwitchReasonLabel: string;
  sheetSwitchReasonFollowupAnotherLabel: string;
  sheetSwitchReasonFollowupExplicitLabel: string;
  sheetSwitchReasonFollowupPreviousLabel: string;
  uploadError: string;
  uploadInvalidFileError: string;
  uploadTooLargeError: string;
  uploadServerError: string;
  previewError: string;
  previewMissingError: string;
  previewServerError: string;
  chatError: string;
  chatConnectionError: string;
  chatInterruptedError: string;
  clarificationExpiredError: string;
  restoreSessionExpiredError: string;
  networkError: string;
  missingFile: string;
  missingQuestion: string;
  suggestionGroups: SuggestedPromptGroup[];
}

export const messages: Record<Locale, UiMessages> = {
  en: {
    eyebrow: "Open-source spreadsheet conversation",
    brand: "Talk2Sheet",
    tagline: "Natural-language analytics and lightweight forecasting for Excel and CSV in a standalone open-source full-stack workspace.",
    capabilityTitle: "Current capability",
    capabilityBody: "Workbook-aware single-sheet routing, single-sheet analytics, whole-sheet execution disclosure, detail rows, Top N, trend charts, structured pipeline feedback, and lightweight time-series forecasting.",
    outOfScopeTitle: "Not in MVP",
    outOfScopeBody: "Workbook-aware sheet selection is supported, but cross-sheet joins or combined multi-sheet analysis are still out of scope, along with advanced statistics, causal inference, complex forecasting workflows, and private infrastructure integrations.",
    languageLabel: "Language",
    uploadTitle: "Upload workbook",
    uploadHint: "Drop an .xlsx, .xls, or .csv file to start a single-sheet conversation inside a workbook.",
    uploadButton: "Choose spreadsheet",
    uploading: "Uploading...",
    workbookTitle: "Workbook & sheets",
    workbookOverviewTitle: "Workbook overview",
    workbookOverviewHint: "Review sheet size and key fields before asking questions.",
    workbookEmpty: "No file uploaded yet.",
    fileIdLabel: "File ID",
    fileTypeLabel: "Type",
    previewTitle: "Sheet preview",
    previewLoading: "Loading preview...",
    previewEmpty: "Select a file and sheet to inspect the table preview.",
    chatTitle: "Conversation",
    chatHint: "The backend streams planner metadata and the final answer over SSE, so execution scope and answer generation stay inspectable.",
    chatEmpty:
      "Start with a single-sheet question, then continue with sequential workbook analysis (A then B). Cross-sheet join is still not supported.",
    examplesButtonLabel: "Examples",
    guideButtonLabel: "Guide",
    quickStartTitle: "Start with one guided question",
    quickStartBody: "Your file is loaded. Start with one question about the current sheet to see the result card, scope disclosure, and pipeline behavior.",
    quickStartPrimary: "Ask first question",
    quickStartSecondary: "Or choose another prompt",
    questionPlaceholder: "Ask about one sheet at a time, for example: Show the monthly revenue trend in the current sheet as a line chart, or forecast next month's total amount.",
    modeLabel: "Mode",
    modeAutoLabel: "Auto",
    modeTextLabel: "Text",
    modeChartLabel: "Chart",
    clarificationSelectedLabel: "Confirmed column",
    followupLabel: "Continue with",
    followupSuggestionSwitchToChart: "Render this result as a chart.",
    followupSuggestionSwitchToText: "Summarize this result in text only.",
    followupSuggestionRefineTop3: "Keep the same scope but limit to Top 3.",
    followupSuggestionAskTrend: "Show the trend for this metric by month.",
    followupSuggestionRecentThreePeriods: "Focus on the most recent 3 periods.",
    followupSuggestionComparePreviousPeriod: "Compare the latest period with the previous one.",
    followupSuggestionAggregateByCategory: "Group these rows by category and rank by total.",
    followupSuggestionForecastNextMonth: "Forecast the next period based on this trend.",
    followupSuggestionSummarizeOneLine: "Give a one-line summary of the key finding.",
    send: "Send",
    stop: "Stop",
    thinking: "Analyzing the active sheet...",
    aborted: "Streaming stopped by user.",
    suggestionsLabel: "Suggested prompts",
    totalRowsLabel: "Total rows",
    previewRowsLabel: "Preview rows",
    colsLoaded: "Columns loaded",
    sheetFieldSummaryLabel: "Field summary",
    sheetFieldSummaryEmpty: "No field summary available.",
    sheetOverridePendingLabel: "Next question will use this sheet",
    scopeLabel: "Execution scope",
    metadataLabel: "Metadata",
    sheetRoutingLabel: "Sheet routing",
    requestedSheetLabel: "Requested sheet",
    resolvedSheetLabel: "Resolved sheet",
    routingMethodLabel: "Matched by",
    routingWhyLabel: "Why this sheet",
    routingBoundaryLabel: "Boundary",
    routingMentionedSheetsLabel: "Mentioned sheets",
    routingChangedLabel: "Routed to another sheet",
    routingMethodSingleSheetLabel: "Single-sheet workbook",
    routingMethodExplicitLabel: "Question explicitly mentioned the sheet",
    routingMethodClarificationLabel: "Clarification selection",
    routingMethodManualOverrideLabel: "Manual sheet selection",
    routingMethodFollowupLabel: "Previous turn context",
    routingMethodAutoLabel: "Workbook auto-routing",
    routingMethodRequestedLabel: "Requested sheet fallback",
    routingBoundarySingleSheetLabel: "Single-sheet in scope",
    routingBoundaryDetectedLabel: "Multi-sheet intent detected (sequential analysis only)",
    routingBoundaryOutOfScopeLabel: "Cross-sheet join is out of scope",
    routingBoundaryOutOfScopeHint:
      "Current version supports workbook routing and sequential single-sheet analysis only. Cross-sheet join or union is not supported yet.",
    filterSummaryLabel: "Applied filters",
    topKSummaryLabel: "Top K",
    trendGrainLabel: "Trend grain",
    conclusionLabel: "Conclusion",
    evidenceLabel: "Evidence",
    riskNoteLabel: "Risk note",
    clarificationLabel: "Clarification",
    clarificationColumnLabel: "Confirm column",
    clarificationSheetLabel: "Confirm sheet",
    clarificationApplyLabel: "Use this field",
    clarificationReasonPrefix: "To keep the result accurate, please confirm this first: ",
    clarificationSelectedMessageTemplate: "Got it, use \"{value}\" and continue the same question.",
    pipelineLabel: "Execution pipeline",
    selectionPlanLabel: "Selection plan",
    transformPlanLabel: "Transform plan",
    detailRowsLabel: "Detail rows",
    evidenceTableLabel: "Evidence table",
    resultTableLabel: "Result table",
    forecastLabel: "Forecast",
    forecastBadgeLabel: "Model estimate",
    forecastTargetLabel: "Target",
    forecastEstimateLabel: "Estimated value",
    forecastRangeLabel: "Reference range",
    forecastModelLabel: "Model",
    forecastHistoryLabel: "History window",
    forecastHistoryPointsLabel: "History points",
    forecastGrainLabel: "Grain",
    forecastHorizonLabel: "Forecast horizon",
    forecastTableLabel: "Forecast output",
    forecastModelLinearLabel: "Linear regression",
    forecastModelSmoothingLabel: "Simple exponential smoothing",
    forecastGrainDayLabel: "Day",
    forecastGrainWeekLabel: "Week",
    forecastGrainMonthLabel: "Month",
    compareLabel: "Period comparison",
    compareBasisLabel: "Basis",
    compareCurrentLabel: "Current period",
    compareBaseLabel: "Base period",
    compareChangeValueLabel: "Change value",
    compareChangePctLabel: "Change %",
    compareRatioLabel: "Ratio",
    compareBasisMomLabel: "Previous period",
    compareBasisYoyLabel: "Year over year",
    chartLabel: "Chart output",
    chartPointCountLabel: "Points",
    noChartData: "No chart data returned for this answer.",
    copyAnswerLabel: "Copy answer",
    copyAnswerDoneLabel: "Copied",
    exportCsvLabel: "Export CSV",
    exportChartLabel: "Export PNG",
    exportChartDoneLabel: "Exported",
    userLabel: "User",
    assistantLabel: "Assistant",
    streamingLabel: "streaming",
    sourceSheetLabel: "Source",
    sheetSwitchFromLabel: "Switched from",
    sheetSwitchReasonLabel: "Switch reason",
    sheetSwitchReasonFollowupAnotherLabel: "Follow-up requested another sheet",
    sheetSwitchReasonFollowupExplicitLabel: "Follow-up explicitly selected this sheet",
    sheetSwitchReasonFollowupPreviousLabel: "Follow-up requested the previous sheet",
    uploadError: "Upload failed",
    uploadInvalidFileError: "Only .xlsx, .xls, and .csv files are supported.",
    uploadTooLargeError: "This file is too large to upload. Try a smaller workbook.",
    uploadServerError: "The server could not process this file upload.",
    previewError: "Preview failed",
    previewMissingError: "This uploaded workbook is no longer available. Upload it again.",
    previewServerError: "The server could not load the selected sheet preview.",
    chatError: "Conversation failed",
    chatConnectionError: "Cannot reach the streaming API. Check the backend and try again.",
    chatInterruptedError: "The streaming response was interrupted. Retry the question.",
    clarificationExpiredError: "The earlier clarification context has expired. Ask the question again.",
    restoreSessionExpiredError: "The previous workbook session has expired. Upload the file again.",
    networkError: "Cannot reach the API service. Check the local backend and try again.",
    missingFile: "Upload a spreadsheet before starting a conversation.",
    missingQuestion: "Enter a question first.",
    suggestionGroups: [
      {
        label: "Single-sheet analysis",
        prompts: [
          "How many rows are in the current sheet?",
          "Show the top 5 categories by amount in the current sheet.",
        ],
      },
      {
        label: "Sequential multi-sheet (A then B)",
        prompts: [
          "Start with Sales sheet and show monthly amount trend.",
          "Then continue on another sheet and summarize signup trend.",
        ],
      },
      {
        label: "Boundary examples (not supported yet)",
        prompts: [
          "Join Sales and Users by email and calculate conversion.",
          "Union two sheets and show one combined ranking chart.",
        ],
      },
    ],
  },
  "zh-CN": {
    eyebrow: "开源表格对话分析",
    brand: "Talk2Sheet",
    tagline: "一个支持 Excel / CSV 自然语言分析与轻量预测的独立开源全栈工作台。",
    capabilityTitle: "当前能力",
    capabilityBody: "支持 workbook 内单 sheet 智能路由、单工作表分析、整表执行口径披露、明细返回、Top N、趋势图、轻量时间序列预测和结构化执行链路反馈。",
    outOfScopeTitle: "暂不包含",
    outOfScopeBody: "当前已支持 workbook 内的单 sheet 选择与路由，但仍不支持跨 sheet 联合分析、跨工作表关联计算，以及高级统计、因果推断、复杂预测流程和私有基础设施依赖。",
    languageLabel: "语言",
    uploadTitle: "上传工作簿",
    uploadHint: "上传 .xlsx、.xls 或 .csv 文件，然后围绕 workbook 中的单个工作表发起对话。",
    uploadButton: "选择表格文件",
    uploading: "上传中...",
    workbookTitle: "工作簿与工作表",
    workbookOverviewTitle: "工作簿概览",
    workbookOverviewHint: "先看每个 sheet 的规模和关键字段，再选择要分析的工作表。",
    workbookEmpty: "尚未上传文件。",
    fileIdLabel: "文件 ID",
    fileTypeLabel: "类型",
    previewTitle: "工作表预览",
    previewLoading: "正在加载预览...",
    previewEmpty: "请选择文件和工作表查看预览。",
    chatTitle: "对话分析",
    chatHint: "后端会通过 SSE 流式返回规划元数据与最终答案，便于查看执行口径与回答生成过程。",
    chatEmpty: "建议先问单 sheet 问题，再按“先 A 后 B”顺序分析。当前仍不支持跨 sheet join。",
    examplesButtonLabel: "示例",
    guideButtonLabel: "说明",
    quickStartTitle: "先跑一个引导问题",
    quickStartBody: "文件已经加载完成。先围绕当前工作表发起一个问题，可以快速看到结果卡片、执行口径和分析链路。",
    quickStartPrimary: "先问第一个问题",
    quickStartSecondary: "或者换一个示例问题",
    questionPlaceholder: "一次只围绕一个工作表提问，例如：把当前工作表的月度营收趋势画成折线图，或者预测下个月总费用。",
    modeLabel: "模式",
    modeAutoLabel: "自动",
    modeTextLabel: "文本",
    modeChartLabel: "图表",
    clarificationSelectedLabel: "已确认字段",
    followupLabel: "继续问",
    followupSuggestionSwitchToChart: "把这个结果改成图表展示。",
    followupSuggestionSwitchToText: "把这个结果改成纯文字结论。",
    followupSuggestionRefineTop3: "保持当前口径，只看前 3 个。",
    followupSuggestionAskTrend: "按月看这个指标的趋势。",
    followupSuggestionRecentThreePeriods: "只看最近 3 个周期。",
    followupSuggestionComparePreviousPeriod: "对比最近一期和上一期的变化。",
    followupSuggestionAggregateByCategory: "把这些明细按类别汇总并做排名。",
    followupSuggestionForecastNextMonth: "基于这条趋势预测下一个周期。",
    followupSuggestionSummarizeOneLine: "用一句话总结最关键结论。",
    send: "发送",
    stop: "停止",
    thinking: "正在分析当前工作表...",
    aborted: "已停止流式返回。",
    suggestionsLabel: "示例问题",
    totalRowsLabel: "总行数",
    previewRowsLabel: "预览行数",
    colsLoaded: "已加载列数",
    sheetFieldSummaryLabel: "字段摘要",
    sheetFieldSummaryEmpty: "暂无字段摘要。",
    sheetOverridePendingLabel: "下一问将以当前 sheet 为准",
    scopeLabel: "执行口径",
    metadataLabel: "元数据",
    sheetRoutingLabel: "Sheet 路由",
    requestedSheetLabel: "请求 sheet",
    resolvedSheetLabel: "实际命中",
    routingMethodLabel: "命中方式",
    routingWhyLabel: "命中原因",
    routingBoundaryLabel: "能力边界",
    routingMentionedSheetsLabel: "提及工作表",
    routingChangedLabel: "已自动切换到其他 sheet",
    routingMethodSingleSheetLabel: "单 sheet 工作簿",
    routingMethodExplicitLabel: "问题中显式指定了 sheet",
    routingMethodClarificationLabel: "来自确认选择",
    routingMethodManualOverrideLabel: "来自手动切换 sheet",
    routingMethodFollowupLabel: "沿用上一轮上下文",
    routingMethodAutoLabel: "工作簿自动路由",
    routingMethodRequestedLabel: "按请求 sheet 兜底",
    routingBoundarySingleSheetLabel: "单 sheet 范围内",
    routingBoundaryDetectedLabel: "已识别为多 sheet 问题（仅支持顺序分析）",
    routingBoundaryOutOfScopeLabel: "涉及跨 sheet 联合分析（当前不支持）",
    routingBoundaryOutOfScopeHint: "当前版本仅支持 workbook 路由与顺序式单 sheet 分析，暂不支持跨 sheet join 或 union。",
    filterSummaryLabel: "筛选条件",
    topKSummaryLabel: "Top K 口径",
    trendGrainLabel: "趋势粒度",
    conclusionLabel: "结论",
    evidenceLabel: "依据",
    riskNoteLabel: "风险提示",
    clarificationLabel: "需要确认",
    clarificationColumnLabel: "确认字段",
    clarificationSheetLabel: "确认工作表",
    clarificationApplyLabel: "使用这个字段",
    clarificationReasonPrefix: "为保证结果准确，请先确认：",
    clarificationSelectedMessageTemplate: "已确认使用「{value}」，继续按原问题分析。",
    pipelineLabel: "执行链路",
    selectionPlanLabel: "选择计划",
    transformPlanLabel: "转换计划",
    detailRowsLabel: "明细数据",
    evidenceTableLabel: "证据表",
    resultTableLabel: "结果表格",
    forecastLabel: "预测结果",
    forecastBadgeLabel: "模型估计",
    forecastTargetLabel: "目标周期",
    forecastEstimateLabel: "预测值",
    forecastRangeLabel: "参考区间",
    forecastModelLabel: "预测模型",
    forecastHistoryLabel: "历史区间",
    forecastHistoryPointsLabel: "历史周期数",
    forecastGrainLabel: "时间粒度",
    forecastHorizonLabel: "预测步长",
    forecastTableLabel: "预测明细",
    forecastModelLinearLabel: "线性回归趋势",
    forecastModelSmoothingLabel: "简单指数平滑",
    forecastGrainDayLabel: "日",
    forecastGrainWeekLabel: "周",
    forecastGrainMonthLabel: "月",
    compareLabel: "周期对比",
    compareBasisLabel: "对比口径",
    compareCurrentLabel: "当前周期",
    compareBaseLabel: "基准周期",
    compareChangeValueLabel: "变化值",
    compareChangePctLabel: "变化率",
    compareRatioLabel: "倍数",
    compareBasisMomLabel: "环比",
    compareBasisYoyLabel: "同比",
    chartLabel: "图表结果",
    chartPointCountLabel: "点数",
    noChartData: "当前回答未返回图表数据。",
    copyAnswerLabel: "复制回答",
    copyAnswerDoneLabel: "已复制",
    exportCsvLabel: "导出 CSV",
    exportChartLabel: "导出 PNG",
    exportChartDoneLabel: "已导出",
    userLabel: "用户",
    assistantLabel: "助手",
    streamingLabel: "流式返回中",
    sourceSheetLabel: "结果来源",
    sheetSwitchFromLabel: "已从以下 sheet 切换",
    sheetSwitchReasonLabel: "切换原因",
    sheetSwitchReasonFollowupAnotherLabel: "根据追问“另一个 sheet”自动切换",
    sheetSwitchReasonFollowupExplicitLabel: "根据追问中明确指定的 sheet 切换",
    sheetSwitchReasonFollowupPreviousLabel: "根据追问“上一个 sheet”自动切换",
    uploadError: "上传失败",
    uploadInvalidFileError: "仅支持上传 .xlsx、.xls 和 .csv 文件。",
    uploadTooLargeError: "当前文件过大，暂时无法上传，请换一个更小的工作簿。",
    uploadServerError: "服务端暂时无法处理这次上传。",
    previewError: "预览失败",
    previewMissingError: "之前上传的工作簿已不可用，请重新上传文件。",
    previewServerError: "服务端暂时无法加载这个 sheet 的预览。",
    chatError: "对话失败",
    chatConnectionError: "当前无法连接流式会话接口，请确认后端已启动后再重试。",
    chatInterruptedError: "这次流式返回被中断了，请重试当前问题。",
    clarificationExpiredError: "上一轮确认上下文已失效，请重新提问一次。",
    restoreSessionExpiredError: "上一次工作簿会话已失效，请重新上传文件。",
    networkError: "当前无法连接 API 服务，请确认本地后端已启动后再试。",
    missingFile: "请先上传表格文件。",
    missingQuestion: "请先输入问题。",
    suggestionGroups: [
      {
        label: "单 Sheet 分析",
        prompts: [
          "当前工作表有多少行？",
          "按类别列出当前工作表金额前 5 名。",
        ],
      },
      {
        label: "顺序多 Sheet（先 A 后 B）",
        prompts: [
          "先分析 Sales 工作表的月度金额趋势。",
          "然后继续看另一个 sheet，并总结注册趋势。",
        ],
      },
      {
        label: "边界问题（当前不支持）",
        prompts: [
          "把 Sales 和 Users 按邮箱 join 后计算转化率。",
          "把两个 sheet union 后输出一个合并排行图。",
        ],
      },
    ],
  },
  "ja-JP": {
    eyebrow: "オープンソースの表計算対話分析",
    brand: "Talk2Sheet",
    tagline: "Excel / CSV を自然言語で分析し、軽量な予測も行える独立したオープンソースのフルスタック実装です。",
    capabilityTitle: "現在の機能",
    capabilityBody: "ワークブック内の単一シートルーティング、単一シート分析、シート全体の実行範囲表示、詳細行、Top N、トレンドチャート、軽量な時系列予測、構造化パイプライン返却に対応しています。",
    outOfScopeTitle: "MVP 対象外",
    outOfScopeBody: "ワークブック内の単一シート選択とルーティングには対応していますが、複数シートをまたぐ結合や複合分析、高度な統計、因果推論、複雑な予測ワークフロー、非公開インフラ連携はまだ対象外です。",
    languageLabel: "言語",
    uploadTitle: "ワークブックをアップロード",
    uploadHint: ".xlsx、.xls、.csv をアップロードし、ワークブック内の 1 シート単位で対話分析を開始します。",
    uploadButton: "スプレッドシートを選択",
    uploading: "アップロード中...",
    workbookTitle: "ワークブックとシート",
    workbookOverviewTitle: "ワークブック概要",
    workbookOverviewHint: "質問前に各シートの規模と主要フィールドを確認できます。",
    workbookEmpty: "まだファイルがアップロードされていません。",
    fileIdLabel: "ファイル ID",
    fileTypeLabel: "種類",
    previewTitle: "シートプレビュー",
    previewLoading: "プレビューを読み込み中...",
    previewEmpty: "ファイルとシートを選択するとプレビューが表示されます。",
    chatTitle: "対話分析",
    chatHint: "バックエンドは SSE でプランナー情報と最終回答を逐次返し、実行範囲と回答生成の流れを確認できます。",
    chatEmpty:
      "まずは単一シートの質問から始め、次にワークブック内で A→B の順次分析へ進んでください。シート横断 join はまだ未対応です。",
    examplesButtonLabel: "サンプル",
    guideButtonLabel: "説明",
    quickStartTitle: "まずはガイド付きの質問から",
    quickStartBody: "ファイルは読み込み済みです。まず現在のシートについて 1 つ質問すると、結果カード、実行範囲、パイプラインの動きが確認できます。",
    quickStartPrimary: "最初の質問を実行",
    quickStartSecondary: "別のサンプル質問を使う",
    questionPlaceholder: "1 回につき 1 シートについて質問してください。例: 現在のシートの月次売上トレンドを折れ線グラフで表示して、または来月の合計費用を予測して。",
    modeLabel: "モード",
    modeAutoLabel: "自動",
    modeTextLabel: "テキスト",
    modeChartLabel: "チャート",
    clarificationSelectedLabel: "確認した列",
    followupLabel: "続けて質問",
    followupSuggestionSwitchToChart: "この結果をチャート表示に切り替える。",
    followupSuggestionSwitchToText: "この結果をテキスト要約に切り替える。",
    followupSuggestionRefineTop3: "同じ条件のまま Top 3 に絞る。",
    followupSuggestionAskTrend: "この指標の月次トレンドを表示する。",
    followupSuggestionRecentThreePeriods: "直近 3 期間に絞って確認する。",
    followupSuggestionComparePreviousPeriod: "最新期間と前期間を比較する。",
    followupSuggestionAggregateByCategory: "この明細をカテゴリ別に集計して順位を出す。",
    followupSuggestionForecastNextMonth: "このトレンドをもとに次期間を予測する。",
    followupSuggestionSummarizeOneLine: "重要な結論を 1 行で要約する。",
    send: "送信",
    stop: "停止",
    thinking: "アクティブシートを分析中...",
    aborted: "ストリーミングを停止しました。",
    suggestionsLabel: "サンプル質問",
    totalRowsLabel: "総行数",
    previewRowsLabel: "プレビュー行数",
    colsLoaded: "読込列数",
    sheetFieldSummaryLabel: "フィールド要約",
    sheetFieldSummaryEmpty: "フィールド要約はありません。",
    sheetOverridePendingLabel: "次の質問ではこのシートを使います",
    scopeLabel: "実行範囲",
    metadataLabel: "メタデータ",
    sheetRoutingLabel: "シートルーティング",
    requestedSheetLabel: "要求シート",
    resolvedSheetLabel: "実行シート",
    routingMethodLabel: "判定方法",
    routingWhyLabel: "選定理由",
    routingBoundaryLabel: "対応範囲",
    routingMentionedSheetsLabel: "言及シート",
    routingChangedLabel: "別シートにルーティングされました",
    routingMethodSingleSheetLabel: "単一シートのブック",
    routingMethodExplicitLabel: "質問でシートが明示された",
    routingMethodClarificationLabel: "確認選択",
    routingMethodManualOverrideLabel: "手動シート選択",
    routingMethodFollowupLabel: "前ターンの文脈",
    routingMethodAutoLabel: "ワークブック自動ルーティング",
    routingMethodRequestedLabel: "要求シートへのフォールバック",
    routingBoundarySingleSheetLabel: "単一シート範囲内",
    routingBoundaryDetectedLabel: "複数シート意図を検出（順次分析のみ対応）",
    routingBoundaryOutOfScopeLabel: "シート横断 join は対象外",
    routingBoundaryOutOfScopeHint:
      "現バージョンはワークブック内ルーティングと順次単一シート分析まで対応します。シート横断 join/union にはまだ対応していません。",
    filterSummaryLabel: "適用フィルター",
    topKSummaryLabel: "Top K 条件",
    trendGrainLabel: "トレンド粒度",
    conclusionLabel: "結論",
    evidenceLabel: "根拠",
    riskNoteLabel: "注意点",
    clarificationLabel: "確認が必要です",
    clarificationColumnLabel: "列を確認",
    clarificationSheetLabel: "シートを確認",
    clarificationApplyLabel: "この列を使う",
    clarificationReasonPrefix: "結果の精度を保つため、先に確認してください: ",
    clarificationSelectedMessageTemplate: "「{value}」を使って、同じ質問を続けます。",
    pipelineLabel: "実行パイプライン",
    selectionPlanLabel: "選択プラン",
    transformPlanLabel: "変換プラン",
    detailRowsLabel: "詳細行",
    evidenceTableLabel: "根拠テーブル",
    resultTableLabel: "結果テーブル",
    forecastLabel: "予測結果",
    forecastBadgeLabel: "モデル推定",
    forecastTargetLabel: "対象期間",
    forecastEstimateLabel: "予測値",
    forecastRangeLabel: "参考区間",
    forecastModelLabel: "予測モデル",
    forecastHistoryLabel: "履歴範囲",
    forecastHistoryPointsLabel: "履歴期間数",
    forecastGrainLabel: "粒度",
    forecastHorizonLabel: "予測ホライズン",
    forecastTableLabel: "予測テーブル",
    forecastModelLinearLabel: "線形回帰",
    forecastModelSmoothingLabel: "単純指数平滑",
    forecastGrainDayLabel: "日",
    forecastGrainWeekLabel: "週",
    forecastGrainMonthLabel: "月",
    compareLabel: "期間比較",
    compareBasisLabel: "比較基準",
    compareCurrentLabel: "現在期間",
    compareBaseLabel: "基準期間",
    compareChangeValueLabel: "変化量",
    compareChangePctLabel: "変化率",
    compareRatioLabel: "比率",
    compareBasisMomLabel: "前期比",
    compareBasisYoyLabel: "前年比",
    chartLabel: "チャート出力",
    chartPointCountLabel: "点数",
    noChartData: "この回答ではチャートデータが返されませんでした。",
    copyAnswerLabel: "回答をコピー",
    copyAnswerDoneLabel: "コピー済み",
    exportCsvLabel: "CSV を書き出す",
    exportChartLabel: "PNG を書き出す",
    exportChartDoneLabel: "書き出し済み",
    userLabel: "ユーザー",
    assistantLabel: "アシスタント",
    streamingLabel: "ストリーミング中",
    sourceSheetLabel: "結果元",
    sheetSwitchFromLabel: "切替元シート",
    sheetSwitchReasonLabel: "切替理由",
    sheetSwitchReasonFollowupAnotherLabel: "フォローアップで別シート指定があったため",
    sheetSwitchReasonFollowupExplicitLabel: "フォローアップで明示シート指定があったため",
    sheetSwitchReasonFollowupPreviousLabel: "フォローアップで前のシート指定があったため",
    uploadError: "アップロード失敗",
    uploadInvalidFileError: ".xlsx、.xls、.csv ファイルのみアップロードできます。",
    uploadTooLargeError: "このファイルは大きすぎてアップロードできません。より小さいワークブックを試してください。",
    uploadServerError: "サーバーがこのアップロードを処理できませんでした。",
    previewError: "プレビュー失敗",
    previewMissingError: "以前アップロードしたワークブックは利用できなくなりました。再アップロードしてください。",
    previewServerError: "サーバーがこのシートのプレビューを読み込めませんでした。",
    chatError: "対話失敗",
    chatConnectionError: "ストリーミング API に接続できません。バックエンドを確認して再試行してください。",
    chatInterruptedError: "ストリーミング応答が中断されました。同じ質問でもう一度試してください。",
    clarificationExpiredError: "前の確認コンテキストは期限切れです。もう一度質問してください。",
    restoreSessionExpiredError: "前回のワークブックセッションは期限切れです。ファイルを再アップロードしてください。",
    networkError: "API サービスに接続できません。ローカルのバックエンドを確認して再試行してください。",
    missingFile: "先にスプレッドシートをアップロードしてください。",
    missingQuestion: "先に質問を入力してください。",
    suggestionGroups: [
      {
        label: "単一シート分析",
        prompts: [
          "現在のシートの行数は？",
          "現在のシートでカテゴリ別 Top 5 を表示。",
        ],
      },
      {
        label: "順次マルチシート（A→B）",
        prompts: [
          "まず Sales シートの月次金額トレンドを分析。",
          "次に別シートへ続けて、登録トレンドを要約。",
        ],
      },
      {
        label: "境界例（未対応）",
        prompts: [
          "Sales と Users をメールで join して転換率を計算。",
          "2 つのシートを union して 1 つのランキングチャートを作成。",
        ],
      },
    ],
  },
};

export function normalizeLocale(input: string | null | undefined): Locale {
  if (!input) {
    return "en";
  }
  const lowered = input.toLowerCase();
  if (lowered.startsWith("zh")) {
    return "zh-CN";
  }
  if (lowered.startsWith("ja")) {
    return "ja-JP";
  }
  return "en";
}
