<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from "vue";

import { copyText, downloadChartPng, downloadCsv } from "../lib/browserExport";
import type { UiMessages } from "../i18n/messages";
import type { AnswerSegments, ChatMessage } from "../types";
import ClarificationOptions from "./ClarificationOptions.vue";
import DataTable from "./DataTable.vue";
import SimpleChart from "./SimpleChart.vue";

const props = defineProps<{
  message: ChatMessage;
  showDebug?: boolean;
  ui: UiMessages;
}>();

const emit = defineEmits<{
  clarificationSelect: [value: string];
  followupSelect: [question: string];
  continueNextStep: [];
}>();

const copyState = ref<"idle" | "done">("idle");
let copyFeedbackTimer: ReturnType<typeof setTimeout> | null = null;
const chartExportState = ref<"idle" | "done">("idle");
let chartExportFeedbackTimer: ReturnType<typeof setTimeout> | null = null;

onBeforeUnmount(() => {
  if (copyFeedbackTimer) {
    clearTimeout(copyFeedbackTimer);
    copyFeedbackTimer = null;
  }
  if (chartExportFeedbackTimer) {
    clearTimeout(chartExportFeedbackTimer);
    chartExportFeedbackTimer = null;
  }
});

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function readString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function readNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const numeric = Number(value.replace(/,/g, ""));
    return Number.isFinite(numeric) ? numeric : null;
  }
  return null;
}

function formatSheetDescriptor(sheetName: string, sheetIndex: number | null): string {
  const normalizedName = sheetName.trim();
  if (normalizedName && sheetIndex) {
    return `${normalizedName} (#${sheetIndex})`;
  }
  if (normalizedName) {
    return normalizedName;
  }
  if (sheetIndex) {
    return `#${sheetIndex}`;
  }
  return "—";
}

function formatSheetSwitchReason(reason: string): string {
  const normalized = reason.trim().toLowerCase();
  if (!normalized) {
    return "";
  }
  if (normalized === "followup_switch_to_another_sheet") {
    return props.ui.sheetSwitchReasonFollowupAnotherLabel;
  }
  if (normalized === "followup_switch_to_explicit_sheet") {
    return props.ui.sheetSwitchReasonFollowupExplicitLabel;
  }
  if (normalized === "followup_switch_to_previous_sheet") {
    return props.ui.sheetSwitchReasonFollowupPreviousLabel;
  }
  return "";
}

function normalizeTaskStepStatus(status: string): "pending" | "current" | "completed" | "failed" {
  const normalized = status.trim().toLowerCase();
  if (normalized === "current") {
    return "current";
  }
  if (normalized === "completed") {
    return "completed";
  }
  if (normalized === "failed") {
    return "failed";
  }
  return "pending";
}

function formatTaskStepStatusLabel(status: "pending" | "current" | "completed" | "failed"): string {
  if (status === "current") {
    return props.ui.taskStepCurrentLabel;
  }
  if (status === "completed") {
    return props.ui.taskStepCompletedLabel;
  }
  if (status === "failed") {
    return props.ui.taskStepFailedLabel;
  }
  return props.ui.taskStepPendingLabel;
}

const detailRows = computed(() => {
  const rows = props.message.pipeline?.preview_rows;
  return Array.isArray(rows) ? (rows as unknown[][]) : [];
});

const detailColumns = computed(() => {
  const pipelineColumns = Array.isArray(props.message.pipeline?.result_columns)
    ? (props.message.pipeline?.result_columns as unknown[]).filter((item): item is string => typeof item === "string")
    : [];
  const fallbackColumns = Array.isArray(props.message.tableColumns) ? props.message.tableColumns : [];
  const rowWidth = detailRows.value[0]?.length ?? 0;
  const preferred = pipelineColumns.length ? pipelineColumns : fallbackColumns;
  if (!rowWidth) {
    return preferred;
  }
  if (preferred.length === rowWidth) {
    return preferred;
  }
  if (pipelineColumns.length >= rowWidth) {
    return pipelineColumns.slice(0, rowWidth);
  }
  if (fallbackColumns.length >= rowWidth) {
    return fallbackColumns.slice(0, rowWidth);
  }
  return Array.from({ length: rowWidth }, (_, index) => `Column ${index + 1}`);
});

const selectionPlan = computed(() => {
  return props.message.pipeline?.selection_plan ?? null;
});

const transformPlan = computed(() => {
  return props.message.pipeline?.transform_plan ?? null;
});

function formatFilterSummary(filters: Array<{ col?: unknown; op?: unknown; value?: unknown }>): string {
  const parts = filters
    .map((filter) => {
      const col = readString(filter.col);
      const op = readString(filter.op);
      const value = formatDisplayValue(filter.value);
      if (!col) {
        return "";
      }
      if (!op) {
        return `${col} = ${value}`;
      }
      return `${col} ${op} ${value}`;
    })
    .filter((item) => item);
  return parts.join(" · ");
}

const analysisSummaryMeta = computed(() => {
  const planner = plannerMeta.value;
  const selection = asRecord(selectionPlan.value);
  const transform = asRecord(transformPlan.value);
  const plannerIntent = readString(planner?.intent);

  const plannerValueFilters = Array.isArray(planner?.value_filters)
    ? (planner?.value_filters as unknown[])
        .map((item) => asRecord(item))
        .filter((item): item is Record<string, unknown> => Boolean(item))
        .map((item) => ({ col: item.column, op: "=", value: item.value }))
    : [];
  const selectionFilters = Array.isArray(selection?.filters)
    ? (selection?.filters as unknown[])
        .map((item) => asRecord(item))
        .filter((item): item is Record<string, unknown> => Boolean(item))
    : [];
  const filters = plannerValueFilters.length ? plannerValueFilters : selectionFilters;
  const filtersText = filters.length ? formatFilterSummary(filters as Array<{ col?: unknown; op?: unknown; value?: unknown }>) : "";

  const transformTopK = readNumber(transform?.top_k);
  const plannerTopK = readNumber(planner?.top_k);
  const topK = transformTopK ?? plannerTopK;

  let trendGrain = "";
  if (plannerIntent === "trend") {
    trendGrain = readString(planner?.bucket_grain);
    if (!trendGrain && Array.isArray(transform?.derived_columns)) {
      const firstDerived = asRecord(transform.derived_columns[0]);
      trendGrain = readString(firstDerived?.grain);
    }
  }
  const trendGrainText = trendGrain ? forecastGrainLabel(trendGrain) : "";

  if (!filtersText && !topK && !trendGrainText) {
    return null;
  }

  return {
    filtersText,
    topK: topK ? String(topK) : "",
    trendGrainText,
  };
});

function uniquePrompts(candidates: string[]): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const candidate of candidates) {
    const normalized = candidate.trim();
    if (!normalized || seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    output.push(normalized);
  }
  return output;
}

const plannerMeta = computed(() => asRecord(props.message.pipeline?.planner));
const answerGenerationMeta = computed(() => asRecord(props.message.pipeline?.answer_generation));
const transformMeta = computed(() => asRecord(props.message.pipeline?.transform_meta));
const forecastMetaSource = computed(() => asRecord(transformMeta.value?.forecast));

function forecastModelLabel(model: string): string {
  const normalized = model.trim().toLowerCase();
  if (normalized === "linear_regression") {
    return props.ui.forecastModelLinearLabel;
  }
  if (normalized === "simple_exponential_smoothing") {
    return props.ui.forecastModelSmoothingLabel;
  }
  return model || "—";
}

function forecastGrainLabel(grain: string): string {
  const normalized = grain.trim().toLowerCase();
  if (normalized === "day") {
    return props.ui.forecastGrainDayLabel;
  }
  if (normalized === "week") {
    return props.ui.forecastGrainWeekLabel;
  }
  if (normalized === "month") {
    return props.ui.forecastGrainMonthLabel;
  }
  return grain || "—";
}

function formatDisplayValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (typeof value === "string") {
    return value.trim() || "—";
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const rounded = Math.round(value);
    if (Math.abs(value - rounded) < 1e-9) {
      return rounded.toLocaleString();
    }
    return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
  }
  return String(value);
}

const forecastMeta = computed(() => {
  const source = forecastMetaSource.value;
  const answerMeta = answerGenerationMeta.value;
  const planner = plannerMeta.value;
  const summaryKind = readString(answerMeta?.summary_kind);
  const intent = readString(planner?.intent);
  const isForecast = props.message.role === "assistant" && (summaryKind === "forecast_timeseries" || intent === "forecast_timeseries" || Boolean(source));
  if (!isForecast) {
    return null;
  }

  const period = readString(answerMeta?.period) || readString(source?.target_period);
  const estimate =
    readString(answerMeta?.forecast_value) ||
    formatDisplayValue(readNumber(source?.forecast_value) ?? source?.forecast_value);
  const lowerBound =
    readString(answerMeta?.lower_bound) ||
    formatDisplayValue(readNumber(source?.lower_bound) ?? source?.lower_bound);
  const upperBound =
    readString(answerMeta?.upper_bound) ||
    formatDisplayValue(readNumber(source?.upper_bound) ?? source?.upper_bound);
  const model = readString(source?.model) || readString(answerMeta?.model);
  const grain = readString(source?.grain);
  const historyStart = readString(source?.history_start);
  const historyEnd = readString(source?.history_end);
  const historyPoints = readNumber(source?.history_points);
  const horizon = readNumber(source?.horizon);

  return {
    period: period || "—",
    estimate,
    range: lowerBound !== "—" && upperBound !== "—" ? `${lowerBound} - ${upperBound}` : "—",
    modelLabel: forecastModelLabel(model),
    grainLabel: forecastGrainLabel(grain),
    historyRange: historyStart && historyEnd ? `${historyStart} - ${historyEnd}` : historyStart || historyEnd || "—",
    historyPoints: historyPoints !== null ? String(historyPoints) : "",
    horizon: horizon !== null ? String(horizon) : "",
  };
});

function compareBasisLabel(basis: string): string {
  const normalized = basis.trim().toLowerCase();
  if (normalized === "year_over_year") {
    return props.ui.compareBasisYoyLabel;
  }
  return props.ui.compareBasisMomLabel;
}

const periodCompareMeta = computed(() => {
  const planner = plannerMeta.value;
  const answerMeta = answerGenerationMeta.value;
  const summaryKind = readString(answerMeta?.summary_kind);
  const intent = readString(planner?.intent);
  const isPeriodCompare = props.message.role === "assistant" && (summaryKind === "period_compare" || intent === "period_compare");
  if (!isPeriodCompare) {
    return null;
  }

  const currentPeriod = readString(answerMeta?.current_period) || readString(planner?.current_period);
  const previousPeriod = readString(answerMeta?.previous_period) || readString(planner?.previous_period);
  const currentValue = readString(answerMeta?.current_value);
  const previousValue = readString(answerMeta?.previous_value);
  const changeValue = readString(answerMeta?.change_value);
  const changePct = readString(answerMeta?.change_pct);
  const ratioValue = readString(answerMeta?.compare_ratio);
  const compareBasis = readString(answerMeta?.compare_basis) || readString(planner?.compare_basis) || "previous_period";

  if (!currentPeriod || !previousPeriod) {
    return null;
  }

  return {
    currentPeriod,
    previousPeriod,
    currentValue: currentValue || "—",
    previousValue: previousValue || "—",
    changeValue: changeValue || "—",
    changePct: changePct || "—",
    ratioValue: ratioValue || "—",
    basisLabel: compareBasisLabel(compareBasis),
  };
});

const detailTableLabel = computed(() => {
  if (forecastMeta.value) {
    return props.ui.forecastTableLabel;
  }
  const summaryKind = readString(answerGenerationMeta.value?.summary_kind);
  if (summaryKind === "detail") {
    return props.ui.evidenceTableLabel;
  }
  const isDetailRows = Boolean((transformPlan.value as { return_rows?: boolean } | null)?.return_rows);
  return isDetailRows ? props.ui.detailRowsLabel : props.ui.resultTableLabel;
});

const compactTable = computed(() => {
  const isDetailRows = Boolean((transformPlan.value as { return_rows?: boolean } | null)?.return_rows);
  return !isDetailRows;
});

const copyableAnswerText = computed(() => {
  const sections: string[] = [];
  if (structuredConclusion.value) {
    sections.push(`${props.ui.conclusionLabel}\n${structuredConclusion.value}`);
  }
  if (structuredEvidence.value) {
    sections.push(`${props.ui.evidenceLabel}\n${structuredEvidence.value}`);
  }
  if (structuredRiskNote.value) {
    sections.push(`${props.ui.riskNoteLabel}\n${structuredRiskNote.value}`);
  }

  if (sections.length) {
    return sections.join("\n\n");
  }

  const messageText = readString(props.message.text);
  if (messageText) {
    return messageText;
  }
  return readString(props.message.analysisText);
});

const exportCsvFilename = computed(() => {
  const routing = asRecord(props.message.pipeline?.sheet_routing);
  const resolvedSheetIndex = readNumber(routing?.resolved_sheet_index);
  const tableType = forecastMeta.value ? "forecast" : "result";
  return resolvedSheetIndex
    ? `talk2sheet-sheet-${resolvedSheetIndex}-${tableType}.csv`
    : `talk2sheet-${tableType}.csv`;
});

const canExportChart = computed(() => {
  return Boolean(props.message.chartSpec && (props.message.chartData?.length ?? 0) > 0);
});

const chartExportFilename = computed(() => {
  const routing = asRecord(props.message.pipeline?.sheet_routing);
  const resolvedSheetIndex = readNumber(routing?.resolved_sheet_index);
  const chartType = props.message.chartSpec?.type || "chart";
  return resolvedSheetIndex
    ? `talk2sheet-sheet-${resolvedSheetIndex}-${chartType}.png`
    : `talk2sheet-${chartType}.png`;
});

const chartContextMeta = computed(() => {
  const raw = asRecord(props.message.pipeline?.chart_context);
  if (!raw) {
    return null;
  }
  const title = readString(raw.title);
  const xLabel = readString(raw.x_label);
  const yLabel = readString(raw.y_label);
  const yUnit = readString(raw.y_unit);
  const pointCount = readNumber(raw.point_count);
  const fallbackReason = readString(raw.fallback_reason);
  const rendered = raw.rendered === true;
  const requested = raw.requested === true;
  if (!requested && !title && !xLabel && !yLabel && !fallbackReason) {
    return null;
  }
  return {
    title,
    xLabel,
    yLabel,
    yUnit,
    pointCount,
    fallbackReason,
    rendered,
    requested,
  };
});

const chartFallbackNote = computed(() => {
  const context = chartContextMeta.value;
  if (!context || !context.requested || context.rendered) {
    return "";
  }
  return context.fallbackReason;
});

const answerSegments = computed<AnswerSegments | null>(() => {
  if (props.message.answerSegments) {
    return props.message.answerSegments;
  }
  const raw = ((props.message.pipeline?.answer_generation as Record<string, unknown> | undefined)?.segments ?? null) as
    | Record<string, unknown>
    | null;
  if (!raw) {
    return null;
  }
  const conclusion = typeof raw.conclusion === "string" ? raw.conclusion : "";
  const evidence = typeof raw.evidence === "string" ? raw.evidence : "";
  const riskNote =
    typeof raw.risk_note === "string" ? raw.risk_note : typeof raw.riskNote === "string" ? raw.riskNote : "";
  if (!conclusion && !evidence && !riskNote) {
    return null;
  }
  return {
    conclusion: conclusion || undefined,
    evidence: evidence || undefined,
    riskNote: riskNote || undefined,
  };
});

const structuredConclusion = computed(() => {
  return answerSegments.value?.conclusion || (props.message.role === "assistant" ? props.message.text : "");
});

const structuredEvidence = computed(() => {
  return answerSegments.value?.evidence || "";
});

const structuredRiskNote = computed(() => {
  return answerSegments.value?.riskNote || "";
});

const hasStructuredAnswer = computed(() => {
  if (props.message.role !== "assistant") {
    return false;
  }
  return Boolean(structuredConclusion.value || structuredEvidence.value || structuredRiskNote.value);
});

const fallbackAnalysisText = computed(() => {
  if (hasStructuredAnswer.value) {
    return "";
  }
  if (props.message.analysisText && props.message.analysisText !== props.message.text) {
    return props.message.analysisText;
  }
  return "";
});

const clarification = computed(() => {
  return props.message.clarification || null;
});

const clarificationTitle = computed(() => {
  if (clarification.value?.kind === "sheet_resolution") {
    return props.ui.clarificationSheetLabel;
  }
  return props.ui.clarificationColumnLabel;
});

const sheetRoutingMeta = computed(() => {
  const routing = asRecord(props.message.pipeline?.sheet_routing);
  if (!routing) {
    return null;
  }

  const requestedSheetIndex = readNumber(routing.requested_sheet_index);
  const resolvedSheetIndex = readNumber(routing.resolved_sheet_index);
  const resolvedSheetName = readString(routing.resolved_sheet_name);
  const workbookSheetCount = readNumber(routing.workbook_sheet_count) || 0;
  const matchedBy = readString(routing.matched_by);
  const reason = readString(routing.reason);
  const boundaryStatus = readString(routing.boundary_status);
  const boundaryReason = readString(routing.boundary_reason);
  const decompositionHint = readString(routing.decomposition_hint);
  const routingExplanation = readString(routing.explanation);
  const mentionedSheets = Array.isArray(routing.mentioned_sheets)
    ? (routing.mentioned_sheets as unknown[])
        .map((item) => asRecord(item))
        .filter((item): item is Record<string, unknown> => Boolean(item))
        .map((item) => formatSheetDescriptor(readString(item.sheet_name), readNumber(item.sheet_index)))
        .filter((item) => item && item !== "—")
    : [];

  if (!resolvedSheetIndex && reason !== "ambiguous_sheet_match") {
    return null;
  }

  const meta = asRecord(props.message.meta);
  const requestedSheetName = readString(meta?.requested_sheet_name);
  const changed = Boolean(requestedSheetIndex && resolvedSheetIndex && requestedSheetIndex !== resolvedSheetIndex);

  let methodLabel = props.ui.routingMethodRequestedLabel;
  if (matchedBy === "single_sheet") {
    methodLabel = props.ui.routingMethodSingleSheetLabel;
  } else if (matchedBy === "explicit_reference") {
    methodLabel = props.ui.routingMethodExplicitLabel;
  } else if (matchedBy === "clarification_resolution") {
    methodLabel = props.ui.routingMethodClarificationLabel;
  } else if (matchedBy === "requested_override") {
    methodLabel = props.ui.routingMethodManualOverrideLabel;
  } else if (matchedBy === "followup") {
    methodLabel = props.ui.routingMethodFollowupLabel;
  } else if (matchedBy === "auto_routing") {
    methodLabel = props.ui.routingMethodAutoLabel;
  }

  if (workbookSheetCount <= 1 && !changed && matchedBy === "requested_sheet") {
    return null;
  }

  let boundaryLabel = props.ui.routingBoundarySingleSheetLabel;
  if (boundaryStatus === "multi_sheet_detected") {
    boundaryLabel = props.ui.routingBoundaryDetectedLabel;
  } else if (boundaryStatus === "multi_sheet_out_of_scope") {
    boundaryLabel = props.ui.routingBoundaryOutOfScopeLabel;
  }

  const boundaryOutOfScope = boundaryStatus === "multi_sheet_out_of_scope" || boundaryReason === "cross_sheet_join_not_supported";

  return {
    requestedLabel: formatSheetDescriptor(requestedSheetName, requestedSheetIndex),
    resolvedLabel: formatSheetDescriptor(resolvedSheetName, resolvedSheetIndex),
    methodLabel,
    explanationLabel: routingExplanation,
    changed,
    boundaryLabel,
    boundaryNote: boundaryOutOfScope ? props.ui.routingBoundaryOutOfScopeHint : "",
    decompositionHint,
    mentionedSheetsLabel: mentionedSheets.join(", "),
  };
});

const sourceSheetMeta = computed(() => {
  if (props.message.role !== "assistant") {
    return null;
  }
  const pipeline = asRecord(props.message.pipeline);
  if (!pipeline) {
    return null;
  }
  const routing = asRecord(pipeline.sheet_routing);
  const sequence = asRecord(pipeline.sheet_sequence);

  const sourceSheetIndex = readNumber(pipeline.source_sheet_index) ?? readNumber(routing?.resolved_sheet_index);
  const sourceSheetName = readString(pipeline.source_sheet_name) || readString(routing?.resolved_sheet_name);
  if (!sourceSheetIndex && !sourceSheetName) {
    return null;
  }

  const previousSheetIndex = readNumber(sequence?.previous_sheet_index);
  const previousSheetName = readString(sequence?.previous_sheet_name);
  const switchedFromPrevious =
    sequence?.switched_from_previous === true || readString(routing?.reason).startsWith("followup_switch_to_");
  const switchReason = readString(sequence?.last_sheet_switch_reason) || readString(routing?.reason);
  const switchedFromLabel =
    switchedFromPrevious && (previousSheetIndex || previousSheetName)
      ? formatSheetDescriptor(previousSheetName, previousSheetIndex)
      : "";
  const switchReasonLabel = switchedFromPrevious ? formatSheetSwitchReason(switchReason) : "";

  return {
    sourceLabel: formatSheetDescriptor(sourceSheetName, sourceSheetIndex),
    switchedFromLabel,
    switchReasonLabel,
  };
});

const analysisAnchorNotice = computed(() => {
  if (props.message.role !== "assistant") {
    return null;
  }
  const pipeline = asRecord(props.message.pipeline);
  if (!pipeline) {
    return null;
  }
  const reused = pipeline.analysis_anchor_reused === true;
  const anchor = asRecord(pipeline.analysis_anchor);
  if (!reused || !anchor) {
    return null;
  }
  return {
    label: props.ui.analysisAnchorLabel,
    hint: props.ui.analysisAnchorHint,
  };
});

const taskStepsMeta = computed(() => {
  if (props.message.role !== "assistant") {
    return null;
  }
  const pipeline = asRecord(props.message.pipeline);
  if (!pipeline) {
    return null;
  }
  const rawSteps = Array.isArray(pipeline.task_steps) ? (pipeline.task_steps as unknown[]) : [];
  const steps = rawSteps
    .map((item) => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item, index) => {
      const stepId = readString(item.step_id) || `step-${index + 1}`;
      const sheetIndex = readNumber(item.sheet_index);
      const sheetName = readString(item.sheet_name);
      const status = normalizeTaskStepStatus(readString(item.status));
      return {
        stepId,
        status,
        statusLabel: formatTaskStepStatusLabel(status),
        sheetLabel: formatSheetDescriptor(sheetName, sheetIndex),
      };
    })
    .filter((item) => item.sheetLabel !== "—");
  if (!steps.length) {
    return null;
  }

  const currentStepId = readString(pipeline.current_step_id);
  const hasExplicitCurrentStep = Boolean(currentStepId && steps.some((step) => step.stepId === currentStepId));
  return {
    currentStepId: hasExplicitCurrentStep ? currentStepId : "",
    steps,
  };
});

const continueNextStepMeta = computed(() => {
  if (props.message.role !== "assistant" || props.message.status === "streaming" || Boolean(clarification.value)) {
    return null;
  }
  const stepState = taskStepsMeta.value;
  if (!stepState || stepState.steps.length <= 1) {
    return null;
  }
  const currentIndex = stepState.currentStepId
    ? stepState.steps.findIndex((step) => step.stepId === stepState.currentStepId)
    : -1;
  let target =
    currentIndex >= 0 && currentIndex + 1 < stepState.steps.length
      ? stepState.steps[currentIndex + 1]
      : null;
  if (target && !["pending", "failed"].includes(target.status)) {
    target = null;
  }
  if (!target) {
    target = stepState.steps.find((step) => step.status === "pending" || step.status === "failed") || null;
  }
  if (!target) {
    return null;
  }
  return {
    targetLabel: target.sheetLabel,
  };
});

const stepComparisonMeta = computed(() => {
  if (props.message.role !== "assistant") {
    return null;
  }
  const pipeline = asRecord(props.message.pipeline);
  if (!pipeline) {
    return null;
  }
  const comparison = asRecord(pipeline.step_comparison);
  if (!comparison) {
    return null;
  }
  const previous = asRecord(comparison.previous_step);
  const current = asRecord(comparison.current_step);
  if (!previous || !current) {
    return null;
  }
  const previousSheetLabel = formatSheetDescriptor(readString(previous.sheet_name), readNumber(previous.sheet_index));
  const currentSheetLabel = formatSheetDescriptor(readString(current.sheet_name), readNumber(current.sheet_index));
  if (previousSheetLabel === "—" || currentSheetLabel === "—" || previousSheetLabel === currentSheetLabel) {
    return null;
  }
  const previousSummary = readString(previous.answer_summary) || readString(previous.intent);
  const currentSummary =
    readString(current.answer_summary) ||
    readString(current.intent) ||
    readString(structuredConclusion.value) ||
    readString(props.message.text);
  if (!previousSummary || !currentSummary) {
    return null;
  }
  return {
    previousTitle: `${props.ui.stepComparisonPreviousLabel} · ${previousSheetLabel}`,
    previousSummary,
    currentTitle: `${props.ui.stepComparisonCurrentLabel} · ${currentSheetLabel}`,
    currentSummary,
    independentHint: comparison.independent_scopes === false ? "" : props.ui.stepComparisonIndependentHint,
  };
});

const followupSuggestions = computed(() => {
  if (props.message.role !== "assistant" || props.message.status === "streaming" || Boolean(clarification.value)) {
    return [];
  }
  const planner = plannerMeta.value;
  const answerMeta = answerGenerationMeta.value;
  const intent = readString(planner?.intent) || readString(answerMeta?.summary_kind);
  const hasChart = Boolean(props.message.chartSpec);

  const candidates: string[] = [];
  if (!hasChart) {
    candidates.push(props.ui.followupSuggestionSwitchToChart);
  } else {
    candidates.push(props.ui.followupSuggestionSwitchToText);
  }

  if (intent === "ranking" || intent === "share") {
    candidates.push(props.ui.followupSuggestionRefineTop3);
    candidates.push(props.ui.followupSuggestionAskTrend);
  } else if (intent === "trend") {
    candidates.push(props.ui.followupSuggestionRecentThreePeriods);
    candidates.push(props.ui.followupSuggestionComparePreviousPeriod);
  } else if (intent === "period_compare") {
    candidates.push(props.ui.followupSuggestionAskTrend);
    candidates.push(props.ui.followupSuggestionComparePreviousPeriod);
  } else if (intent === "detail_rows" || intent === "detail") {
    candidates.push(props.ui.followupSuggestionAggregateByCategory);
    candidates.push(props.ui.followupSuggestionRefineTop3);
  } else if (intent === "forecast_timeseries") {
    candidates.push(props.ui.followupSuggestionForecastNextMonth);
    candidates.push(props.ui.followupSuggestionRecentThreePeriods);
  } else {
    candidates.push(props.ui.followupSuggestionSummarizeOneLine);
  }

  return uniquePrompts(candidates).slice(0, 3);
});

function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

function formatMetaValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

async function handleCopyAnswer(): Promise<void> {
  if (!copyableAnswerText.value) {
    return;
  }
  try {
    await copyText(copyableAnswerText.value);
    copyState.value = "done";
    if (copyFeedbackTimer) {
      clearTimeout(copyFeedbackTimer);
    }
    copyFeedbackTimer = globalThis.setTimeout(() => {
      copyState.value = "idle";
      copyFeedbackTimer = null;
    }, 1800);
  } catch {
    copyState.value = "idle";
  }
}

function handleExportCsv(): void {
  downloadCsv(exportCsvFilename.value, detailColumns.value, detailRows.value);
}

async function handleExportChart(): Promise<void> {
  if (!props.message.chartSpec || !canExportChart.value) {
    return;
  }
  try {
    await downloadChartPng(chartExportFilename.value, props.message.chartSpec, props.message.chartData || []);
    chartExportState.value = "done";
    if (chartExportFeedbackTimer) {
      clearTimeout(chartExportFeedbackTimer);
    }
    chartExportFeedbackTimer = globalThis.setTimeout(() => {
      chartExportState.value = "idle";
      chartExportFeedbackTimer = null;
    }, 1800);
  } catch {
    chartExportState.value = "idle";
  }
}
</script>

<template>
  <article class="message-card" :class="`message-card-${message.role}`">
    <div class="message-head">
      <span class="message-role">
        {{ message.role === "user" ? ui.userLabel : ui.assistantLabel }}
      </span>
      <span v-if="message.status === 'streaming'" class="message-streaming">{{ ui.streamingLabel }}</span>
    </div>

    <div v-if="sourceSheetMeta" class="message-source-sheet">
      <span class="source-pill">{{ ui.sourceSheetLabel }}: {{ sourceSheetMeta.sourceLabel }}</span>
      <span v-if="sourceSheetMeta.switchedFromLabel" class="source-pill source-pill-emphasis">
        {{ ui.sheetSwitchFromLabel }} {{ sourceSheetMeta.switchedFromLabel }}
      </span>
      <span v-if="sourceSheetMeta.switchReasonLabel" class="source-pill source-pill-subtle">
        {{ ui.sheetSwitchReasonLabel }}: {{ sourceSheetMeta.switchReasonLabel }}
      </span>
    </div>

    <div v-if="analysisAnchorNotice" class="message-anchor-notice">
      <span class="source-pill source-pill-subtle">{{ analysisAnchorNotice.label }}: {{ analysisAnchorNotice.hint }}</span>
    </div>

    <div v-if="taskStepsMeta" class="message-task-steps">
      <div class="section-label">{{ ui.taskStepsLabel }}</div>
      <ol class="task-step-list">
        <li
          v-for="step in taskStepsMeta.steps"
          :key="step.stepId"
          class="task-step-item"
          :class="[`task-step-${step.status}`, { 'task-step-active': taskStepsMeta.currentStepId === step.stepId }]"
        >
          <span class="task-step-main">{{ step.sheetLabel }}</span>
          <span class="task-step-badges">
            <span v-if="taskStepsMeta.currentStepId === step.stepId" class="task-step-current">{{ ui.taskCurrentStepLabel }}</span>
            <span class="task-step-status">{{ step.statusLabel }}</span>
          </span>
        </li>
      </ol>
      <div v-if="continueNextStepMeta" class="task-step-actions">
        <button
          type="button"
          class="message-action message-action-secondary message-action-next-step"
          @click="emit('continueNextStep')"
        >
          {{ ui.followupContinueNextStepLabel }} · {{ continueNextStepMeta.targetLabel }}
        </button>
      </div>
    </div>

    <div v-if="stepComparisonMeta" class="message-step-comparison">
      <div class="section-label">{{ ui.stepComparisonLabel }}</div>
      <div class="step-comparison-grid">
        <div class="step-comparison-card">
          <div class="step-comparison-title">{{ stepComparisonMeta.previousTitle }}</div>
          <p class="step-comparison-text">{{ stepComparisonMeta.previousSummary }}</p>
        </div>
        <div class="step-comparison-card step-comparison-card-current">
          <div class="step-comparison-title">{{ stepComparisonMeta.currentTitle }}</div>
          <p class="step-comparison-text">{{ stepComparisonMeta.currentSummary }}</p>
        </div>
      </div>
      <div v-if="stepComparisonMeta.independentHint" class="routing-note">
        {{ stepComparisonMeta.independentHint }}
      </div>
    </div>

    <template v-if="message.role === 'assistant' && hasStructuredAnswer">
      <div class="answer-stack">
        <div v-if="periodCompareMeta" class="compare-panel">
          <div class="compare-head">
            <div class="answer-label compare-label">{{ ui.compareLabel }}</div>
            <span class="compare-badge">{{ periodCompareMeta.basisLabel }}</span>
          </div>

          <div class="compare-grid">
            <div class="compare-card compare-card-primary">
              <span class="compare-card-label">{{ ui.compareCurrentLabel }}</span>
              <strong>{{ periodCompareMeta.currentValue }}</strong>
              <small>{{ periodCompareMeta.currentPeriod }}</small>
            </div>

            <div class="compare-card">
              <span class="compare-card-label">{{ ui.compareBaseLabel }}</span>
              <strong>{{ periodCompareMeta.previousValue }}</strong>
              <small>{{ periodCompareMeta.previousPeriod }}</small>
            </div>

            <div class="compare-card">
              <span class="compare-card-label">{{ ui.compareChangeValueLabel }}</span>
              <strong>{{ periodCompareMeta.changeValue }}</strong>
            </div>

            <div class="compare-card">
              <span class="compare-card-label">{{ ui.compareChangePctLabel }}</span>
              <strong>{{ periodCompareMeta.changePct }}</strong>
              <small>{{ ui.compareRatioLabel }} {{ periodCompareMeta.ratioValue }}</small>
            </div>
          </div>
        </div>

        <div v-if="forecastMeta" class="forecast-panel">
          <div class="forecast-head">
            <div class="answer-label forecast-label">{{ ui.forecastLabel }}</div>
            <span class="forecast-badge">{{ ui.forecastBadgeLabel }}</span>
          </div>

          <div class="forecast-grid">
            <div class="forecast-card forecast-card-primary">
              <span class="forecast-card-label">{{ ui.forecastEstimateLabel }}</span>
              <strong>{{ forecastMeta.estimate }}</strong>
              <small>{{ ui.forecastTargetLabel }} {{ forecastMeta.period }}</small>
            </div>

            <div class="forecast-card">
              <span class="forecast-card-label">{{ ui.forecastRangeLabel }}</span>
              <strong>{{ forecastMeta.range }}</strong>
            </div>

            <div class="forecast-card">
              <span class="forecast-card-label">{{ ui.forecastModelLabel }}</span>
              <strong>{{ forecastMeta.modelLabel }}</strong>
              <small>{{ ui.forecastGrainLabel }} {{ forecastMeta.grainLabel }}</small>
            </div>

            <div class="forecast-card">
              <span class="forecast-card-label">{{ ui.forecastHistoryLabel }}</span>
              <strong>{{ forecastMeta.historyRange }}</strong>
              <small v-if="forecastMeta.historyPoints">
                {{ ui.forecastHistoryPointsLabel }} {{ forecastMeta.historyPoints }}
              </small>
            </div>
          </div>

          <div v-if="forecastMeta.horizon" class="forecast-footnote">
            {{ ui.forecastHorizonLabel }} {{ forecastMeta.horizon }}
          </div>
        </div>

        <div class="answer-block answer-block-conclusion">
          <div class="answer-label">{{ ui.conclusionLabel }}</div>
          <p class="message-text">{{ structuredConclusion }}</p>
        </div>

        <div v-if="structuredEvidence" class="answer-block answer-block-secondary">
          <div class="answer-label">{{ ui.evidenceLabel }}</div>
          <p class="message-secondary">{{ structuredEvidence }}</p>
        </div>

        <div v-if="structuredRiskNote" class="answer-block answer-block-risk">
          <div class="answer-label">{{ ui.riskNoteLabel }}</div>
          <p class="message-risk">{{ structuredRiskNote }}</p>
        </div>
      </div>
    </template>
    <template v-else>
      <p class="message-text">{{ message.text }}</p>
      <p v-if="fallbackAnalysisText" class="message-secondary">
        {{ fallbackAnalysisText }}
      </p>
    </template>

    <div v-if="message.role === 'assistant' && copyableAnswerText" class="message-actions">
      <button type="button" class="message-action" @click="handleCopyAnswer">
        {{ copyState === "done" ? ui.copyAnswerDoneLabel : ui.copyAnswerLabel }}
      </button>
    </div>

    <div v-if="message.role === 'assistant' && followupSuggestions.length" class="message-followup">
      <div class="section-label">{{ ui.followupLabel }}</div>
      <div class="followup-list">
        <button
          v-for="prompt in followupSuggestions"
          :key="prompt"
          type="button"
          class="message-action message-action-secondary message-action-followup"
          @click="emit('followupSelect', prompt)"
        >
          {{ prompt }}
        </button>
      </div>
    </div>

    <div v-if="message.executionDisclosure" class="message-scope">
      <div class="scope-label">{{ ui.scopeLabel }}</div>
      <div class="scope-body">
        <div>{{ message.executionDisclosure.scope_text }}</div>
        <div v-if="message.executionDisclosure.scope_warning">{{ message.executionDisclosure.scope_warning }}</div>
        <div v-if="message.executionDisclosure.fallback_reason">{{ message.executionDisclosure.fallback_reason }}</div>
      </div>
    </div>

    <div v-if="message.role === 'assistant' && sheetRoutingMeta" class="message-routing">
      <div class="section-label">{{ ui.sheetRoutingLabel }}</div>
      <div class="routing-grid">
        <div class="routing-item">
          <span>{{ ui.requestedSheetLabel }}</span>
          <strong>{{ sheetRoutingMeta.requestedLabel }}</strong>
        </div>
        <div class="routing-item">
          <span>{{ ui.resolvedSheetLabel }}</span>
          <strong>{{ sheetRoutingMeta.resolvedLabel }}</strong>
        </div>
        <div class="routing-item">
          <span>{{ ui.routingMethodLabel }}</span>
          <strong>{{ sheetRoutingMeta.methodLabel }}</strong>
        </div>
        <div v-if="sheetRoutingMeta.explanationLabel" class="routing-item">
          <span>{{ ui.routingWhyLabel }}</span>
          <strong>{{ sheetRoutingMeta.explanationLabel }}</strong>
        </div>
        <div class="routing-item">
          <span>{{ ui.routingBoundaryLabel }}</span>
          <strong>{{ sheetRoutingMeta.boundaryLabel }}</strong>
        </div>
        <div v-if="sheetRoutingMeta.mentionedSheetsLabel" class="routing-item">
          <span>{{ ui.routingMentionedSheetsLabel }}</span>
          <strong>{{ sheetRoutingMeta.mentionedSheetsLabel }}</strong>
        </div>
      </div>
      <div v-if="sheetRoutingMeta.changed" class="routing-note">{{ ui.routingChangedLabel }}</div>
      <div v-if="sheetRoutingMeta.boundaryNote" class="routing-note">{{ sheetRoutingMeta.boundaryNote }}</div>
      <div v-if="sheetRoutingMeta.decompositionHint" class="routing-note">{{ sheetRoutingMeta.decompositionHint }}</div>
    </div>

    <div v-if="message.role === 'assistant' && analysisSummaryMeta" class="message-analysis-summary">
      <div class="summary-grid">
        <div v-if="analysisSummaryMeta.filtersText" class="summary-item">
          <span>{{ ui.filterSummaryLabel }}</span>
          <strong>{{ analysisSummaryMeta.filtersText }}</strong>
        </div>
        <div v-if="analysisSummaryMeta.topK" class="summary-item">
          <span>{{ ui.topKSummaryLabel }}</span>
          <strong>{{ analysisSummaryMeta.topK }}</strong>
        </div>
        <div v-if="analysisSummaryMeta.trendGrainText" class="summary-item">
          <span>{{ ui.trendGrainLabel }}</span>
          <strong>{{ analysisSummaryMeta.trendGrainText }}</strong>
        </div>
      </div>
    </div>

    <ClarificationOptions
      v-if="clarification"
      :clarification="clarification"
      :title="clarificationTitle"
      :apply-label="ui.clarificationApplyLabel"
      :reason-prefix="ui.clarificationReasonPrefix"
      @select="emit('clarificationSelect', $event)"
    />

    <details v-if="props.showDebug && message.meta" class="message-meta">
      <summary>{{ ui.metadataLabel }}</summary>
      <dl class="meta-grid">
        <template v-for="(value, key) in message.meta" :key="key">
          <dt>{{ key }}</dt>
          <dd>{{ formatMetaValue(value) }}</dd>
        </template>
      </dl>
    </details>

    <div v-if="message.chartSpec" class="message-chart">
      <div class="section-head">
        <div class="section-label">{{ ui.chartLabel }}</div>
        <button
          v-if="canExportChart"
          type="button"
          class="message-action message-action-secondary message-action-chart"
          @click="handleExportChart"
        >
          {{ chartExportState === "done" ? ui.exportChartDoneLabel : ui.exportChartLabel }}
        </button>
      </div>
      <div v-if="chartContextMeta && (chartContextMeta.title || chartContextMeta.xLabel || chartContextMeta.yLabel)" class="chart-context">
        <div v-if="chartContextMeta.title" class="chart-context-title">{{ chartContextMeta.title }}</div>
        <div v-if="chartContextMeta.xLabel || chartContextMeta.yLabel" class="chart-context-meta">
          X: {{ chartContextMeta.xLabel || "—" }} · Y: {{ chartContextMeta.yLabel || "—" }}
          <span v-if="chartContextMeta.yUnit"> ({{ chartContextMeta.yUnit }})</span>
        </div>
        <div v-if="chartContextMeta.pointCount !== null" class="chart-context-meta">
          {{ ui.chartPointCountLabel }}: {{ chartContextMeta.pointCount }}
        </div>
      </div>
      <SimpleChart
        :spec="message.chartSpec"
        :data="message.chartData || []"
        :no-data-text="ui.noChartData"
      />
    </div>

    <div v-if="!message.chartSpec && chartFallbackNote" class="message-chart-fallback">
      <div class="section-label">{{ ui.chartLabel }}</div>
      <p class="message-secondary">{{ chartFallbackNote }}</p>
    </div>

    <div v-if="detailRows.length" class="message-detail-table">
      <div class="section-head">
        <div class="section-label">{{ detailTableLabel }}</div>
        <button type="button" class="message-action message-action-secondary message-action-csv" @click="handleExportCsv">
          {{ ui.exportCsvLabel }}
        </button>
      </div>
      <DataTable
        :columns="detailColumns"
        :rows="detailRows"
        :empty-text="ui.noChartData"
        :compact="compactTable"
      />
    </div>

    <details v-if="props.showDebug && message.pipeline" class="message-pipeline">
      <summary>{{ ui.pipelineLabel }}</summary>

      <div v-if="selectionPlan" class="pipeline-block">
        <div class="section-label">{{ ui.selectionPlanLabel }}</div>
        <pre>{{ formatJson(selectionPlan) }}</pre>
      </div>

      <div v-if="transformPlan" class="pipeline-block">
        <div class="section-label">{{ ui.transformPlanLabel }}</div>
        <pre>{{ formatJson(transformPlan) }}</pre>
      </div>
    </details>
  </article>
</template>

<style scoped>
.message-card {
  border-radius: 24px;
  padding: 1rem 1.05rem;
  border: 1px solid rgba(18, 41, 74, 0.12);
  background: rgba(255, 255, 255, 0.92);
  box-shadow: 0 10px 24px rgba(23, 50, 84, 0.07);
  min-width: 0;
  width: 100%;
}

.message-card-user {
  background: linear-gradient(180deg, rgba(29, 95, 133, 0.13), rgba(29, 95, 133, 0.07));
}

.message-card-assistant {
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(250, 246, 240, 0.92)),
    rgba(255, 255, 255, 0.92);
}

.message-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.7rem;
}

.message-source-sheet {
  display: flex;
  flex-wrap: wrap;
  gap: 0.42rem;
  margin-bottom: 0.62rem;
}

.message-anchor-notice {
  margin-bottom: 0.62rem;
}

.source-pill {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.24rem 0.62rem;
  background: rgba(29, 95, 133, 0.1);
  color: rgba(23, 50, 84, 0.8);
  font-size: 0.72rem;
  letter-spacing: 0.04em;
}

.source-pill-emphasis {
  background: rgba(200, 92, 60, 0.14);
  color: #8e3e28;
}

.source-pill-subtle {
  background: rgba(23, 50, 84, 0.08);
  color: rgba(23, 50, 84, 0.72);
}

.message-task-steps {
  margin-bottom: 0.68rem;
}

.message-step-comparison {
  margin-bottom: 0.72rem;
}

.step-comparison-grid {
  display: grid;
  gap: 0.46rem;
}

.step-comparison-card {
  border-radius: 14px;
  border: 1px solid rgba(18, 41, 74, 0.1);
  background: rgba(255, 255, 255, 0.8);
  padding: 0.48rem 0.58rem;
}

.step-comparison-card-current {
  border-color: rgba(29, 95, 133, 0.24);
  background: rgba(29, 95, 133, 0.08);
}

.step-comparison-title {
  color: rgba(23, 50, 84, 0.74);
  font-size: 0.72rem;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  margin-bottom: 0.28rem;
}

.step-comparison-text {
  margin: 0;
  color: #173254;
  font-size: 0.82rem;
  line-height: 1.42;
}

.task-step-list {
  margin: 0;
  padding: 0;
  list-style: none;
  display: grid;
  gap: 0.42rem;
}

.task-step-actions {
  margin-top: 0.5rem;
}

.task-step-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
  border-radius: 14px;
  border: 1px solid rgba(18, 41, 74, 0.1);
  background: rgba(255, 255, 255, 0.78);
  padding: 0.42rem 0.56rem;
}

.task-step-item.task-step-active {
  border-color: rgba(29, 95, 133, 0.35);
  background: rgba(29, 95, 133, 0.1);
}

.task-step-item.task-step-completed {
  border-color: rgba(50, 122, 84, 0.24);
}

.task-step-item.task-step-failed {
  border-color: rgba(200, 92, 60, 0.28);
  background: rgba(200, 92, 60, 0.08);
}

.task-step-main {
  color: #173254;
  font-size: 0.82rem;
}

.task-step-badges {
  display: inline-flex;
  align-items: center;
  gap: 0.34rem;
}

.task-step-current,
.task-step-status {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.12rem 0.42rem;
  font-size: 0.7rem;
  letter-spacing: 0.04em;
}

.task-step-current {
  background: rgba(29, 95, 133, 0.16);
  color: #1d5f85;
}

.task-step-status {
  background: rgba(23, 50, 84, 0.08);
  color: rgba(23, 50, 84, 0.74);
}

.message-role,
.message-streaming {
  font-size: 0.76rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.72);
}

.message-streaming {
  color: #c85c3c;
}

.message-text {
  margin: 0;
  color: #173254;
  line-height: 1.7;
}

.answer-stack {
  display: grid;
  gap: 0.75rem;
}

.compare-panel {
  border-radius: 20px;
  padding: 0.95rem;
  border: 1px solid rgba(200, 92, 60, 0.18);
  background:
    linear-gradient(180deg, rgba(200, 92, 60, 0.1), rgba(248, 242, 233, 0.72)),
    rgba(255, 255, 255, 0.94);
}

.compare-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.8rem;
}

.compare-label {
  margin-bottom: 0;
}

.compare-badge {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.24rem 0.62rem;
  background: rgba(200, 92, 60, 0.14);
  color: #c85c3c;
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.04em;
}

.compare-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.72rem;
}

.compare-card {
  display: grid;
  gap: 0.22rem;
  border-radius: 16px;
  padding: 0.78rem 0.82rem;
  background: rgba(255, 255, 255, 0.78);
  border: 1px solid rgba(18, 41, 74, 0.08);
}

.compare-card-primary {
  background: rgba(200, 92, 60, 0.12);
  border-color: rgba(200, 92, 60, 0.2);
}

.compare-card-label {
  font-size: 0.72rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.54);
}

.compare-card strong {
  color: #173254;
  font-size: 1.08rem;
  line-height: 1.35;
}

.compare-card small {
  color: rgba(23, 50, 84, 0.68);
  font-size: 0.83rem;
  line-height: 1.5;
}

.forecast-panel {
  border-radius: 20px;
  padding: 0.95rem;
  border: 1px solid rgba(29, 95, 133, 0.16);
  background:
    linear-gradient(180deg, rgba(29, 95, 133, 0.08), rgba(248, 242, 233, 0.72)),
    rgba(255, 255, 255, 0.94);
}

.forecast-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
  margin-bottom: 0.8rem;
}

.forecast-label {
  margin-bottom: 0;
}

.forecast-badge {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 0.24rem 0.62rem;
  background: rgba(29, 95, 133, 0.14);
  color: #1d5f85;
  font-size: 0.74rem;
  font-weight: 700;
  letter-spacing: 0.04em;
}

.forecast-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 0.72rem;
}

.forecast-card {
  display: grid;
  gap: 0.22rem;
  border-radius: 16px;
  padding: 0.78rem 0.82rem;
  background: rgba(255, 255, 255, 0.78);
  border: 1px solid rgba(18, 41, 74, 0.08);
}

.forecast-card-primary {
  background: rgba(29, 95, 133, 0.12);
  border-color: rgba(29, 95, 133, 0.18);
}

.forecast-card-label {
  font-size: 0.72rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.54);
}

.forecast-card strong {
  color: #173254;
  font-size: 1.08rem;
  line-height: 1.35;
}

.forecast-card small,
.forecast-footnote {
  color: rgba(23, 50, 84, 0.68);
  font-size: 0.83rem;
  line-height: 1.5;
}

.forecast-footnote {
  margin-top: 0.72rem;
}

.answer-block {
  border-radius: 18px;
  padding: 0.8rem 0.9rem;
  background: rgba(18, 41, 74, 0.035);
  border: 1px solid rgba(18, 41, 74, 0.08);
}

.answer-block-conclusion {
  background: linear-gradient(180deg, rgba(29, 95, 133, 0.1), rgba(18, 41, 74, 0.02));
  border-color: rgba(29, 95, 133, 0.22);
}

.answer-block-conclusion .message-text {
  font-weight: 600;
}

.answer-block-secondary {
  background: rgba(29, 95, 133, 0.075);
  border-color: rgba(29, 95, 133, 0.16);
}

.answer-block-risk {
  background: rgba(200, 92, 60, 0.1);
  border: 1px solid rgba(200, 92, 60, 0.16);
}

.answer-label {
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.56);
  margin-bottom: 0.35rem;
}

.message-secondary {
  margin: 0;
  color: rgba(23, 50, 84, 0.7);
  line-height: 1.65;
}

.message-risk {
  margin: 0;
  color: #8e3e28;
  line-height: 1.6;
}

.message-scope,
.message-routing,
.message-analysis-summary,
.message-followup,
.message-meta,
.message-chart,
.message-chart-fallback,
.message-detail-table,
.pipeline-block {
  margin-top: 1rem;
  min-width: 0;
}

.message-actions,
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.75rem;
}

.message-actions {
  margin-top: 0.8rem;
}

.section-head {
  margin-bottom: 0.5rem;
}

.message-action {
  border: 1px solid rgba(29, 95, 133, 0.16);
  border-radius: 999px;
  padding: 0.42rem 0.74rem;
  background: rgba(255, 255, 255, 0.92);
  color: #1d5f85;
  font-size: 0.82rem;
  font-weight: 600;
  transition: transform 150ms ease, border-color 150ms ease, background 150ms ease;
}

.message-action:hover {
  transform: translateY(-1px);
  border-color: rgba(29, 95, 133, 0.26);
  background: rgba(255, 255, 255, 0.98);
}

.message-action-secondary {
  color: #173254;
}

.message-followup .section-label {
  margin-bottom: 0.45rem;
}

.followup-list {
  display: flex;
  flex-wrap: wrap;
  gap: 0.45rem;
}

.message-action-followup {
  border-radius: 14px;
  padding: 0.34rem 0.62rem;
  font-size: 0.77rem;
  font-weight: 500;
}

.message-action-next-step {
  border-radius: 14px;
  padding: 0.34rem 0.66rem;
  font-size: 0.77rem;
  font-weight: 600;
}

.scope-label,
.meta-label,
.section-label {
  font-size: 0.78rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.62);
  margin-bottom: 0.5rem;
}

.section-head .section-label {
  margin-bottom: 0;
}

.chart-context {
  margin-bottom: 0.55rem;
  display: grid;
  gap: 0.2rem;
}

.chart-context-title {
  font-size: 0.86rem;
  color: #173254;
  font-weight: 600;
  line-height: 1.4;
}

.chart-context-meta {
  font-size: 0.78rem;
  color: rgba(23, 50, 84, 0.68);
  line-height: 1.45;
}

.scope-body {
  display: grid;
  gap: 0.3rem;
  color: #173254;
}

.message-routing {
  border-radius: 18px;
  padding: 0.82rem 0.9rem;
  border: 1px solid rgba(29, 95, 133, 0.14);
  background: rgba(29, 95, 133, 0.06);
}

.routing-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.75rem;
}

.routing-item {
  display: grid;
  gap: 0.2rem;
}

.routing-item span,
.routing-note {
  font-size: 0.76rem;
  color: rgba(23, 50, 84, 0.62);
}

.routing-item strong {
  color: #173254;
  line-height: 1.45;
  overflow-wrap: anywhere;
}

.routing-note {
  margin-top: 0.55rem;
}

.message-analysis-summary {
  border-radius: 18px;
  padding: 0.78rem 0.88rem;
  border: 1px solid rgba(18, 41, 74, 0.1);
  background: rgba(248, 242, 233, 0.52);
}

.summary-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 0.7rem;
}

.summary-item {
  display: grid;
  gap: 0.18rem;
}

.summary-item span {
  font-size: 0.74rem;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.6);
}

.summary-item strong {
  color: #173254;
  font-size: 0.9rem;
  line-height: 1.45;
  overflow-wrap: anywhere;
}

.message-meta {
  border: 1px solid rgba(18, 41, 74, 0.08);
  border-radius: 16px;
  padding: 0.7rem 0.8rem 0.8rem;
  background: rgba(248, 242, 233, 0.56);
}

.message-meta summary {
  cursor: pointer;
  list-style: none;
  font-size: 0.78rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.62);
  font-weight: 700;
}

.message-meta summary::-webkit-details-marker {
  display: none;
}

.message-meta[open] summary {
  margin-bottom: 0.7rem;
}

.meta-grid {
  display: grid;
  grid-template-columns: minmax(110px, 180px) 1fr;
  gap: 0.5rem 0.85rem;
  margin: 0;
}

.meta-grid dt {
  color: rgba(23, 50, 84, 0.64);
}

.meta-grid dd {
  margin: 0;
  color: #173254;
  overflow-wrap: anywhere;
}

.message-pipeline {
  margin-top: 1rem;
  border-top: 1px solid rgba(18, 41, 74, 0.08);
  padding-top: 0.9rem;
}

.message-pipeline summary {
  cursor: pointer;
  color: #173254;
  font-weight: 700;
}

.pipeline-block pre {
  margin: 0;
  overflow: auto;
  padding: 0.9rem 1rem;
  border-radius: 16px;
  background: #112138;
  color: #f6efe6;
  font-size: 0.84rem;
  line-height: 1.55;
}

@media (max-width: 720px) {
  .compare-grid,
  .forecast-grid {
    grid-template-columns: 1fr;
  }

  .summary-grid {
    grid-template-columns: 1fr;
  }

  .routing-grid {
    grid-template-columns: 1fr;
  }
}
</style>
