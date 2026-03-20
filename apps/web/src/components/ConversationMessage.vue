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

const detailTableLabel = computed(() => {
  if (forecastMeta.value) {
    return props.ui.forecastTableLabel;
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

  return {
    requestedLabel: formatSheetDescriptor(requestedSheetName, requestedSheetIndex),
    resolvedLabel: formatSheetDescriptor(resolvedSheetName, resolvedSheetIndex),
    methodLabel,
    changed,
  };
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

    <template v-if="message.role === 'assistant' && hasStructuredAnswer">
      <div class="answer-stack">
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

        <div class="answer-block">
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
      </div>
      <div v-if="sheetRoutingMeta.changed" class="routing-note">{{ ui.routingChangedLabel }}</div>
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
      <SimpleChart
        :spec="message.chartSpec"
        :data="message.chartData || []"
        :no-data-text="ui.noChartData"
      />
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
}

.answer-block-secondary {
  background: rgba(29, 95, 133, 0.075);
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
.message-meta,
.message-chart,
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
  .forecast-grid {
    grid-template-columns: 1fr;
  }

  .routing-grid {
    grid-template-columns: 1fr;
  }
}
</style>
