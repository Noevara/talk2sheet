import type {
  AnswerSegments,
  ChatMessage,
  ChatMode,
  ClarificationOption,
  ClarificationPayload,
  ExecutionDisclosure,
} from "../types";

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

export function normalizeDisclosure(payload: Record<string, unknown>): ExecutionDisclosure | null {
  const raw = (payload.execution_disclosure || payload.executionDisclosure || payload) as Record<string, unknown>;
  if (!raw) {
    return null;
  }

  const scopeText = typeof raw.scope_text === "string" ? raw.scope_text : typeof raw.scopeText === "string" ? raw.scopeText : "";
  const scopeWarning =
    typeof raw.scope_warning === "string" ? raw.scope_warning : typeof raw.scopeWarning === "string" ? raw.scopeWarning : "";
  const fallbackReason =
    typeof raw.fallback_reason === "string"
      ? raw.fallback_reason
      : typeof raw.fallbackReason === "string"
        ? raw.fallbackReason
        : "";
  const dataScope =
    raw.data_scope === "sampled_first_n" || raw.dataScope === "sampled_first_n" ? "sampled_first_n" : "exact_full_table";

  if (!scopeText && !scopeWarning && !fallbackReason) {
    return null;
  }

  return {
    data_scope: dataScope,
    exact_used: Boolean(raw.exact_used ?? raw.exactUsed ?? dataScope === "exact_full_table"),
    scope_text: scopeText,
    scope_warning: scopeWarning,
    fallback_reason: fallbackReason,
    fallback_reason_code:
      typeof raw.fallback_reason_code === "string"
        ? raw.fallback_reason_code
        : typeof raw.fallbackReasonCode === "string"
          ? raw.fallbackReasonCode
          : "",
    max_rows: typeof raw.max_rows === "number" ? raw.max_rows : typeof raw.maxRows === "number" ? raw.maxRows : null,
  };
}

export function extractAnswerSegments(payload: Record<string, unknown>): AnswerSegments | null {
  const direct = payload.answer_segments;
  const nested =
    payload.pipeline && typeof payload.pipeline === "object"
      ? ((payload.pipeline as Record<string, unknown>).answer_generation as Record<string, unknown> | undefined)?.segments
      : undefined;
  const raw = (direct && typeof direct === "object" ? direct : nested && typeof nested === "object" ? nested : null) as
    | Record<string, unknown>
    | null;

  if (!raw) {
    return null;
  }

  const conclusion = typeof raw.conclusion === "string" ? raw.conclusion.trim() : "";
  const evidence = typeof raw.evidence === "string" ? raw.evidence.trim() : "";
  const riskNote =
    typeof raw.risk_note === "string"
      ? raw.risk_note.trim()
      : typeof raw.riskNote === "string"
        ? raw.riskNote.trim()
        : "";

  if (!conclusion && !evidence && !riskNote) {
    return null;
  }

  return {
    conclusion: conclusion || undefined,
    evidence: evidence || undefined,
    riskNote: riskNote || undefined,
  };
}

function extractClarificationOption(value: unknown): ClarificationOption | null {
  const raw = asRecord(value);
  if (!raw) {
    return null;
  }
  const label = typeof raw.label === "string" ? raw.label.trim() : "";
  const optionValue = typeof raw.value === "string" ? raw.value.trim() : label;
  const description = typeof raw.description === "string" ? raw.description.trim() : "";
  if (!label || !optionValue) {
    return null;
  }
  return {
    label,
    value: optionValue,
    description: description || undefined,
  };
}

export function extractClarification(payload: Record<string, unknown> | null | undefined): ClarificationPayload | null {
  const raw = asRecord(payload?.clarification);
  if (!raw) {
    return null;
  }
  const kind =
    raw.kind === "sheet_resolution" ||
    payload?.clarification_stage === "sheet_routing" ||
    raw.field === "sheet"
      ? "sheet_resolution"
      : "column_resolution";
  const reason = typeof raw.reason === "string" ? raw.reason.trim() : "";
  const field = typeof raw.field === "string" ? raw.field.trim() : "";
  const options = Array.isArray(raw.options)
    ? raw.options
        .map((item) => extractClarificationOption(item))
        .filter((item): item is ClarificationOption => Boolean(item))
    : [];
  if (!reason || !options.length) {
    return null;
  }
  return { kind, reason, field: field || undefined, options };
}

export function applyStreamPayload(message: ChatMessage, payload: Record<string, unknown>): void {
  if (payload.meta && typeof payload.meta === "object") {
    message.meta = payload.meta as Record<string, unknown>;
  }

  if (payload.pipeline && typeof payload.pipeline === "object") {
    const pipeline = payload.pipeline as Record<string, unknown>;
    message.pipeline = pipeline;
    message.executionDisclosure = normalizeDisclosure(payload);
    message.clarification = extractClarification(pipeline);
    const resultColumns = pipeline.result_columns;
    if (Array.isArray(resultColumns)) {
      message.tableColumns = resultColumns.filter((item): item is string => typeof item === "string");
    }
  }

  const answerSegments = extractAnswerSegments(payload);
  if (answerSegments) {
    message.answerSegments = answerSegments;
  }

  if (typeof payload.answer === "string" && payload.answer !== "<|EOS|>") {
    message.text = payload.answer;
    message.mode = (payload.mode as ChatMode | undefined) || "text";
    message.analysisText = typeof payload.analysis_text === "string" ? payload.analysis_text : undefined;
    message.executionDisclosure = normalizeDisclosure(payload) || message.executionDisclosure || null;
    message.chartSpec = payload.chart_spec && typeof payload.chart_spec === "object" ? (payload.chart_spec as ChatMessage["chartSpec"]) : null;
    message.chartData = Array.isArray(payload.chart_data) ? (payload.chart_data as Record<string, unknown>[]) : null;
  }
}
