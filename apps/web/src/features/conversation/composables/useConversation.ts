import { ref, shallowRef, type ComputedRef, type Ref } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import { applyStreamPayload } from "../../../lib/chatPayload";
import type {
  SpreadsheetBatchResponse,
  ChatMessage,
  ChatMode,
  ClarificationResolution,
  Locale,
  PreviewResponse,
  UploadedFileResponse,
} from "../../../types";
import { useSseChat } from "./useSseChat";

function createMessageId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function buildClarificationDisplayText(ui: UiMessages, selectedValue: string): string {
  const template = ui.clarificationSelectedMessageTemplate.trim();
  if (template.includes("{value}")) {
    return template.replace("{value}", selectedValue);
  }
  if (template) {
    return `${template} ${selectedValue}`;
  }
  return `${ui.clarificationSelectedLabel}: ${selectedValue}`;
}

function readRequestText(message: ChatMessage): string {
  const requestText = message.meta?.requestText;
  return typeof requestText === "string" && requestText.trim() ? requestText.trim() : message.text;
}

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

export function useConversation(options: {
  locale: Ref<Locale>;
  ui: ComputedRef<UiMessages>;
}) {
  const question = ref("");
  const composerFocusToken = ref(0);
  const chatMessages = ref<ChatMessage[]>([]);
  const conversationId = ref<string | null>(null);
  const chatMode = ref<ChatMode>("auto");

  const workbookContext = shallowRef<{
    workbook: Ref<UploadedFileResponse | null>;
    preview: Ref<PreviewResponse | null>;
    selectedSheetIndex: Ref<number>;
    pendingSheetOverride: Ref<boolean>;
    clearPendingSheetOverride: () => void;
  } | null>(null);

  const sseChat = useSseChat({ ui: options.ui });

  function bindWorkbookContext(context: {
    workbook: Ref<UploadedFileResponse | null>;
    preview: Ref<PreviewResponse | null>;
    selectedSheetIndex: Ref<number>;
    pendingSheetOverride: Ref<boolean>;
    clearPendingSheetOverride: () => void;
  }): void {
    workbookContext.value = context;
  }

  function stopStreaming(): void {
    sseChat.stopStreaming();
  }

  function applySuggestedFollowup(questionText: string): void {
    const normalized = String(questionText || "").trim();
    if (!normalized) {
      return;
    }
    question.value = normalized;
    composerFocusToken.value += 1;
    sseChat.errorMessage.value = "";
  }

  function resetConversation(): void {
    stopStreaming();
    question.value = "";
    chatMessages.value = [];
    conversationId.value = null;
    sseChat.errorMessage.value = "";
  }

  function restoreState(snapshot: ConversationSnapshot): void {
    stopStreaming();
    question.value = snapshot.question;
    chatMessages.value = snapshot.chatMessages;
    conversationId.value = snapshot.conversationId;
    chatMode.value = snapshot.chatMode;
    sseChat.errorMessage.value = "";
  }

  function snapshotState(): ConversationSnapshot {
    return {
      question: question.value,
      chatMessages: chatMessages.value,
      conversationId: conversationId.value,
      chatMode: chatMode.value,
    };
  }

  function findPreviousUserQuestion(messageId: string): string {
    const messageIndex = chatMessages.value.findIndex((message) => message.id === messageId);
    if (messageIndex <= 0) {
      return "";
    }
    for (let index = messageIndex - 1; index >= 0; index -= 1) {
      const message = chatMessages.value[index];
      if (message.role === "user") {
        return readRequestText(message);
      }
    }
    return "";
  }

  async function submitQuestion(input?: {
    requestQuestion?: string;
    displayQuestion?: string;
    clarificationResolution?: ClarificationResolution | null;
    followupAction?: "continue_next_step" | null;
  }): Promise<void> {
    if (!workbookContext.value?.workbook.value) {
      sseChat.errorMessage.value = options.ui.value.missingFile;
      return;
    }

    const normalizedInput = input || {};
    const pendingQuestion =
      typeof normalizedInput.requestQuestion === "string" ? normalizedInput.requestQuestion.trim() : question.value.trim();
    if (!pendingQuestion) {
      sseChat.errorMessage.value = options.ui.value.missingQuestion;
      return;
    }
    if (sseChat.chatBusy.value) {
      return;
    }

    const userText = pendingQuestion;
    const displayText =
      typeof normalizedInput.displayQuestion === "string" && normalizedInput.displayQuestion.trim()
        ? normalizedInput.displayQuestion.trim()
        : userText;
    sseChat.errorMessage.value = "";
    question.value = "";

    const userMessage: ChatMessage = {
      id: createMessageId(),
      role: "user",
      text: displayText,
      status: "done",
      meta: {
        requestText: userText,
      },
    };

    const assistantMessage: ChatMessage = {
      id: createMessageId(),
      role: "assistant",
      text: options.ui.value.thinking,
      status: "streaming",
      tableColumns: workbookContext.value.preview.value?.columns || [],
      clarification: null,
    };

    chatMessages.value = [...chatMessages.value, userMessage, assistantMessage];
    const streamMessage = chatMessages.value[chatMessages.value.length - 1];

    const streamResult = await sseChat.runChatStream(
      {
        file_id: workbookContext.value.workbook.value.file_id,
        chat_text: userText,
        mode: chatMode.value,
        sheet_index: workbookContext.value.selectedSheetIndex.value,
        sheet_override: workbookContext.value.pendingSheetOverride.value,
        locale: options.locale.value,
        conversation_id: conversationId.value,
        clarification_resolution: normalizedInput.clarificationResolution || null,
        followup_action: normalizedInput.followupAction || null,
      },
      {
        onMessage: (payload) => {
          if (typeof payload.conversation_id === "string" && payload.conversation_id) {
            conversationId.value = payload.conversation_id;
          }
          if (payload.answer === "<|EOS|>") {
            return;
          }
          applyStreamPayload(streamMessage, payload);
        },
      },
    );

    if (streamResult.errorMessage) {
      streamMessage.text = streamResult.errorMessage;
    } else {
      workbookContext.value.clearPendingSheetOverride();
    }
    streamMessage.status = "done";
  }

  async function handleClarificationSelect(payload: { messageId: string; value: string }): Promise<void> {
    const sourceMessage = chatMessages.value.find((message) => message.id === payload.messageId);
    const sourceQuestion = findPreviousUserQuestion(payload.messageId);
    if (!sourceQuestion) {
      sseChat.errorMessage.value = options.ui.value.clarificationExpiredError;
      return;
    }
    const clarificationResolution: ClarificationResolution = {
      kind: sourceMessage?.clarification?.kind || "column_resolution",
      source_field: sourceMessage?.clarification?.field || null,
      selected_value: payload.value,
    };
    await submitQuestion({
      requestQuestion: sourceQuestion,
      displayQuestion: buildClarificationDisplayText(options.ui.value, payload.value),
      clarificationResolution,
    });
  }

  async function handleContinueNextStep(payload: { messageId: string }): Promise<void> {
    const sourceQuestion = findPreviousUserQuestion(payload.messageId);
    if (!sourceQuestion) {
      sseChat.errorMessage.value = options.ui.value.clarificationExpiredError;
      return;
    }
    await submitQuestion({
      requestQuestion: sourceQuestion,
      displayQuestion: options.ui.value.followupContinueNextStepSubmittedLabel,
      followupAction: "continue_next_step",
    });
  }

  function buildBatchResultMessage(payload: SpreadsheetBatchResponse, questionText: string): ChatMessage {
    const summary = payload.summary || { total: 0, succeeded: 0, failed: 0 };
    const text = `${options.ui.value.batchSummaryTotalsLabel}: ${summary.succeeded}/${summary.total}`;
    return {
      id: createMessageId(),
      role: "assistant",
      text,
      status: "done",
      pipeline: {
        batch_question: questionText,
        batch_results: Array.isArray(payload.batch_results) ? payload.batch_results : [],
        batch_summary: summary,
      },
      meta: {
        request_id: payload.request_id,
      },
    };
  }

  function buildBatchResponseFromPayload(
    payload: Record<string, unknown>,
    fallback: {
      fileId: string;
      question: string;
      mode: ChatMode;
      sheetIndexes: number[];
    },
  ): SpreadsheetBatchResponse {
    const rawSummary = asRecord(payload.summary);
    const rawResults = Array.isArray(payload.batch_results) ? payload.batch_results : [];
    const total = readNumber(rawSummary?.total) ?? rawResults.length;
    const succeeded = readNumber(rawSummary?.succeeded) ?? 0;
    const failed = readNumber(rawSummary?.failed) ?? Math.max(0, total - succeeded);
    const sheetIndexes = Array.isArray(payload.sheet_indexes)
      ? payload.sheet_indexes.map((item) => Number(item || 0)).filter((item) => item > 0)
      : fallback.sheetIndexes;
    return {
      request_id: readString(payload.request_id) || createMessageId(),
      file_id: readString(payload.file_id) || fallback.fileId,
      question: readString(payload.question) || fallback.question,
      mode: readString(payload.mode) || fallback.mode,
      sheet_indexes: sheetIndexes,
      batch_results: rawResults as SpreadsheetBatchResponse["batch_results"],
      summary: {
        total,
        succeeded,
        failed,
      },
    };
  }

  function applyBatchProgressPayload(
    message: ChatMessage,
    payload: Record<string, unknown>,
    fallbackTotal: number,
  ): void {
    const rawCurrentSheet = asRecord(payload.current_sheet);
    const done = Math.max(0, readNumber(payload.done) ?? 0);
    const total = Math.max(done, readNumber(payload.total) ?? fallbackTotal);
    const currentSheetIndex = readNumber(rawCurrentSheet?.sheet_index) ?? readNumber(payload.current_sheet_index);
    const currentSheetName = readString(rawCurrentSheet?.sheet_name) || readString(payload.current_sheet_name);
    const status = readString(payload.status) || "running";
    const sheetStatus = readString(payload.sheet_status);
    message.pipeline = {
      ...(message.pipeline || {}),
      batch_progress: {
        done,
        total,
        status,
        current_sheet_index: currentSheetIndex,
        current_sheet_name: currentSheetName,
        sheet_status: sheetStatus,
      },
    };
    message.text = `${options.ui.value.batchRunBusyLabel} ${done}/${total}`;
  }

  async function handleBatchAnalysis(sheetIndexes: number[]): Promise<void> {
    const workbook = workbookContext.value?.workbook.value;
    if (!workbook) {
      sseChat.errorMessage.value = options.ui.value.missingFile;
      return;
    }
    const normalizedQuestion = question.value.trim();
    if (!normalizedQuestion) {
      sseChat.errorMessage.value = options.ui.value.missingQuestion;
      return;
    }
    if (sseChat.chatBusy.value) {
      return;
    }
    const normalizedSheetIndexes = Array.from(
      new Set(sheetIndexes.map((item) => Number(item || 0)).filter((item) => item > 0)),
    );
    if (!normalizedSheetIndexes.length) {
      sseChat.errorMessage.value = options.ui.value.batchMissingSheetsError;
      return;
    }

    sseChat.errorMessage.value = "";

    const userMessage: ChatMessage = {
      id: createMessageId(),
      role: "user",
      text: `${normalizedQuestion} (${options.ui.value.batchRunLabel})`,
      status: "done",
      meta: {
        requestText: normalizedQuestion,
      },
    };
    const assistantMessage: ChatMessage = {
      id: createMessageId(),
      role: "assistant",
      text: `${options.ui.value.batchRunBusyLabel} 0/${normalizedSheetIndexes.length}`,
      status: "streaming",
      pipeline: {
        batch_progress: {
          done: 0,
          total: normalizedSheetIndexes.length,
          status: "running",
          current_sheet_index: null,
          current_sheet_name: "",
          sheet_status: "",
        },
      },
    };
    chatMessages.value = [...chatMessages.value, userMessage, assistantMessage];
    const streamMessage = chatMessages.value[chatMessages.value.length - 1];
    let finalBatchReceived = false;

    const streamResult = await sseChat.runBatchStream(
      {
        file_id: workbook.file_id,
        question: normalizedQuestion,
        mode: chatMode.value,
        sheet_indexes: normalizedSheetIndexes,
        locale: options.locale.value,
      },
      {
        onMessage: (rawPayload) => {
          const payload = asRecord(rawPayload);
          if (!payload) {
            return;
          }
          const eventType = readString(payload.type);
          if (eventType === "batch_progress") {
            applyBatchProgressPayload(streamMessage, payload, normalizedSheetIndexes.length);
            return;
          }
          if (eventType === "batch_result") {
            const batchResponse = buildBatchResponseFromPayload(payload, {
              fileId: workbook.file_id,
              question: normalizedQuestion,
              mode: chatMode.value,
              sheetIndexes: normalizedSheetIndexes,
            });
            const batchMessage = buildBatchResultMessage(batchResponse, normalizedQuestion);
            streamMessage.text = batchMessage.text;
            streamMessage.status = "done";
            streamMessage.pipeline = batchMessage.pipeline;
            streamMessage.meta = batchMessage.meta;
            finalBatchReceived = true;
            question.value = "";
            return;
          }
          if (eventType === "batch_error") {
            const errorMessage = readString(payload.error) || options.ui.value.chatInterruptedError;
            sseChat.errorMessage.value = errorMessage;
            streamMessage.text = errorMessage;
            streamMessage.status = "done";
            return;
          }
        },
      },
    );

    if (streamResult.errorMessage && !finalBatchReceived) {
      streamMessage.text = streamResult.errorMessage;
      streamMessage.status = "done";
    } else if (!finalBatchReceived && streamMessage.status === "streaming") {
      streamMessage.status = "done";
    }
  }

  return {
    question,
    composerFocusToken,
    chatMessages,
    chatBusy: sseChat.chatBusy,
    chatMode,
    errorMessage: sseChat.errorMessage,
    bindWorkbookContext,
    restoreState,
    snapshotState,
    stopStreaming,
    resetConversation,
    applySuggestedFollowup,
    submitQuestion,
    handleClarificationSelect,
    handleContinueNextStep,
    handleBatchAnalysis,
  };
}

export type ConversationState = ReturnType<typeof useConversation>;
export interface ConversationSnapshot {
  question: string;
  chatMessages: ChatMessage[];
  conversationId: string | null;
  chatMode: ChatMode;
}
