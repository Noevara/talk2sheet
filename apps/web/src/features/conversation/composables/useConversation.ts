import { ref, shallowRef, type ComputedRef, type Ref } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import { applyStreamPayload } from "../../../lib/chatPayload";
import type {
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
  return `${ui.clarificationSelectedLabel}: ${selectedValue}`;
}

function readRequestText(message: ChatMessage): string {
  const requestText = message.meta?.requestText;
  return typeof requestText === "string" && requestText.trim() ? requestText.trim() : message.text;
}

export function useConversation(options: {
  locale: Ref<Locale>;
  ui: ComputedRef<UiMessages>;
}) {
  const question = ref("");
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

  function resetConversation(): void {
    stopStreaming();
    question.value = "";
    chatMessages.value = [];
    conversationId.value = null;
    sseChat.errorMessage.value = "";
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
      sseChat.errorMessage.value = options.ui.value.missingQuestion;
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

  return {
    question,
    chatMessages,
    chatBusy: sseChat.chatBusy,
    chatMode,
    errorMessage: sseChat.errorMessage,
    bindWorkbookContext,
    stopStreaming,
    resetConversation,
    submitQuestion,
    handleClarificationSelect,
  };
}

export type ConversationState = ReturnType<typeof useConversation>;
