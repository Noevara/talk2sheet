import { ref, type ComputedRef } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import { streamSpreadsheetChat } from "../../../lib/api";
import type { SpreadsheetChatRequest } from "../../../types";

function extractError(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) {
    return `${fallback}: ${error.message}`;
  }
  return fallback;
}

export function useSseChat(options: { ui: ComputedRef<UiMessages> }) {
  const chatBusy = ref(false);
  const errorMessage = ref("");
  const activeController = ref<AbortController | null>(null);

  function stopStreaming(): void {
    if (activeController.value) {
      activeController.value.abort();
    }
  }

  async function runChatStream(
    request: SpreadsheetChatRequest,
    handlers: {
      onMessage: (payload: Record<string, unknown>) => void;
    },
  ): Promise<{ aborted: boolean; errorMessage: string | null }> {
    const controller = new AbortController();
    activeController.value = controller;
    chatBusy.value = true;
    errorMessage.value = "";

    try {
      await streamSpreadsheetChat(request, {
        signal: controller.signal,
        onMessage: handlers.onMessage,
      });
      return { aborted: false, errorMessage: null };
    } catch (error) {
      const message = controller.signal.aborted
        ? options.ui.value.aborted
        : extractError(error, options.ui.value.chatError);
      errorMessage.value = message;
      return {
        aborted: controller.signal.aborted,
        errorMessage: message,
      };
    } finally {
      activeController.value = null;
      chatBusy.value = false;
    }
  }

  return {
    chatBusy,
    errorMessage,
    stopStreaming,
    runChatStream,
  };
}
