import { ref, type ComputedRef } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import { formatChatError } from "../../../lib/errorMessages";
import { streamSpreadsheetChat, streamWorkbookBatchAnalysis } from "../../../lib/api";
import type { SpreadsheetBatchRequest, SpreadsheetChatRequest } from "../../../types";

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
    return runManagedStream(
      (payload, options) => streamSpreadsheetChat(payload, options),
      request,
      handlers,
    );
  }

  async function runBatchStream(
    request: SpreadsheetBatchRequest,
    handlers: {
      onMessage: (payload: Record<string, unknown>) => void;
    },
  ): Promise<{ aborted: boolean; errorMessage: string | null }> {
    return runManagedStream(
      (payload, options) => streamWorkbookBatchAnalysis(payload, options),
      request,
      handlers,
    );
  }

  async function runManagedStream<TRequest>(
    streamRunner: (
      request: TRequest,
      options: {
        signal?: AbortSignal;
        onMessage: (payload: Record<string, unknown>) => void;
      },
    ) => Promise<void>,
    request: TRequest,
    handlers: {
      onMessage: (payload: Record<string, unknown>) => void;
    },
  ): Promise<{ aborted: boolean; errorMessage: string | null }> {
    const controller = new AbortController();
    activeController.value = controller;
    chatBusy.value = true;
    errorMessage.value = "";

    try {
      await streamRunner(request, {
        signal: controller.signal,
        onMessage: handlers.onMessage,
      });
      return { aborted: false, errorMessage: null };
    } catch (error) {
      const message = controller.signal.aborted
        ? options.ui.value.aborted
        : formatChatError(error, options.ui.value);
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
    runBatchStream,
  };
}
