import { computed, ref } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { messages } from "../../../i18n/messages";
import type { Locale, UploadedFileResponse } from "../../../types";
import { useConversation } from "./useConversation";

const sseState = vi.hoisted(() => ({
  chatBusy: { value: false },
  errorMessage: { value: "" },
  runChatStream: vi.fn(async (...args: unknown[]) => {
    void args;
    return { errorMessage: "", aborted: false };
  }),
  runBatchStream: vi.fn(async (...args: unknown[]) => {
    void args;
    return { errorMessage: "", aborted: false };
  }),
  stopStreaming: vi.fn(),
}));

vi.mock("./useSseChat", () => ({
  useSseChat: () => sseState,
}));

describe("useConversation", () => {
  beforeEach(() => {
    sseState.chatBusy.value = false;
    sseState.errorMessage.value = "";
    sseState.runChatStream.mockClear();
    sseState.runBatchStream.mockClear();
    sseState.stopStreaming.mockClear();
  });

  it("prefills question draft from follow-up suggestion and bumps composer focus token", () => {
    const state = useConversation({
      locale: ref<Locale>("en"),
      ui: computed(() => messages.en),
    });

    state.errorMessage.value = "temporary error";
    state.applySuggestedFollowup("  Show the monthly trend for this metric.  ");

    expect(state.question.value).toBe("Show the monthly trend for this metric.");
    expect(state.composerFocusToken.value).toBe(1);
    expect(state.errorMessage.value).toBe("");

    state.applySuggestedFollowup("   ");
    expect(state.composerFocusToken.value).toBe(1);
  });

  it("sends followup_action=continue_next_step when continue-next-step is triggered", async () => {
    const state = useConversation({
      locale: ref<Locale>("en"),
      ui: computed(() => messages.en),
    });
    const workbook = ref<UploadedFileResponse | null>({
      file_id: "file-1",
      file_name: "demo.xlsx",
      file_type: "xlsx",
      sheets: [],
    });
    state.bindWorkbookContext({
      workbook,
      preview: ref(null),
      selectedSheetIndex: ref(1),
      pendingSheetOverride: ref(false),
      clearPendingSheetOverride: vi.fn(),
    });
    state.chatMessages.value = [
      {
        id: "user-1",
        role: "user",
        text: "Join Sales and Users by email and show conversion.",
        status: "done",
        meta: {
          requestText: "Join Sales and Users by email and show conversion.",
        },
      },
      {
        id: "assistant-1",
        role: "assistant",
        text: "Please choose one sheet first.",
        status: "done",
      },
    ];

    await state.handleContinueNextStep({ messageId: "assistant-1" });

    expect(sseState.runChatStream).toHaveBeenCalledTimes(1);
    const firstCall = sseState.runChatStream.mock.calls[0] as unknown[] | undefined;
    expect(firstCall?.[0]).toMatchObject({
      file_id: "file-1",
      chat_text: "Join Sales and Users by email and show conversion.",
      followup_action: "continue_next_step",
      clarification_resolution: null,
    });
    const latestUserMessage = state.chatMessages.value[state.chatMessages.value.length - 2];
    expect(latestUserMessage.role).toBe("user");
    expect(latestUserMessage.text).toBe(messages.en.followupContinueNextStepSubmittedLabel);
  });

  it("runs workbook batch analysis and stores batch result in assistant message", async () => {
    sseState.runBatchStream.mockImplementationOnce(async (...args: unknown[]) => {
      const handlers = (args[1] || {}) as { onMessage?: (payload: Record<string, unknown>) => void };
      const onMessage = handlers.onMessage || (() => {});
      onMessage({
        type: "batch_progress",
        done: 1,
        total: 2,
        status: "running",
        current_sheet: {
          sheet_index: 1,
          sheet_name: "Sales",
        },
      });
      onMessage({
        type: "batch_result",
        request_id: "req-batch-1",
        file_id: "file-1",
        question: "How many rows?",
        mode: "text",
        sheet_indexes: [1, 2],
        batch_results: [
          {
            sheet_index: 1,
            sheet_name: "Sales",
            status: "success",
            mode: "text",
            answer: "Sales rows: 120",
            result_row_count: 1,
            pipeline: { intent: "row_count" },
          },
          {
            sheet_index: 2,
            sheet_name: "Users",
            status: "failed",
            mode: "text",
            error: "Internal error",
            reason_code: "analysis_exception",
          },
        ],
        summary: {
          total: 2,
          succeeded: 1,
          failed: 1,
        },
      });
      onMessage({
        type: "batch_done",
      });
      return { errorMessage: "", aborted: false };
    });

    const state = useConversation({
      locale: ref<Locale>("en"),
      ui: computed(() => messages.en),
    });
    const workbook = ref<UploadedFileResponse | null>({
      file_id: "file-1",
      file_name: "demo.xlsx",
      file_type: "xlsx",
      sheets: [
        { index: 1, name: "Sales" },
        { index: 2, name: "Users" },
      ],
    });
    state.bindWorkbookContext({
      workbook,
      preview: ref(null),
      selectedSheetIndex: ref(1),
      pendingSheetOverride: ref(false),
      clearPendingSheetOverride: vi.fn(),
    });
    state.question.value = "How many rows?";

    await state.handleBatchAnalysis([1, 2]);

    expect(sseState.runBatchStream).toHaveBeenCalledWith({
      file_id: "file-1",
      question: "How many rows?",
      mode: "auto",
      sheet_indexes: [1, 2],
      locale: "en",
    }, expect.any(Object));
    expect(state.chatMessages.value).toHaveLength(2);
    expect(state.chatMessages.value[0]).toMatchObject({
      role: "user",
      text: "How many rows? (Run batch)",
      status: "done",
    });
    expect(state.chatMessages.value[1]).toMatchObject({
      role: "assistant",
      status: "done",
      text: "Batch completed: 1/2",
      meta: {
        request_id: "req-batch-1",
      },
    });
    expect((state.chatMessages.value[1].pipeline?.batch_results as unknown[] | undefined)?.length).toBe(2);
    expect(state.question.value).toBe("");
    expect(state.chatBusy.value).toBe(false);
  });

  it("blocks batch analysis when no sheet is selected", async () => {
    const state = useConversation({
      locale: ref<Locale>("en"),
      ui: computed(() => messages.en),
    });
    const workbook = ref<UploadedFileResponse | null>({
      file_id: "file-1",
      file_name: "demo.xlsx",
      file_type: "xlsx",
      sheets: [
        { index: 1, name: "Sales" },
        { index: 2, name: "Users" },
      ],
    });
    state.bindWorkbookContext({
      workbook,
      preview: ref(null),
      selectedSheetIndex: ref(1),
      pendingSheetOverride: ref(false),
      clearPendingSheetOverride: vi.fn(),
    });
    state.question.value = "How many rows?";

    await state.handleBatchAnalysis([]);

    expect(sseState.runBatchStream).not.toHaveBeenCalled();
    expect(state.errorMessage.value).toBe(messages.en.batchMissingSheetsError);
    expect(state.chatMessages.value).toHaveLength(0);
  });
});
