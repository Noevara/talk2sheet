import { computed, ref } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { messages } from "../../../i18n/messages";
import type { Locale, UploadedFileResponse } from "../../../types";
import { useConversation } from "./useConversation";

const sseState = vi.hoisted(() => ({
  chatBusy: { value: false },
  errorMessage: { value: "" },
  runChatStream: vi.fn(async () => ({ errorMessage: "", aborted: false })),
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
});
