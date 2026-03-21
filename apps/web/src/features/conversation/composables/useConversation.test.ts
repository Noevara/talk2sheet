import { computed, ref } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { messages } from "../../../i18n/messages";
import type { Locale } from "../../../types";
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
});
