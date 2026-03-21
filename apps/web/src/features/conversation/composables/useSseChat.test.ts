import { computed } from "vue";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "../../../lib/api";
import { messages } from "../../../i18n/messages";
import { useSseChat } from "./useSseChat";

vi.mock("../../../lib/api", () => ({
  streamSpreadsheetChat: vi.fn(),
  streamWorkbookBatchAnalysis: vi.fn(),
  ApiError: class ApiError extends Error {
    status: number;
    requestId: string | null;
    detail: string;

    constructor(status: number, detail: string, requestId: string | null = null) {
      super(requestId ? `${detail} [request_id=${requestId}]` : detail);
      this.name = "ApiError";
      this.status = status;
      this.requestId = requestId;
      this.detail = detail;
    }
  },
}));

const apiModule = await import("../../../lib/api");

describe("useSseChat", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("maps network failures to a connection error", async () => {
    vi.mocked(apiModule.streamSpreadsheetChat).mockRejectedValueOnce(new TypeError("Failed to fetch"));

    const state = useSseChat({
      ui: computed(() => messages.en),
    });

    const result = await state.runChatStream(
      {
        file_id: "file-1",
        chat_text: "show total",
        mode: "text",
        sheet_index: 1,
        locale: "en",
      },
      {
        onMessage: vi.fn(),
      },
    );

    expect(result.errorMessage).toBe(messages.en.chatConnectionError);
    expect(state.errorMessage.value).toBe(messages.en.chatConnectionError);
  });

  it("returns the aborted message when the stream is cancelled", async () => {
    vi.mocked(apiModule.streamSpreadsheetChat).mockImplementationOnce(async (_request, options) => {
      options.signal?.throwIfAborted();
      return new Promise<void>((_resolve, reject) => {
        options.signal?.addEventListener("abort", () => {
          reject(new DOMException("Aborted", "AbortError"));
        });
      });
    });

    const state = useSseChat({
      ui: computed(() => messages.en),
    });

    const streamPromise = state.runChatStream(
      {
        file_id: "file-1",
        chat_text: "show total",
        mode: "text",
        sheet_index: 1,
        locale: "en",
      },
      {
        onMessage: vi.fn(),
      },
    );

    state.stopStreaming();

    const result = await streamPromise;

    expect(result.aborted).toBe(true);
    expect(result.errorMessage).toBe(messages.en.aborted);
  });

  it("keeps request ids on API chat failures", async () => {
    vi.mocked(apiModule.streamSpreadsheetChat).mockRejectedValueOnce(
      new ApiError(500, "planner failed", "req-chat"),
    );

    const state = useSseChat({
      ui: computed(() => messages.en),
    });

    const result = await state.runChatStream(
      {
        file_id: "file-1",
        chat_text: "show total",
        mode: "text",
        sheet_index: 1,
        locale: "en",
      },
      {
        onMessage: vi.fn(),
      },
    );

    expect(result.errorMessage).toBe("The streaming response was interrupted. Retry the question. [request_id=req-chat]");
  });

  it("maps network failures for batch stream requests", async () => {
    vi.mocked(apiModule.streamWorkbookBatchAnalysis).mockRejectedValueOnce(new TypeError("Failed to fetch"));

    const state = useSseChat({
      ui: computed(() => messages.en),
    });

    const result = await state.runBatchStream(
      {
        file_id: "file-1",
        question: "show total",
        mode: "text",
        sheet_indexes: [1, 2],
        locale: "en",
      },
      {
        onMessage: vi.fn(),
      },
    );

    expect(result.errorMessage).toBe(messages.en.chatConnectionError);
    expect(state.errorMessage.value).toBe(messages.en.chatConnectionError);
  });
});
