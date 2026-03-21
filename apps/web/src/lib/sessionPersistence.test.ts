import { afterEach, describe, expect, it } from "vitest";

import { clearPersistedSession, loadPersistedSession, savePersistedSession } from "./sessionPersistence";

describe("sessionPersistence", () => {
  afterEach(() => {
    clearPersistedSession();
  });

  it("restores persisted sessions and normalizes streaming messages", () => {
    savePersistedSession({
      version: 1,
      locale: "en",
      workbook: {
        workbook: {
          file_id: "file-1",
          file_name: "demo.xlsx",
          file_type: "xlsx",
          sheets: [{ index: 1, name: "Sheet1" }],
        },
        selectedSheetIndex: 1,
        pendingSheetOverride: false,
        batchSelectedSheetIndexes: [1],
        recentBatchSheetIndexes: [1, 2],
        preview: {
          file_id: "file-1",
          sheet_index: 1,
          sheet_name: "Sheet1",
          columns: ["Category", "Amount"],
          rows: [["A", 1]],
        },
      },
      conversation: {
        question: "show total",
        conversationId: "conv-1",
        chatMode: "auto",
        chatMessages: [
          {
            id: "msg-1",
            role: "assistant",
            text: "Thinking",
            status: "streaming",
          },
        ],
      },
    });

    const restored = loadPersistedSession();

    expect(restored?.conversation.chatMessages[0]?.status).toBe("done");
    expect(restored?.workbook.workbook?.file_id).toBe("file-1");
    expect(restored?.workbook.recentBatchSheetIndexes).toEqual([1, 2]);
  });
});
