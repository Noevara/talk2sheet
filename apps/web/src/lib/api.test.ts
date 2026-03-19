import { afterEach, describe, expect, it, vi } from "vitest";

import { streamSpreadsheetChat } from "./api";

const originalFetch = globalThis.fetch;

describe("api error formatting", () => {
  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it("includes request_id from error payloads", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ detail: "Spreadsheet file not found.", request_id: "req-404" }), {
        status: 404,
        headers: {
          "Content-Type": "application/json",
          "X-Request-ID": "req-404",
        },
      })
    ) as typeof fetch;

    await expect(
      streamSpreadsheetChat(
        {
          file_id: "missing",
          chat_text: "test",
          mode: "text",
          sheet_index: 1,
          locale: "en",
        },
        {
          onMessage: () => {},
        }
      )
    ).rejects.toThrow("Spreadsheet file not found. [request_id=req-404]");
  });
});
