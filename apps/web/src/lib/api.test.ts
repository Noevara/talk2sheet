import { afterEach, describe, expect, it, vi } from "vitest";

import {
  fetchPreview,
  fetchWorkbookSheets,
  runWorkbookBatchAnalysis,
  streamSpreadsheetChat,
  streamWorkbookBatchAnalysis,
  uploadSpreadsheet,
} from "./api";
import { REQUEST_ID_HEADER } from "./requestId";

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

  it("adds X-Request-ID to standard json requests", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ file_id: "file-1", sheet_index: 1, sheet_name: "Sheet1", columns: [], rows: [] }), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
        },
      })
    ) as typeof fetch;

    await fetchPreview("file-1", 1);

    const requestInit = vi.mocked(globalThis.fetch).mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(requestInit.headers);
    expect(headers.get(REQUEST_ID_HEADER)).toBeTruthy();
  });

  it("adds X-Request-ID to workbook sheets requests", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ file_id: "file-1", file_name: "demo.xlsx", file_type: "xlsx", sheets: [] }), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
        },
      })
    ) as typeof fetch;

    await fetchWorkbookSheets("file-1");

    const requestInit = vi.mocked(globalThis.fetch).mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(requestInit.headers);
    expect(headers.get(REQUEST_ID_HEADER)).toBeTruthy();
  });

  it("adds X-Request-ID to file uploads", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ file_id: "file-1", file_name: "demo.csv", file_type: "csv", sheets: [] }), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
        },
      })
    ) as typeof fetch;

    await uploadSpreadsheet(new File(["a,b\n1,2\n"], "demo.csv", { type: "text/csv" }));

    const requestInit = vi.mocked(globalThis.fetch).mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(requestInit.headers);
    expect(headers.get(REQUEST_ID_HEADER)).toBeTruthy();
  });

  it("adds X-Request-ID to workbook batch analysis requests", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          request_id: "req-batch",
          file_id: "file-1",
          question: "How many rows?",
          mode: "text",
          sheet_indexes: [1],
          batch_results: [],
          summary: { total: 1, succeeded: 1, failed: 0 },
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
          },
        }
      )
    ) as typeof fetch;

    await runWorkbookBatchAnalysis({
      file_id: "file-1",
      question: "How many rows?",
      mode: "text",
      sheet_indexes: [1],
      locale: "en",
    });

    const requestInit = vi.mocked(globalThis.fetch).mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(requestInit.headers);
    expect(headers.get(REQUEST_ID_HEADER)).toBeTruthy();
  });

  it("adds X-Request-ID to workbook batch stream requests", async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("data: {\"type\":\"batch_done\"}\n\n"));
        controller.close();
      },
    });
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(stream, {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream",
        },
      })
    ) as typeof fetch;

    await streamWorkbookBatchAnalysis(
      {
        file_id: "file-1",
        question: "How many rows?",
        mode: "text",
        sheet_indexes: [1],
        locale: "en",
      },
      {
        onMessage: () => {},
      },
    );

    const requestInit = vi.mocked(globalThis.fetch).mock.calls[0]?.[1] as RequestInit;
    const headers = new Headers(requestInit.headers);
    expect(headers.get(REQUEST_ID_HEADER)).toBeTruthy();
  });
});
