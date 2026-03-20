import { computed } from "vue";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "../../../lib/api";
import { messages } from "../../../i18n/messages";
import { useWorkbook } from "./useWorkbook";

vi.mock("../../../lib/api", () => ({
  uploadSpreadsheet: vi.fn(),
  fetchPreview: vi.fn(),
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

describe("useWorkbook", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("rejects unsupported file types before upload", async () => {
    const state = useWorkbook({
      ui: computed(() => messages.en),
      resetConversation: vi.fn(),
    });

    const input = document.createElement("input");
    const file = new File(["hello"], "notes.txt", { type: "text/plain" });
    Object.defineProperty(input, "files", {
      value: [file],
      configurable: true,
    });

    await state.handleFileSelection({
      target: input,
    } as unknown as Event);

    expect(state.errorMessage.value).toBe(messages.en.uploadInvalidFileError);
    expect(apiModule.uploadSpreadsheet).not.toHaveBeenCalled();
  });

  it("clears restored state when the workbook file no longer exists", async () => {
    vi.mocked(apiModule.fetchPreview).mockRejectedValueOnce(
      new ApiError(404, "Spreadsheet file not found.", "req-missing"),
    );

    const resetConversation = vi.fn();
    const state = useWorkbook({
      ui: computed(() => messages.en),
      resetConversation,
    });

    state.restoreState({
      workbook: {
        file_id: "file-1",
        file_name: "demo.xlsx",
        file_type: "xlsx",
        sheets: [{ index: 1, name: "Sheet1" }],
      },
      selectedSheetIndex: 1,
      pendingSheetOverride: false,
      preview: {
        file_id: "file-1",
        sheet_index: 1,
        sheet_name: "Sheet1",
        columns: ["Category"],
        rows: [["A"]],
      },
    });

    const restored = await state.revalidateRestoredState();

    expect(restored).toBe(false);
    expect(state.workbook.value).toBeNull();
    expect(state.preview.value).toBeNull();
    expect(state.errorMessage.value).toBe(messages.en.restoreSessionExpiredError);
    expect(resetConversation).toHaveBeenCalledTimes(1);
  });
});
