import { describe, expect, it } from "vitest";

import { messages } from "../i18n/messages";
import { ApiError } from "./api";
import { formatChatError, formatPreviewError, formatUploadError, isMissingWorkbookError } from "./errorMessages";

describe("errorMessages", () => {
  it("formats upload too large errors with request ids", () => {
    const error = new ApiError(413, "Payload too large", "req-upload");
    expect(formatUploadError(error, messages.en)).toBe(
      "This file is too large to upload. Try a smaller workbook. [request_id=req-upload]",
    );
  });

  it("detects missing uploaded workbooks for preview recovery", () => {
    const error = new ApiError(404, "Spreadsheet file not found.", "req-preview");
    expect(isMissingWorkbookError(error)).toBe(true);
    expect(formatPreviewError(error, messages.en)).toBe(
      "This uploaded workbook is no longer available. Upload it again. [request_id=req-preview]",
    );
  });

  it("maps network chat failures to connection guidance", () => {
    expect(formatChatError(new TypeError("Failed to fetch"), messages.en)).toBe(
      "Cannot reach the streaming API. Check the backend and try again.",
    );
  });
});
