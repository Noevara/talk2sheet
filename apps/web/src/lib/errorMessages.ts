import type { UiMessages } from "../i18n/messages";
import { ApiError } from "./api";

function appendRequestId(message: string, requestId: string | null | undefined): string {
  return requestId ? `${message} [request_id=${requestId}]` : message;
}

function withApiContext(message: string, error: ApiError, includeDetail = false): string {
  if (!includeDetail || !error.detail.trim()) {
    return appendRequestId(message, error.requestId);
  }
  return appendRequestId(`${message}: ${error.detail}`, error.requestId);
}

export function isNetworkError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  return error instanceof TypeError || /failed to fetch|networkerror|load failed/i.test(error.message);
}

export function isMissingWorkbookError(error: unknown): boolean {
  if (!(error instanceof ApiError)) {
    return false;
  }
  return error.status === 404 || /not found|no such file|does not exist|missing/i.test(error.detail.toLowerCase());
}

export function formatUploadError(error: unknown, ui: UiMessages): string {
  if (isNetworkError(error)) {
    return ui.networkError;
  }
  if (error instanceof ApiError) {
    const detail = error.detail.toLowerCase();
    if (error.status === 413 || detail.includes("too large")) {
      return withApiContext(ui.uploadTooLargeError, error);
    }
    if (error.status === 400 || error.status === 415 || error.status === 422 || detail.includes(".xlsx") || detail.includes(".xls") || detail.includes(".csv") || detail.includes("format") || detail.includes("extension")) {
      return withApiContext(ui.uploadInvalidFileError, error);
    }
    if (error.status >= 500) {
      return withApiContext(ui.uploadServerError, error);
    }
    return withApiContext(ui.uploadError, error, true);
  }
  return ui.uploadError;
}

export function formatPreviewError(error: unknown, ui: UiMessages): string {
  if (isNetworkError(error)) {
    return ui.networkError;
  }
  if (error instanceof ApiError) {
    if (isMissingWorkbookError(error)) {
      return withApiContext(ui.previewMissingError, error);
    }
    if (error.status >= 500) {
      return withApiContext(ui.previewServerError, error);
    }
    return withApiContext(ui.previewError, error, true);
  }
  return ui.previewError;
}

export function formatChatError(error: unknown, ui: UiMessages): string {
  if (isNetworkError(error)) {
    return ui.chatConnectionError;
  }
  if (error instanceof ApiError) {
    if (error.status >= 500) {
      return withApiContext(ui.chatInterruptedError, error);
    }
    return withApiContext(ui.chatError, error, true);
  }
  return ui.chatInterruptedError;
}
