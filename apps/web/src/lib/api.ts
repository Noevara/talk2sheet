import type { PreviewResponse, SpreadsheetChatRequest, UploadedFileResponse } from "../types";
import { createRequestId, REQUEST_ID_HEADER } from "./requestId";

const API_BASE_URL = (
  import.meta.env.VITE_API_BASE_URL ||
  (import.meta.env.DEV ? "http://127.0.0.1:8000/api" : "/api")
).replace(/\/$/, "");

type ApiFetchOptions = {
  method?: string;
  headers?: HeadersInit;
  body?: BodyInit | null;
  signal?: AbortSignal;
};

function buildUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_BASE_URL}${normalizedPath}`;
}

function buildRequestHeaders(headers?: HeadersInit): Headers {
  const merged = new Headers(headers);
  if (!merged.has(REQUEST_ID_HEADER)) {
    merged.set(REQUEST_ID_HEADER, createRequestId());
  }
  return merged;
}

async function apiFetch(path: string, options: ApiFetchOptions = {}): Promise<Response> {
  return fetch(buildUrl(path), {
    ...options,
    headers: buildRequestHeaders(options.headers),
  });
}

export class ApiError extends Error {
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
}

async function parseError(response: Response): Promise<{ detail: string; requestId: string | null }> {
  const headerRequestId = response.headers.get("x-request-id");
  try {
    const payload = (await response.json()) as { detail?: string; request_id?: string };
    return {
      requestId: payload.request_id || headerRequestId,
      detail: payload.detail || response.statusText || "Unknown error",
    };
  } catch {
    return {
      requestId: headerRequestId,
      detail: response.statusText || "Unknown error",
    };
  }
}

async function ensureJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const parsedError = await parseError(response);
    throw new ApiError(response.status, parsedError.detail, parsedError.requestId);
  }
  return (await response.json()) as T;
}

export async function uploadSpreadsheet(file: File): Promise<UploadedFileResponse> {
  const body = new FormData();
  body.append("file", file);

  const response = await apiFetch("/files/upload", {
    method: "POST",
    body,
  });

  return ensureJson<UploadedFileResponse>(response);
}

export async function fetchPreview(fileId: string, sheetIndex: number): Promise<PreviewResponse> {
  const response = await apiFetch(`/files/${fileId}/preview?sheet_index=${sheetIndex}`);
  return ensureJson<PreviewResponse>(response);
}

function emitSseChunk(chunk: string, onMessage: (payload: Record<string, unknown>) => void): void {
  const lines = chunk
    .split("\n")
    .map((line) => line.trimEnd())
    .filter(Boolean);

  const dataLines = lines.filter((line) => line.startsWith("data:"));
  if (!dataLines.length) {
    return;
  }

  const payload = dataLines.map((line) => line.slice(5).trimStart()).join("\n");
  onMessage(JSON.parse(payload) as Record<string, unknown>);
}

export async function streamSpreadsheetChat(
  request: SpreadsheetChatRequest,
  options: {
    signal?: AbortSignal;
    onMessage: (payload: Record<string, unknown>) => void;
  },
): Promise<void> {
  const response = await apiFetch("/spreadsheet/chat/stream", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
    signal: options.signal,
  });

  if (!response.ok || !response.body) {
    const parsedError = await parseError(response);
    throw new ApiError(response.status, parsedError.detail, parsedError.requestId);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value || new Uint8Array(), { stream: !done }).replace(/\r\n/g, "\n");

    let boundary = buffer.indexOf("\n\n");
    while (boundary >= 0) {
      const chunk = buffer.slice(0, boundary).trim();
      buffer = buffer.slice(boundary + 2);
      if (chunk) {
        emitSseChunk(chunk, options.onMessage);
      }
      boundary = buffer.indexOf("\n\n");
    }

    if (done) {
      break;
    }
  }

  const trailing = buffer.trim();
  if (trailing) {
    emitSseChunk(trailing, options.onMessage);
  }
}
