import type { PreviewResponse, SpreadsheetChatRequest, UploadedFileResponse } from "../types";

const API_BASE_URL = (
  import.meta.env.VITE_API_BASE_URL ||
  (import.meta.env.DEV ? "http://127.0.0.1:8000/api" : "/api")
).replace(/\/$/, "");

function buildUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_BASE_URL}${normalizedPath}`;
}

async function parseError(response: Response): Promise<string> {
  const headerRequestId = response.headers.get("x-request-id");
  try {
    const payload = (await response.json()) as { detail?: string; request_id?: string };
    const requestId = payload.request_id || headerRequestId;
    const detail = payload.detail || response.statusText;
    return requestId ? `${detail} [request_id=${requestId}]` : detail;
  } catch {
    const detail = response.statusText || "Unknown error";
    return headerRequestId ? `${detail} [request_id=${headerRequestId}]` : detail;
  }
}

async function ensureJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    throw new Error(await parseError(response));
  }
  return (await response.json()) as T;
}

export async function uploadSpreadsheet(file: File): Promise<UploadedFileResponse> {
  const body = new FormData();
  body.append("file", file);

  const response = await fetch(buildUrl("/files/upload"), {
    method: "POST",
    body,
  });

  return ensureJson<UploadedFileResponse>(response);
}

export async function fetchPreview(fileId: string, sheetIndex: number): Promise<PreviewResponse> {
  const response = await fetch(buildUrl(`/files/${fileId}/preview?sheet_index=${sheetIndex}`));
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
  const response = await fetch(buildUrl("/spreadsheet/chat/stream"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
    signal: options.signal,
  });

  if (!response.ok || !response.body) {
    throw new Error(await parseError(response));
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
