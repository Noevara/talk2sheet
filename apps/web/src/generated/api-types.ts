// This file is generated from packages/contracts/openapi.json.
// Do not edit manually.

export interface HealthResponse {
  status?: string;
  service?: string;
}

export interface SheetDescriptor {
  index: number;
  name: string;
  rows?: number | null;
  columns?: number | null;
}

export interface UploadedFileResponse {
  file_id: string;
  file_name: string;
  file_type: string;
  sheets?: SheetDescriptor[];
}

export interface PreviewResponse {
  file_id: string;
  sheet_index: number;
  sheet_name: string;
  columns: string[];
  rows: unknown[][];
  total_rows?: number | null;
  preview_row_count?: number | null;
}

export interface ClarificationResolution {
  kind?: "column_resolution" | "sheet_resolution";
  source_field?: string | null;
  selected_value: string;
}

export interface SpreadsheetChatRequest {
  file_id: string;
  chat_text: string;
  mode?: "auto" | "text" | "chart";
  sheet_index?: number;
  sheet_override?: boolean;
  locale?: string;
  conversation_id?: string | null;
  clarification_resolution?: ClarificationResolution | null;
}
