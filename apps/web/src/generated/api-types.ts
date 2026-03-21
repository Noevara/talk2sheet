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
  field_summary?: string[];
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
  followup_action?: string | null;
}

export interface ExecutionDisclosure {
  data_scope: "exact_full_table" | "sampled_first_n";
  exact_used: boolean;
  scope_text: string;
  scope_warning?: string;
  fallback_reason?: string;
  fallback_reason_code?: string;
  max_rows?: number | null;
}

export interface SpreadsheetBatchRequest {
  file_id: string;
  question: string;
  mode?: "auto" | "text" | "chart";
  sheet_indexes?: number[];
  locale?: string;
}

export interface SpreadsheetBatchResult {
  sheet_index: number;
  sheet_name: string;
  status: "success" | "failed";
  mode?: "auto" | "text" | "chart" | string;
  answer?: string;
  analysis_text?: string;
  result_row_count?: number;
  pipeline?: Record<string, unknown>;
  execution_disclosure?: ExecutionDisclosure | null;
  error?: string;
  reason_code?: string;
}

export interface SpreadsheetBatchSummary {
  total: number;
  succeeded: number;
  failed: number;
}

export interface SpreadsheetBatchResponse {
  request_id: string;
  file_id: string;
  question: string;
  mode?: "auto" | "text" | "chart" | string;
  sheet_indexes?: number[];
  batch_results?: SpreadsheetBatchResult[];
  summary: SpreadsheetBatchSummary;
}
