import type {
  ClarificationResolution as GeneratedClarificationResolution,
  PreviewResponse as GeneratedPreviewResponse,
  SheetDescriptor as GeneratedSheetDescriptor,
  SpreadsheetChatRequest as GeneratedSpreadsheetChatRequest,
  UploadedFileResponse as GeneratedUploadedFileResponse,
} from "./generated/api-types";

export type Locale = "en" | "zh-CN" | "ja-JP";
export type ChatMode = "auto" | "text" | "chart";
export type ChartType = "line" | "bar" | "pie";

export type SheetDescriptor = GeneratedSheetDescriptor;
export interface UploadedFileResponse extends Omit<GeneratedUploadedFileResponse, "sheets"> {
  sheets: SheetDescriptor[];
}
export type PreviewResponse = GeneratedPreviewResponse;

export interface ExecutionDisclosure {
  data_scope: "exact_full_table" | "sampled_first_n";
  exact_used: boolean;
  scope_text: string;
  scope_warning?: string;
  fallback_reason?: string;
  fallback_reason_code?: string;
  max_rows?: number | null;
}

export interface ChartSpec {
  type: ChartType;
  title?: string | null;
  x: string;
  y: string;
  top_k?: number | null;
}

export interface ClarificationResolution extends Omit<GeneratedClarificationResolution, "kind"> {
  kind: "column_resolution" | "sheet_resolution";
}

export interface SpreadsheetChatRequest extends Omit<GeneratedSpreadsheetChatRequest, "mode" | "locale"> {
  mode: ChatMode;
  locale: Locale;
  sheet_override?: boolean;
  clarification_resolution?: ClarificationResolution | null;
}

export interface AnswerSegments {
  conclusion?: string;
  evidence?: string;
  riskNote?: string;
}

export interface ClarificationOption {
  label: string;
  value: string;
  description?: string;
}

export interface ClarificationPayload {
  kind?: "column_resolution" | "sheet_resolution";
  reason: string;
  field?: string;
  options: ClarificationOption[];
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  text: string;
  status?: "streaming" | "done";
  mode?: ChatMode;
  analysisText?: string;
  answerSegments?: AnswerSegments | null;
  executionDisclosure?: ExecutionDisclosure | null;
  chartSpec?: ChartSpec | null;
  chartData?: Record<string, unknown>[] | null;
  clarification?: ClarificationPayload | null;
  pipeline?: Record<string, unknown> | null;
  meta?: Record<string, unknown> | null;
  tableColumns?: string[];
}
