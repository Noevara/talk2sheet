import { describe, expect, it } from "vitest";

import { applyStreamPayload, extractClarification } from "./chatPayload";
import type { ChatMessage } from "../types";

function createAssistantMessage(): ChatMessage {
  return {
    id: "assistant-1",
    role: "assistant",
    text: "Thinking",
    status: "streaming",
    clarification: null,
    tableColumns: [],
  };
}

describe("chatPayload", () => {
  it("extracts structured clarification payload from pipeline metadata", () => {
    const clarification = extractClarification({
      clarification: {
        reason: "Which field should be used as the dimension?",
        field: "dimension_column",
        options: [
          { label: "Service", value: "Service" },
          { label: "Region", value: "Region", description: "Break down by geography" },
        ],
      },
    });

    expect(clarification).toEqual({
      kind: "column_resolution",
      reason: "Which field should be used as the dimension?",
      field: "dimension_column",
      options: [
        { label: "Service", value: "Service" },
        { label: "Region", value: "Region", description: "Break down by geography" },
      ],
    });
  });

  it("applies answer, segments, clarification and result columns to the stream message", () => {
    const message = createAssistantMessage();

    applyStreamPayload(message, {
      answer: "Service A ranks first.",
      mode: "chart",
      analysis_text: "A bar chart was generated from grouped totals.",
      answer_segments: {
        conclusion: "Service A ranks first.",
        evidence: "Grouped totals show Service A at 180 and Service B at 50.",
        risk_note: "This answer reflects the active sheet only.",
      },
      pipeline: {
        result_columns: ["Service", "Amount"],
        clarification: {
          reason: "Which field should be used as the dimension?",
          field: "dimension_column",
          options: [{ label: "Service", value: "Service" }],
        },
      },
      execution_disclosure: {
        data_scope: "exact_full_table",
        exact_used: true,
        scope_text: "Used the full active sheet.",
      },
    });

    expect(message.text).toBe("Service A ranks first.");
    expect(message.mode).toBe("chart");
    expect(message.answerSegments?.evidence).toContain("Service A");
    expect(message.clarification?.kind).toBe("column_resolution");
    expect(message.clarification?.field).toBe("dimension_column");
    expect(message.tableColumns).toEqual(["Service", "Amount"]);
    expect(message.executionDisclosure?.scope_text).toBe("Used the full active sheet.");
  });

  it("recognizes sheet routing clarification payloads", () => {
    const clarification = extractClarification({
      clarification_stage: "sheet_routing",
      clarification: {
        reason: "Choose a sheet first.",
        field: "sheet",
        options: [
          { label: "Sales", value: "Sales" },
          { label: "Users", value: "Users" },
        ],
      },
    });

    expect(clarification).toEqual({
      kind: "sheet_resolution",
      reason: "Choose a sheet first.",
      field: "sheet",
      options: [
        { label: "Sales", value: "Sales" },
        { label: "Users", value: "Users" },
      ],
    });
  });
});
