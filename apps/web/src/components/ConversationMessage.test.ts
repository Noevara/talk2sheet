import { mount } from "@vue/test-utils";
import { beforeEach, describe, expect, it, vi } from "vitest";

import ConversationMessage from "./ConversationMessage.vue";
import type { ChatMessage } from "../types";
import { messages } from "../i18n/messages";
import { copyText, downloadChartPng, downloadCsv } from "../lib/browserExport";

vi.mock("../lib/browserExport", () => ({
  copyText: vi.fn(() => Promise.resolve()),
  downloadCsv: vi.fn(),
  downloadChartPng: vi.fn(() => Promise.resolve()),
}));

const ui = messages.en;

describe("ConversationMessage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders structured answer segments for assistant messages", () => {
    const message: ChatMessage = {
      id: "assistant-1",
      role: "assistant",
      text: "Service A ranks first.",
      answerSegments: {
        conclusion: "Service A ranks first.",
        evidence: "Grouped totals show Service A at 180.",
        riskNote: "This answer reflects the active sheet only.",
      },
      clarification: null,
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Conclusion");
    expect(wrapper.text()).toContain("Service A ranks first.");
    expect(wrapper.text()).toContain("Grouped totals show Service A at 180.");
    expect(wrapper.text()).toContain("This answer reflects the active sheet only.");
  });

  it("copies the assistant answer with structured sections", async () => {
    const message: ChatMessage = {
      id: "assistant-copy",
      role: "assistant",
      text: "Service A ranks first.",
      answerSegments: {
        conclusion: "Service A ranks first.",
        evidence: "Grouped totals show Service A at 180.",
        riskNote: "This answer reflects the active sheet only.",
      },
      clarification: null,
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    const copyButton = wrapper.find("button.message-action");
    await copyButton.trigger("click");

    expect(copyText).toHaveBeenCalledWith(
      "Conclusion\nService A ranks first.\n\nEvidence\nGrouped totals show Service A at 180.\n\nRisk note\nThis answer reflects the active sheet only.",
    );
    expect(wrapper.text()).toContain("Copied");
  });

  it("emits selected clarification option from the rendered clarification card", async () => {
    const message: ChatMessage = {
      id: "assistant-2",
      role: "assistant",
      text: "Need clarification",
      clarification: {
        reason: "Which field should be used?",
        field: "dimension_column",
        options: [
          { label: "Service", value: "Service" },
          { label: "Region", value: "Region" },
        ],
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    const buttons = wrapper.findAll("button.clarification-option");
    expect(buttons).toHaveLength(2);

    await buttons[1].trigger("click");

    expect(wrapper.emitted("clarificationSelect")).toEqual([["Region"]]);
  });

  it("renders sheet clarification title and friendly reason prefix", () => {
    const message: ChatMessage = {
      id: "assistant-clarification-sheet",
      role: "assistant",
      text: "Need clarification",
      clarification: {
        kind: "sheet_resolution",
        reason: "I found multiple candidate sheets.",
        options: [
          { label: "Sales", value: "Sales" },
          { label: "Users", value: "Users" },
        ],
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Confirm sheet");
    expect(wrapper.text()).toContain("To keep the result accurate, please confirm this first:");
  });

  it("renders sheet routing summary when another sheet is resolved", () => {
    const message: ChatMessage = {
      id: "assistant-3",
      role: "assistant",
      text: "Users sheet selected.",
      clarification: null,
      pipeline: {
        sheet_routing: {
          requested_sheet_index: 1,
          resolved_sheet_index: 2,
          resolved_sheet_name: "Users",
          matched_by: "auto_routing",
          workbook_sheet_count: 2,
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Sheet routing");
    expect(wrapper.text()).toContain("Requested sheet");
    expect(wrapper.text()).toContain("#1");
    expect(wrapper.text()).toContain("Users (#2)");
    expect(wrapper.text()).toContain("Workbook auto-routing");
    expect(wrapper.text()).toContain("Routed to another sheet");
  });

  it("exports detail rows as csv for assistant result tables", async () => {
    const message: ChatMessage = {
      id: "assistant-export",
      role: "assistant",
      text: "Here is the result table.",
      clarification: null,
      pipeline: {
        result_columns: ["Category", "Amount"],
        preview_rows: [
          ["Service A", 180],
          ["Service B", 120],
        ],
        sheet_routing: {
          resolved_sheet_index: 2,
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    const exportButton = wrapper.find("button.message-action-csv");
    await exportButton.trigger("click");

    expect(downloadCsv).toHaveBeenCalledWith("talk2sheet-sheet-2-result.csv", ["Category", "Amount"], [
      ["Service A", 180],
      ["Service B", 120],
    ]);
  });

  it("exports chart output as png when chart data exists", async () => {
    const message: ChatMessage = {
      id: "assistant-chart-export",
      role: "assistant",
      text: "Here is a trend chart.",
      clarification: null,
      chartSpec: {
        type: "line",
        x: "month",
        y: "amount",
      },
      chartData: [
        { month: "2026-01", amount: 1200 },
        { month: "2026-02", amount: 1320 },
      ],
      pipeline: {
        sheet_routing: {
          resolved_sheet_index: 3,
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    const exportButton = wrapper.find("button.message-action-chart");
    await exportButton.trigger("click");

    expect(downloadChartPng).toHaveBeenCalledWith("talk2sheet-sheet-3-line.png", message.chartSpec, message.chartData);
  });

  it("renders forecast summary cards from pipeline metadata", () => {
    const message: ChatMessage = {
      id: "assistant-forecast",
      role: "assistant",
      text: "Forecast ready.",
      clarification: null,
      pipeline: {
        planner: {
          intent: "forecast_timeseries",
        },
        answer_generation: {
          summary_kind: "forecast_timeseries",
          period: "2026-04",
        },
        transform_meta: {
          forecast: {
            model: "linear_regression",
            grain: "month",
            forecast_value: 2180,
            lower_bound: 2100,
            upper_bound: 2260,
            history_start: "2025-10",
            history_end: "2026-03",
            history_points: 6,
            horizon: 1,
          },
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Forecast");
    expect(wrapper.text()).toContain("2,180");
    expect(wrapper.text()).toContain("2026-04");
    expect(wrapper.text()).toContain("2,100 - 2,260");
    expect(wrapper.text()).toContain("Linear regression");
    expect(wrapper.text()).toContain("2025-10 - 2026-03");
  });
});
