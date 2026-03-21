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
        source_sheet_index: 2,
        source_sheet_name: "Users",
        sheet_routing: {
          requested_sheet_index: 1,
          resolved_sheet_index: 2,
          resolved_sheet_name: "Users",
          matched_by: "auto_routing",
          explanation: "Auto-routed to this sheet based on question and column-match signals.",
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
    expect(wrapper.text()).toContain("Source: Users (#2)");
    expect(wrapper.text()).toContain("Workbook auto-routing");
    expect(wrapper.text()).toContain("Why this sheet");
    expect(wrapper.text()).toContain("Auto-routed to this sheet based on question and column-match signals.");
    expect(wrapper.text()).toContain("Single-sheet in scope");
    expect(wrapper.text()).toContain("Routed to another sheet");
  });

  it("renders switched-from sheet hint for sequential multi-sheet flow", () => {
    const message: ChatMessage = {
      id: "assistant-sheet-switch",
      role: "assistant",
      text: "Switched to another sheet.",
      clarification: null,
      pipeline: {
        source_sheet_index: 2,
        source_sheet_name: "Users",
        sheet_sequence: {
          previous_sheet_index: 1,
          previous_sheet_name: "Sales",
          switched_from_previous: true,
          last_sheet_switch_reason: "followup_switch_to_another_sheet",
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Source: Users (#2)");
    expect(wrapper.text()).toContain("Switched from Sales (#1)");
    expect(wrapper.text()).toContain("Switch reason: Follow-up requested another sheet");
  });

  it("renders previous-sheet switch reason when routed back", () => {
    const message: ChatMessage = {
      id: "assistant-sheet-switch-previous",
      role: "assistant",
      text: "Switched back to previous sheet.",
      clarification: null,
      pipeline: {
        source_sheet_index: 1,
        source_sheet_name: "Sales",
        sheet_sequence: {
          previous_sheet_index: 2,
          previous_sheet_name: "Users",
          switched_from_previous: true,
          last_sheet_switch_reason: "followup_switch_to_previous_sheet",
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Switch reason: Follow-up requested the previous sheet");
  });

  it("renders multi-sheet boundary and mentioned sheets when out of scope", () => {
    const message: ChatMessage = {
      id: "assistant-routing-boundary",
      role: "assistant",
      text: "Cross-sheet join is not supported.",
      clarification: null,
      pipeline: {
        sheet_routing: {
          requested_sheet_index: 1,
          resolved_sheet_index: 1,
          resolved_sheet_name: "Sales",
          matched_by: "explicit_reference",
          workbook_sheet_count: 3,
          boundary_status: "multi_sheet_out_of_scope",
          boundary_reason: "cross_sheet_join_not_supported",
          decomposition_hint: "Suggested decomposition: analyze 'Sales' first, then 'Users'.",
          mentioned_sheets: [
            { sheet_index: 1, sheet_name: "Sales" },
            { sheet_index: 2, sheet_name: "Users" },
          ],
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Boundary");
    expect(wrapper.text()).toContain("Cross-sheet join is out of scope");
    expect(wrapper.text()).toContain("Mentioned sheets");
    expect(wrapper.text()).toContain("Sales (#1), Users (#2)");
    expect(wrapper.text()).toContain("Cross-sheet join or union is not supported yet.");
    expect(wrapper.text()).toContain("analyze 'Sales' first, then 'Users'");
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

  it("renders chart context metadata when provided by pipeline", () => {
    const message: ChatMessage = {
      id: "assistant-chart-context",
      role: "assistant",
      text: "Chart ready.",
      clarification: null,
      chartSpec: {
        type: "bar",
        title: "Amount by Service",
        x: "Service Name",
        y: "value",
      },
      chartData: [{ "Service Name": "Compute", value: 180 }],
      pipeline: {
        chart_context: {
          requested: true,
          rendered: true,
          title: "Amount by Service",
          x_label: "Service Name",
          y_label: "value",
          y_unit: "amount",
          point_count: 1,
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Amount by Service");
    expect(wrapper.text()).toContain("X: Service Name");
    expect(wrapper.text()).toContain("Y: value");
    expect(wrapper.text()).toContain("(amount)");
    expect(wrapper.text()).toContain("Points: 1");
  });

  it("renders chart fallback note when chart is downgraded", () => {
    const message: ChatMessage = {
      id: "assistant-chart-fallback",
      role: "assistant",
      text: "Returned text fallback.",
      clarification: null,
      pipeline: {
        chart_context: {
          requested: true,
          rendered: false,
          fallback_reason: "Chart output was downgraded to text because chart rendering failed.",
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Chart output was downgraded to text");
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

  it("renders period comparison cards from pipeline metadata", () => {
    const message: ChatMessage = {
      id: "assistant-compare",
      role: "assistant",
      text: "Compared periods.",
      clarification: null,
      pipeline: {
        planner: {
          intent: "period_compare",
          compare_basis: "year_over_year",
          current_period: "2025-02",
          previous_period: "2024-02",
        },
        answer_generation: {
          summary_kind: "period_compare",
          current_period: "2025-02",
          previous_period: "2024-02",
          current_value: "60",
          previous_value: "45",
          change_value: "15",
          change_pct: "33.3%",
          compare_ratio: "1.333x",
          compare_basis: "year_over_year",
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Period comparison");
    expect(wrapper.text()).toContain("Year over year");
    expect(wrapper.text()).toContain("2025-02");
    expect(wrapper.text()).toContain("2024-02");
    expect(wrapper.text()).toContain("33.3%");
    expect(wrapper.text()).toContain("1.333x");
  });

  it("renders applied filters and top-k summary from pipeline metadata", () => {
    const message: ChatMessage = {
      id: "assistant-scope-summary",
      role: "assistant",
      text: "Here is the filtered top-k result.",
      clarification: null,
      pipeline: {
        planner: {
          intent: "ranking",
          top_k: 2,
          value_filters: [{ column: "Region", value: "cn-sh" }],
        },
        selection_plan: {
          columns: ["Service Name", "Amount"],
          filters: [{ col: "Region", op: "=", value: "cn-sh" }],
        },
        transform_plan: {
          groupby: ["Service Name"],
          metrics: [{ agg: "sum", col: "Amount", as_name: "value" }],
          order_by: { col: "value", dir: "desc" },
          top_k: 2,
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Applied filters");
    expect(wrapper.text()).toContain("Region = cn-sh");
    expect(wrapper.text()).toContain("Top K");
    expect(wrapper.text()).toContain("2");
  });

  it("uses evidence table label for detail summary responses", () => {
    const message: ChatMessage = {
      id: "assistant-detail-summary",
      role: "assistant",
      text: "Returned detail rows.",
      clarification: null,
      pipeline: {
        answer_generation: {
          summary_kind: "detail",
        },
        transform_plan: {
          return_rows: true,
        },
        result_columns: ["Transaction ID", "Amount"],
        preview_rows: [["T-003", 120]],
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Evidence table");
  });

  it("renders trend grain summary from planner metadata", () => {
    const message: ChatMessage = {
      id: "assistant-trend-grain",
      role: "assistant",
      text: "Trend generated.",
      clarification: null,
      pipeline: {
        planner: {
          intent: "trend",
          bucket_grain: "week",
        },
        transform_plan: {
          derived_columns: [{ as_name: "week_bucket", kind: "date_bucket", source_col: "Date", grain: "week" }],
          groupby: ["week_bucket"],
          metrics: [{ agg: "sum", col: "Amount", as_name: "value" }],
          order_by: { col: "week_bucket", dir: "asc" },
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Trend grain");
    expect(wrapper.text()).toContain("Week");
  });

  it("renders follow-up suggestions and emits selected prompt", async () => {
    const message: ChatMessage = {
      id: "assistant-followup",
      role: "assistant",
      text: "Ranking result ready.",
      clarification: null,
      pipeline: {
        planner: {
          intent: "ranking",
          top_k: 5,
        },
      },
    };

    const wrapper = mount(ConversationMessage, {
      props: {
        message,
        ui,
      },
    });

    expect(wrapper.text()).toContain("Continue with");
    expect(wrapper.text()).toContain("Keep the same scope but limit to Top 3.");

    const button = wrapper
      .findAll("button.message-action-followup")
      .find((item) => item.text().includes("Top 3"));
    expect(button).toBeTruthy();

    await button!.trigger("click");
    expect(wrapper.emitted("followupSelect")).toEqual([["Keep the same scope but limit to Top 3."]]);
  });
});
