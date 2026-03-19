import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";

import ConversationMessage from "./ConversationMessage.vue";
import type { ChatMessage } from "../types";

const labels = {
  userLabel: "User",
  assistantLabel: "Assistant",
  streamingLabel: "streaming",
  scopeLabel: "Execution scope",
  metadataLabel: "Metadata",
  sheetRoutingLabel: "Sheet routing",
  requestedSheetLabel: "Requested sheet",
  resolvedSheetLabel: "Resolved sheet",
  routingMethodLabel: "Matched by",
  routingChangedLabel: "Routed to another sheet",
  routingMethodSingleSheetLabel: "Single-sheet workbook",
  routingMethodExplicitLabel: "Question explicitly mentioned the sheet",
  routingMethodClarificationLabel: "Clarification selection",
  routingMethodManualOverrideLabel: "Manual sheet selection",
  routingMethodFollowupLabel: "Previous turn context",
  routingMethodAutoLabel: "Workbook auto-routing",
  routingMethodRequestedLabel: "Requested sheet fallback",
  conclusionLabel: "Conclusion",
  evidenceLabel: "Evidence",
  riskNoteLabel: "Risk note",
  clarificationLabel: "Clarification",
  clarificationApplyLabel: "Use this field",
  pipelineLabel: "Pipeline",
  selectionPlanLabel: "Selection",
  transformPlanLabel: "Transform",
  detailRowsLabel: "Detail rows",
  resultTableLabel: "Result table",
  forecastLabel: "Forecast",
  forecastBadgeLabel: "Model estimate",
  forecastTargetLabel: "Target",
  forecastEstimateLabel: "Estimate",
  forecastRangeLabel: "Range",
  forecastModelLabel: "Model",
  forecastHistoryLabel: "History",
  forecastHistoryPointsLabel: "Points",
  forecastGrainLabel: "Grain",
  forecastHorizonLabel: "Horizon",
  forecastTableLabel: "Forecast table",
  forecastModelLinearLabel: "Linear regression",
  forecastModelSmoothingLabel: "Simple exponential smoothing",
  forecastGrainDayLabel: "Day",
  forecastGrainWeekLabel: "Week",
  forecastGrainMonthLabel: "Month",
  chartLabel: "Chart",
  noChartData: "No chart data",
};

describe("ConversationMessage", () => {
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
        labels,
      },
    });

    expect(wrapper.text()).toContain("Conclusion");
    expect(wrapper.text()).toContain("Service A ranks first.");
    expect(wrapper.text()).toContain("Grouped totals show Service A at 180.");
    expect(wrapper.text()).toContain("This answer reflects the active sheet only.");
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
        labels,
      },
    });

    const buttons = wrapper.findAll("button.clarification-option");
    expect(buttons).toHaveLength(2);

    await buttons[1].trigger("click");

    expect(wrapper.emitted("clarificationSelect")).toEqual([["Region"]]);
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
        labels,
      },
    });

    expect(wrapper.text()).toContain("Sheet routing");
    expect(wrapper.text()).toContain("Requested sheet");
    expect(wrapper.text()).toContain("#1");
    expect(wrapper.text()).toContain("Users (#2)");
    expect(wrapper.text()).toContain("Workbook auto-routing");
    expect(wrapper.text()).toContain("Routed to another sheet");
  });
});
