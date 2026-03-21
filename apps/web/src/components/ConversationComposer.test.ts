import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";

import ConversationComposer from "./ConversationComposer.vue";
import { messages } from "../i18n/messages";

describe("ConversationComposer", () => {
  it("emits mode changes and submit events", async () => {
    const wrapper = mount(ConversationComposer, {
      props: {
        ui: messages.en,
        question: "Show the monthly trend",
        mode: "auto",
        chatBusy: false,
      },
    });

    await wrapper.find("select").setValue("chart");
    expect(wrapper.emitted("update:mode")).toEqual([["chart"]]);

    await wrapper.find("form").trigger("submit.prevent");
    expect(wrapper.emitted("submit")).toHaveLength(1);
  });

  it("renders the stop action while streaming", () => {
    const wrapper = mount(ConversationComposer, {
      props: {
        ui: messages.en,
        question: "",
        mode: "text",
        chatBusy: true,
      },
    });

    expect(wrapper.text()).toContain(messages.en.stop);
    expect(wrapper.find("button.button-secondary").exists()).toBe(true);
  });

  it("renders categorized example prompts and applies a selected prompt", async () => {
    const wrapper = mount(ConversationComposer, {
      props: {
        ui: messages.en,
        question: "",
        mode: "auto",
        chatBusy: false,
      },
    });

    const helperTriggers = wrapper.findAll("button.helper-trigger");
    await helperTriggers[0].trigger("click");

    expect(wrapper.text()).toContain("Single-sheet analysis");
    expect(wrapper.text()).toContain("Sequential multi-sheet (A then B)");
    expect(wrapper.text()).toContain("Boundary examples (not supported yet)");

    const promptButton = wrapper
      .findAll("button.helper-example-chip")
      .find((button) => button.text().includes("How many rows are in the current sheet?"));
    expect(promptButton).toBeTruthy();

    await promptButton!.trigger("click");

    expect(wrapper.emitted("update:question")).toEqual([["How many rows are in the current sheet?"]]);
  });
});
