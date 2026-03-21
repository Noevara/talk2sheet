<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";

import type { UiMessages } from "../i18n/messages";
import type { ChatMode } from "../types";

const props = defineProps<{
  ui: UiMessages;
  question: string;
  focusToken?: number;
  mode: ChatMode;
  chatBusy: boolean;
}>();

const emit = defineEmits<{
  "update:question": [value: string];
  "update:mode": [value: ChatMode];
  submit: [];
  stop: [];
}>();

const helperPanel = ref<"examples" | "guide" | null>(null);
const composerRef = ref<HTMLElement | null>(null);
const textareaRef = ref<HTMLTextAreaElement | null>(null);

function toggleHelperPanel(panel: "examples" | "guide"): void {
  helperPanel.value = helperPanel.value === panel ? null : panel;
}

function useSuggestedPrompt(prompt: string): void {
  emit("update:question", prompt);
  helperPanel.value = null;
}

function handleDocumentPointerDown(event: PointerEvent): void {
  if (!helperPanel.value) {
    return;
  }
  const target = event.target;
  if (!(target instanceof Node)) {
    return;
  }
  if (composerRef.value?.contains(target)) {
    return;
  }
  helperPanel.value = null;
}

onMounted(() => {
  document.addEventListener("pointerdown", handleDocumentPointerDown);
});

onBeforeUnmount(() => {
  document.removeEventListener("pointerdown", handleDocumentPointerDown);
});

watch(
  () => props.focusToken,
  async (current, previous) => {
    if (!current || current === previous) {
      return;
    }
    await nextTick();
    const textarea = textareaRef.value;
    if (!textarea) {
      return;
    }
    textarea.focus();
    const cursor = textarea.value.length;
    textarea.setSelectionRange(cursor, cursor);
  },
);
</script>

<template>
  <form ref="composerRef" class="composer composer-panel" @submit.prevent="emit('submit')">
    <div class="composer-topbar">
      <div class="helper-bar">
        <button
          type="button"
          class="helper-trigger"
          :class="{ 'helper-trigger-active': helperPanel === 'examples' }"
          @click="toggleHelperPanel('examples')"
        >
          {{ props.ui.examplesButtonLabel }}
        </button>
        <button
          type="button"
          class="helper-trigger"
          :class="{ 'helper-trigger-active': helperPanel === 'guide' }"
          @click="toggleHelperPanel('guide')"
        >
          {{ props.ui.guideButtonLabel }}
        </button>
      </div>

      <label class="mode-switcher">
        <span>{{ props.ui.modeLabel }}</span>
        <select
          :value="props.mode"
          :disabled="props.chatBusy"
          @change="emit('update:mode', ($event.target as HTMLSelectElement).value as ChatMode)"
        >
          <option value="auto">{{ props.ui.modeAutoLabel }}</option>
          <option value="text">{{ props.ui.modeTextLabel }}</option>
          <option value="chart">{{ props.ui.modeChartLabel }}</option>
        </select>
      </label>
    </div>

    <div v-if="helperPanel" class="helper-popover">
      <div v-if="helperPanel === 'examples'" class="helper-panel">
        <div class="helper-panel-title">{{ props.ui.suggestionsLabel }}</div>
        <div class="helper-example-groups">
          <section
            v-for="group in props.ui.suggestionGroups"
            :key="group.label"
            class="helper-example-group"
          >
            <div class="helper-example-group-title">{{ group.label }}</div>
            <div class="helper-example-list">
              <button
                v-for="prompt in group.prompts"
                :key="prompt"
                type="button"
                class="helper-example-chip"
                @click="useSuggestedPrompt(prompt)"
              >
                {{ prompt }}
              </button>
            </div>
          </section>
        </div>
      </div>

      <div v-else class="helper-panel">
        <div class="helper-panel-title">{{ props.ui.capabilityTitle }}</div>
        <div class="helper-panel-text">{{ props.ui.capabilityBody }}</div>
        <div class="helper-panel-title helper-panel-title-secondary">{{ props.ui.outOfScopeTitle }}</div>
        <div class="helper-panel-text">{{ props.ui.outOfScopeBody }}</div>
      </div>
    </div>

    <textarea
      ref="textareaRef"
      class="composer-textarea"
      :value="props.question"
      :placeholder="props.ui.questionPlaceholder"
      @input="emit('update:question', ($event.target as HTMLTextAreaElement).value)"
    ></textarea>

    <div class="composer-actions">
      <button
        v-if="props.chatBusy"
        type="button"
        class="button button-secondary"
        @click="emit('stop')"
      >
        {{ props.ui.stop }}
      </button>
      <button type="submit" class="button button-primary" :disabled="props.chatBusy">
        {{ props.ui.send }}
      </button>
    </div>
  </form>
</template>

<style scoped>
.composer-topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
  flex-wrap: wrap;
}

.mode-switcher {
  display: inline-flex;
  align-items: center;
  gap: 0.55rem;
  min-height: 1.4rem;
}

.mode-switcher span {
  font-size: 0.86rem;
  line-height: 1.4;
  color: rgba(29, 95, 133, 0.92);
  white-space: nowrap;
}

.mode-switcher select {
  min-width: 120px;
  padding: 0.5rem 0.75rem;
  border-radius: 14px;
  border: 1px solid rgba(18, 41, 74, 0.12);
  background: rgba(255, 255, 255, 0.92);
  color: #173254;
  font-size: 0.78rem;
  line-height: 1.2;
}
</style>
