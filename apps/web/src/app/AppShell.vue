<script setup lang="ts">
import { computed, ref } from "vue";

import AppHeader from "../components/AppHeader.vue";
import ConversationFeaturePanel from "../features/conversation/components/ConversationFeaturePanel.vue";
import { useConversation } from "../features/conversation/composables/useConversation";
import LocaleSwitcher from "../features/settings/components/LocaleSwitcher.vue";
import WorkbookFeaturePanel from "../features/workbook/components/WorkbookFeaturePanel.vue";
import { useWorkbook } from "../features/workbook/composables/useWorkbook";
import { messages, normalizeLocale } from "../i18n/messages";
import type { Locale } from "../types";

const locale = ref<Locale>(normalizeLocale(globalThis.navigator?.language));
const ui = computed(() => messages[locale.value]);

const localeOptions: Array<{ value: Locale; label: string }> = [
  { value: "en", label: "English" },
  { value: "zh-CN", label: "中文" },
  { value: "ja-JP", label: "日本語" },
];

const conversation = useConversation({
  locale,
  ui,
});

const workbookState = useWorkbook({
  ui,
  resetConversation: conversation.resetConversation,
});

conversation.bindWorkbookContext({
  workbook: workbookState.workbook,
  preview: workbookState.preview,
  selectedSheetIndex: workbookState.selectedSheetIndex,
  pendingSheetOverride: workbookState.pendingSheetOverride,
  clearPendingSheetOverride: workbookState.clearPendingSheetOverride,
});

const errorMessage = computed(() => workbookState.errorMessage.value || conversation.errorMessage.value);
</script>

<template>
  <div class="app-shell">
    <div class="ambient ambient-left"></div>
    <div class="ambient ambient-right"></div>

    <AppHeader :ui="ui" :workbook="workbookState.workbook.value">
      <template #controls>
        <LocaleSwitcher
          :ui="ui"
          :locale="locale"
          :locale-options="localeOptions"
          @update:locale="locale = $event"
        />
      </template>
    </AppHeader>

    <main class="workspace">
      <section class="content">
        <WorkbookFeaturePanel
          :ui="ui"
          :state="workbookState"
          :error-message="errorMessage"
        />

        <ConversationFeaturePanel
          :ui="ui"
          :messages="conversation.chatMessages.value"
          :question="conversation.question.value"
          :mode="conversation.chatMode.value"
          :chat-busy="conversation.chatBusy.value"
          @clarification-select="conversation.handleClarificationSelect"
          @update:question="conversation.question.value = $event"
          @update:mode="conversation.chatMode.value = $event"
          @submit="conversation.submitQuestion"
          @stop="conversation.stopStreaming"
        />
      </section>
    </main>
  </div>
</template>
