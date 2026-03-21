<script setup lang="ts">
import type { UiMessages } from "../i18n/messages";
import type { ChatMessage, ChatMode } from "../types";
import ConversationComposer from "./ConversationComposer.vue";
import ConversationPanel from "./ConversationPanel.vue";

defineProps<{
  ui: UiMessages;
  messages: ChatMessage[];
  question: string;
  composerFocusToken: number;
  mode: ChatMode;
  chatBusy: boolean;
  errorMessage: string;
}>();

const emit = defineEmits<{
  clarificationSelect: [payload: { messageId: string; value: string }];
  followupSelect: [question: string];
  continueNextStep: [payload: { messageId: string }];
  "update:question": [value: string];
  "update:mode": [value: ChatMode];
  submit: [];
  stop: [];
}>();
</script>

<template>
  <section class="panel conversation-panel">
    <div class="panel-head panel-head-tight">
      <div class="panel-title-stack">
        <h2>{{ ui.chatTitle }}</h2>
      </div>
    </div>

    <ConversationPanel
      :messages="messages"
      :ui="ui"
      @clarification-select="emit('clarificationSelect', $event)"
      @followup-select="emit('followupSelect', $event)"
      @continue-next-step="emit('continueNextStep', $event)"
    />

    <p v-if="errorMessage" class="error-banner">{{ errorMessage }}</p>

    <ConversationComposer
      :ui="ui"
      :question="question"
      :focus-token="composerFocusToken"
      :mode="mode"
      :chat-busy="chatBusy"
      @update:question="emit('update:question', $event)"
      @update:mode="emit('update:mode', $event)"
      @submit="emit('submit')"
      @stop="emit('stop')"
    />
  </section>
</template>
