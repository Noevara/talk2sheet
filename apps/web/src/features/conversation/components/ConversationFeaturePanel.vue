<script setup lang="ts">
import type { UiMessages } from "../../../i18n/messages";
import type { ChatMessage, ChatMode } from "../../../types";
import ConversationWorkspace from "../../../components/ConversationWorkspace.vue";

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
  <ConversationWorkspace
    :ui="ui"
    :messages="messages"
    :question="question"
    :composer-focus-token="composerFocusToken"
    :mode="mode"
    :chat-busy="chatBusy"
    :error-message="errorMessage"
    @clarification-select="emit('clarificationSelect', $event)"
    @followup-select="emit('followupSelect', $event)"
    @continue-next-step="emit('continueNextStep', $event)"
    @update:question="emit('update:question', $event)"
    @update:mode="emit('update:mode', $event)"
    @submit="emit('submit')"
    @stop="emit('stop')"
  />
</template>
