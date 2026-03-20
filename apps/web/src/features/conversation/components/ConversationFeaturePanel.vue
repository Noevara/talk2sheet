<script setup lang="ts">
import type { UiMessages } from "../../../i18n/messages";
import type { ChatMessage, ChatMode } from "../../../types";
import ConversationWorkspace from "../../../components/ConversationWorkspace.vue";

defineProps<{
  ui: UiMessages;
  messages: ChatMessage[];
  question: string;
  mode: ChatMode;
  chatBusy: boolean;
  errorMessage: string;
}>();

const emit = defineEmits<{
  clarificationSelect: [payload: { messageId: string; value: string }];
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
    :mode="mode"
    :chat-busy="chatBusy"
    :error-message="errorMessage"
    @clarification-select="emit('clarificationSelect', $event)"
    @update:question="emit('update:question', $event)"
    @update:mode="emit('update:mode', $event)"
    @submit="emit('submit')"
    @stop="emit('stop')"
  />
</template>
