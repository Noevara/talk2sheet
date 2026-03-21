<script setup lang="ts">
import { nextTick, ref, watch } from "vue";

import type { UiMessages } from "../i18n/messages";
import type { ChatMessage } from "../types";
import ConversationMessage from "./ConversationMessage.vue";

const props = defineProps<{
  messages: ChatMessage[];
  ui: UiMessages;
}>();

const emit = defineEmits<{
  clarificationSelect: [payload: { messageId: string; value: string }];
  followupSelect: [question: string];
  continueNextStep: [payload: { messageId: string }];
}>();

const viewport = ref<HTMLElement | null>(null);

watch(
  () => props.messages,
  async () => {
    await nextTick();
    if (viewport.value) {
      viewport.value.scrollTop = viewport.value.scrollHeight;
    }
  },
  { deep: true },
);
</script>

<template>
  <div ref="viewport" class="conversation-stream panel-surface">
    <div v-if="!props.messages.length" class="panel-empty panel-empty-chat">
      {{ props.ui.chatEmpty }}
    </div>

    <ConversationMessage
      v-for="message in props.messages"
      :key="message.id"
      :message="message"
      :show-debug="false"
      :ui="props.ui"
      @clarification-select="emit('clarificationSelect', { messageId: message.id, value: $event })"
      @followup-select="emit('followupSelect', $event)"
      @continue-next-step="emit('continueNextStep', { messageId: message.id })"
    />
  </div>
</template>
