<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";

import type { UiMessages } from "../i18n/messages";
import type { ChatMessage } from "../types";
import ConversationMessage from "./ConversationMessage.vue";

const props = defineProps<{
  messages: ChatMessage[];
  ui: UiMessages;
}>();

const emit = defineEmits<{
  clarificationSelect: [payload: { messageId: string; value: string }];
}>();

const viewport = ref<HTMLElement | null>(null);

const labels = computed(() => ({
  userLabel: props.ui.userLabel,
  assistantLabel: props.ui.assistantLabel,
  streamingLabel: props.ui.streamingLabel,
  scopeLabel: props.ui.scopeLabel,
  metadataLabel: props.ui.metadataLabel,
  sheetRoutingLabel: props.ui.sheetRoutingLabel,
  requestedSheetLabel: props.ui.requestedSheetLabel,
  resolvedSheetLabel: props.ui.resolvedSheetLabel,
  routingMethodLabel: props.ui.routingMethodLabel,
  routingChangedLabel: props.ui.routingChangedLabel,
  routingMethodSingleSheetLabel: props.ui.routingMethodSingleSheetLabel,
  routingMethodExplicitLabel: props.ui.routingMethodExplicitLabel,
  routingMethodClarificationLabel: props.ui.routingMethodClarificationLabel,
  routingMethodManualOverrideLabel: props.ui.routingMethodManualOverrideLabel,
  routingMethodFollowupLabel: props.ui.routingMethodFollowupLabel,
  routingMethodAutoLabel: props.ui.routingMethodAutoLabel,
  routingMethodRequestedLabel: props.ui.routingMethodRequestedLabel,
  conclusionLabel: props.ui.conclusionLabel,
  evidenceLabel: props.ui.evidenceLabel,
  riskNoteLabel: props.ui.riskNoteLabel,
  clarificationLabel: props.ui.clarificationLabel,
  clarificationApplyLabel: props.ui.clarificationApplyLabel,
  pipelineLabel: props.ui.pipelineLabel,
  selectionPlanLabel: props.ui.selectionPlanLabel,
  transformPlanLabel: props.ui.transformPlanLabel,
  detailRowsLabel: props.ui.detailRowsLabel,
  resultTableLabel: props.ui.resultTableLabel,
  forecastLabel: props.ui.forecastLabel,
  forecastBadgeLabel: props.ui.forecastBadgeLabel,
  forecastTargetLabel: props.ui.forecastTargetLabel,
  forecastEstimateLabel: props.ui.forecastEstimateLabel,
  forecastRangeLabel: props.ui.forecastRangeLabel,
  forecastModelLabel: props.ui.forecastModelLabel,
  forecastHistoryLabel: props.ui.forecastHistoryLabel,
  forecastHistoryPointsLabel: props.ui.forecastHistoryPointsLabel,
  forecastGrainLabel: props.ui.forecastGrainLabel,
  forecastHorizonLabel: props.ui.forecastHorizonLabel,
  forecastTableLabel: props.ui.forecastTableLabel,
  forecastModelLinearLabel: props.ui.forecastModelLinearLabel,
  forecastModelSmoothingLabel: props.ui.forecastModelSmoothingLabel,
  forecastGrainDayLabel: props.ui.forecastGrainDayLabel,
  forecastGrainWeekLabel: props.ui.forecastGrainWeekLabel,
  forecastGrainMonthLabel: props.ui.forecastGrainMonthLabel,
  chartLabel: props.ui.chartLabel,
  noChartData: props.ui.noChartData,
}));

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
      :labels="labels"
      @clarification-select="emit('clarificationSelect', { messageId: message.id, value: $event })"
    />
  </div>
</template>
