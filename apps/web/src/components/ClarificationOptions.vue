<script setup lang="ts">
import { computed } from "vue";

import type { ClarificationPayload } from "../types";

const props = defineProps<{
  clarification: ClarificationPayload;
  title: string;
  applyLabel: string;
  reasonPrefix: string;
}>();

const emit = defineEmits<{
  select: [value: string];
}>();

const clarificationKind = computed<"sheet" | "column">(() => {
  return props.clarification.kind === "sheet_resolution" ? "sheet" : "column";
});

const reasonText = computed(() => {
  const reason = props.clarification.reason.trim();
  if (!reason) {
    return props.reasonPrefix.trim();
  }
  return `${props.reasonPrefix}${reason}`;
});
</script>

<template>
  <div class="clarification-card" :class="`clarification-card-${clarificationKind}`">
    <div class="clarification-title">{{ title }}</div>
    <p class="clarification-reason">{{ reasonText }}</p>

    <div class="clarification-options">
      <button
        v-for="option in props.clarification.options"
        :key="option.value"
        type="button"
        class="clarification-option"
        @click="emit('select', option.value)"
      >
        <strong>{{ option.label }}</strong>
        <span v-if="option.description">{{ option.description }}</span>
        <small>{{ applyLabel }}</small>
      </button>
    </div>
  </div>
</template>

<style scoped>
.clarification-card {
  margin-top: 1rem;
  padding: 0.92rem 0.95rem;
  border-radius: 18px;
}

.clarification-card-column {
  border: 1px solid rgba(29, 95, 133, 0.14);
  background: linear-gradient(180deg, rgba(29, 95, 133, 0.08), rgba(255, 255, 255, 0.88));
}

.clarification-card-sheet {
  border: 1px solid rgba(200, 92, 60, 0.2);
  background: linear-gradient(180deg, rgba(200, 92, 60, 0.1), rgba(255, 255, 255, 0.9));
}

.clarification-title {
  font-size: 0.72rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: rgba(23, 50, 84, 0.56);
}

.clarification-reason {
  margin: 0.38rem 0 0;
  color: #173254;
  line-height: 1.6;
}

.clarification-options {
  display: grid;
  gap: 0.65rem;
  margin-top: 0.82rem;
}

.clarification-option {
  display: grid;
  gap: 0.16rem;
  text-align: left;
  padding: 0.78rem 0.84rem;
  border-radius: 16px;
  border: 1px solid rgba(29, 95, 133, 0.16);
  background: rgba(255, 255, 255, 0.92);
  color: #173254;
  transition: transform 150ms ease, border-color 150ms ease, background 150ms ease;
}

.clarification-option strong {
  font-size: 0.94rem;
}

.clarification-option span,
.clarification-option small {
  color: rgba(23, 50, 84, 0.68);
}

.clarification-option small {
  font-size: 0.76rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}

.clarification-option:hover {
  transform: translateY(-1px);
  border-color: rgba(29, 95, 133, 0.28);
  background: rgba(255, 255, 255, 0.98);
}

.clarification-card-sheet .clarification-option:hover {
  border-color: rgba(200, 92, 60, 0.3);
}
</style>
