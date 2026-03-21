<script setup lang="ts">
import { computed } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import type { WorkbookState } from "../composables/useWorkbook";
import WorkbookPreviewPanel from "../../../components/WorkbookPreviewPanel.vue";

const props = defineProps<{
  ui: UiMessages;
  state: WorkbookState;
  errorMessage: string;
  batchRunBusy: boolean;
  batchQuestion: string;
}>();

const emit = defineEmits<{
  runBatch: [sheetIndexes: number[]];
}>();

const workbookSheets = computed(() => props.state.workbook.value?.sheets || []);
const batchSelection = computed(() => props.state.batchSelectedSheetIndexes.value);
const recentSelection = computed(() => props.state.recentBatchSheetIndexes.value);
const canUseRecentSelection = computed(() => recentSelection.value.length >= 1 && !props.batchRunBusy);
const recentSelectionLabel = computed(() => {
  if (!recentSelection.value.length) {
    return "";
  }
  const sheetNameMap = new Map(workbookSheets.value.map((sheet) => [sheet.index, sheet.name || `#${sheet.index}`]));
  return recentSelection.value
    .map((sheetIndex) => `${sheetNameMap.get(sheetIndex) || `#${sheetIndex}`} (#${sheetIndex})`)
    .join(", ");
});
const canRunBatch = computed(() => {
  return Boolean(
    workbookSheets.value.length >= 2 &&
      batchSelection.value.length >= 1 &&
      props.batchQuestion.trim() &&
      !props.batchRunBusy,
  );
});

function toggleSheetSelection(sheetIndex: number, checked: boolean): void {
  const next = checked
    ? [...batchSelection.value, sheetIndex]
    : batchSelection.value.filter((item) => item !== sheetIndex);
  props.state.setBatchSheetSelection(next);
}

function selectAllSheets(): void {
  props.state.setBatchSheetSelection(workbookSheets.value.map((sheet) => sheet.index));
}

function invertSheetSelection(): void {
  props.state.invertBatchSheetSelection();
}

function applyRecentSelection(): void {
  props.state.applyRecentBatchSelection();
}

function clearSheetSelection(): void {
  props.state.setBatchSheetSelection([]);
}

function runBatchAnalysis(): void {
  const selected = [...batchSelection.value];
  props.state.rememberRecentBatchSelection(selected);
  emit("runBatch", selected);
}
</script>

<template>
  <div class="workbook-feature-stack">
    <WorkbookPreviewPanel
      :ui="ui"
      :workbook="state.workbook.value"
      :preview="state.preview.value"
      :selected-sheet-index="state.selectedSheetIndex.value"
      :pending-sheet-override="state.pendingSheetOverride.value"
      :active-sheet-label="state.activeSheetLabel.value"
      :show-active-sheet-name-pill="state.showActiveSheetNamePill.value"
      :upload-busy="state.uploadBusy.value"
      :preview-busy="state.previewBusy.value"
      :workbook-overview-busy="state.workbookOverviewBusy.value"
      :workbook-overview-error="state.workbookOverviewError.value"
      :error-message="errorMessage"
      @upload="state.handleFileSelection"
      @select-sheet="state.handleManualSheetSelect"
    />

    <section v-if="workbookSheets.length >= 2" class="panel batch-panel">
      <div class="batch-panel-head">
        <h3>{{ ui.batchPanelTitle }}</h3>
        <p>{{ ui.batchPanelHint }}</p>
        <p v-if="recentSelectionLabel" class="batch-recent-hint">
          {{ ui.batchRecentSelectionHintLabel }}: {{ recentSelectionLabel }}
        </p>
      </div>

      <div class="batch-sheet-grid">
        <label v-for="sheet in workbookSheets" :key="`batch-sheet-${sheet.index}`" class="batch-sheet-option">
          <input
            type="checkbox"
            :checked="batchSelection.includes(sheet.index)"
            @change="toggleSheetSelection(sheet.index, ($event.target as HTMLInputElement).checked)"
          />
          <span>{{ sheet.name || `#${sheet.index}` }} (#{{ sheet.index }})</span>
        </label>
      </div>

      <div class="batch-actions">
        <button type="button" class="button button-ghost" @click="selectAllSheets">
          {{ ui.batchSelectAllLabel }}
        </button>
        <button type="button" class="button button-ghost" :disabled="batchRunBusy" @click="invertSheetSelection">
          {{ ui.batchInvertLabel }}
        </button>
        <button type="button" class="button button-ghost" :disabled="!canUseRecentSelection" @click="applyRecentSelection">
          {{ ui.batchUseRecentLabel }}
        </button>
        <button type="button" class="button button-ghost" @click="clearSheetSelection">
          {{ ui.batchClearLabel }}
        </button>
        <button
          type="button"
          class="button button-primary"
          :disabled="!canRunBatch"
          @click="runBatchAnalysis"
        >
          {{ batchRunBusy ? ui.batchRunBusyLabel : ui.batchRunLabel }}
        </button>
      </div>
    </section>
  </div>
</template>

<style scoped>
.workbook-feature-stack {
  display: grid;
  gap: 0.8rem;
}

.batch-panel {
  border-radius: 18px;
  border: 1px solid rgba(18, 41, 74, 0.12);
  background: rgba(255, 255, 255, 0.9);
  padding: 0.75rem 0.85rem;
}

.batch-panel-head h3 {
  margin: 0;
  font-size: 0.95rem;
  color: #173254;
}

.batch-panel-head p {
  margin: 0.2rem 0 0;
  font-size: 0.78rem;
  color: rgba(23, 50, 84, 0.7);
}

.batch-recent-hint {
  margin-top: 0.28rem;
  font-size: 0.75rem;
  color: rgba(23, 50, 84, 0.62);
}

.batch-sheet-grid {
  margin-top: 0.56rem;
  display: grid;
  gap: 0.35rem;
}

.batch-sheet-option {
  display: inline-flex;
  align-items: center;
  gap: 0.38rem;
  font-size: 0.8rem;
  color: #173254;
}

.batch-actions {
  margin-top: 0.64rem;
  display: flex;
  flex-wrap: wrap;
  gap: 0.42rem;
}
</style>
