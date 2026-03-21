<script setup lang="ts">
import type { UiMessages } from "../../../i18n/messages";
import type { WorkbookState } from "../composables/useWorkbook";
import WorkbookPreviewPanel from "../../../components/WorkbookPreviewPanel.vue";

defineProps<{
  ui: UiMessages;
  state: WorkbookState;
  errorMessage: string;
}>();
</script>

<template>
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
</template>
