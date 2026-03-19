<script setup lang="ts">
import type { UiMessages } from "../i18n/messages";
import type { PreviewResponse, UploadedFileResponse } from "../types";
import DataTable from "./DataTable.vue";

const props = defineProps<{
  ui: UiMessages;
  workbook: UploadedFileResponse | null;
  preview: PreviewResponse | null;
  selectedSheetIndex: number;
  pendingSheetOverride: boolean;
  activeSheetLabel: string;
  showActiveSheetNamePill: boolean;
  uploadBusy: boolean;
  previewBusy: boolean;
  errorMessage: string;
}>();

const emit = defineEmits<{
  upload: [event: Event];
  selectSheet: [sheetIndex: number];
}>();

function formatSheetName(sheetName: string, sheetIndex: number): string {
  const normalized = sheetName.trim();
  return normalized || `#${sheetIndex}`;
}
</script>

<template>
  <section class="panel preview-panel">
    <div class="panel-head panel-head-inline panel-head-tight">
      <div class="panel-title-stack">
        <h2>{{ props.ui.previewTitle }}</h2>
        <p v-if="!props.workbook">{{ props.ui.uploadHint }}</p>
        <div v-if="props.preview" class="context-pills">
          <span v-if="props.showActiveSheetNamePill" class="context-pill" :title="props.activeSheetLabel">{{ props.activeSheetLabel }}</span>
          <span class="context-pill">#{{ props.selectedSheetIndex }}</span>
          <span v-if="props.pendingSheetOverride" class="context-pill context-pill-emphasis">{{ props.ui.sheetOverridePendingLabel }}</span>
        </div>
      </div>
      <div class="preview-toolbar">
        <label class="toolbar-upload button button-ghost">
          <input type="file" accept=".xlsx,.xls,.csv" @change="emit('upload', $event)" />
          <span>{{ props.uploadBusy ? props.ui.uploading : props.ui.uploadButton }}</span>
        </label>

        <div v-if="props.preview" class="preview-stats">
          <div>
            <span>{{ props.ui.totalRowsLabel }}</span>
            <strong>{{ props.preview.total_rows ?? props.preview.rows.length }}</strong>
          </div>
          <div>
            <span>{{ props.ui.previewRowsLabel }}</span>
            <strong>{{ props.preview.preview_row_count ?? props.preview.rows.length }}</strong>
          </div>
          <div>
            <span>{{ props.ui.colsLoaded }}</span>
            <strong>{{ props.preview.columns.length }}</strong>
          </div>
        </div>
      </div>
    </div>

    <div v-if="props.workbook" class="preview-dataset-band">
      <div class="workbook-meta workbook-meta-inline">
        <div class="workbook-file">
          <strong :title="props.workbook.file_name">{{ props.workbook.file_name }}</strong>
          <span>{{ props.workbook.file_type }}</span>
        </div>
      </div>

      <div class="sheet-section sheet-section-inline">
        <h3>{{ props.ui.workbookTitle }}</h3>
        <div class="sheet-list sheet-list-inline">
          <button
            v-for="sheet in props.workbook.sheets"
            :key="sheet.index"
            type="button"
            class="sheet-chip"
            :class="{ 'sheet-chip-active': props.selectedSheetIndex === sheet.index }"
            @click="emit('selectSheet', sheet.index)"
          >
            <span :title="formatSheetName(sheet.name, sheet.index)">{{ formatSheetName(sheet.name, sheet.index) }}</span>
            <small>#{{ sheet.index }}</small>
          </button>
        </div>
      </div>
    </div>

    <div class="panel-surface preview-surface">
      <div v-if="!props.workbook" class="preview-empty-state">
        <label class="upload-dropzone preview-dropzone">
          <input type="file" accept=".xlsx,.xls,.csv" @change="emit('upload', $event)" />
          <span class="upload-dropzone-title">{{ props.uploadBusy ? props.ui.uploading : props.ui.uploadButton }}</span>
          <span class="upload-dropzone-subtitle">.xlsx / .xls / .csv</span>
        </label>

        <div class="preview-empty-grid">
          <div class="info-slab">
            <h3>{{ props.ui.capabilityTitle }}</h3>
            <p>{{ props.ui.capabilityBody }}</p>
          </div>

          <div class="info-slab info-slab-muted">
            <h3>{{ props.ui.outOfScopeTitle }}</h3>
            <p>{{ props.ui.outOfScopeBody }}</p>
          </div>
        </div>
      </div>
      <DataTable v-else-if="props.preview" :columns="props.preview.columns" :rows="props.preview.rows" :empty-text="props.ui.previewEmpty" />
      <div v-else class="panel-empty">
        {{ props.previewBusy ? props.ui.previewLoading : props.ui.previewEmpty }}
      </div>
    </div>

    <p v-if="props.errorMessage" class="error-banner">{{ props.errorMessage }}</p>
  </section>
</template>
