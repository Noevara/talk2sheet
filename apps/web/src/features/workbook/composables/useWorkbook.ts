import { computed, ref, type ComputedRef } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import { fetchPreview, fetchWorkbookSheets, uploadSpreadsheet } from "../../../lib/api";
import { formatPreviewError, formatUploadError, isMissingWorkbookError } from "../../../lib/errorMessages";
import type { PreviewResponse, UploadedFileResponse } from "../../../types";

function formatSheetName(sheetName: string, sheetIndex: number): string {
  const normalized = sheetName.trim();
  return normalized || `#${sheetIndex}`;
}

function isSupportedFile(file: File): boolean {
  return /\.(xlsx|xls|csv)$/i.test(file.name);
}

export interface WorkbookSnapshot {
  workbook: UploadedFileResponse | null;
  selectedSheetIndex: number;
  pendingSheetOverride: boolean;
  preview: PreviewResponse | null;
  batchSelectedSheetIndexes: number[];
  recentBatchSheetIndexes?: number[];
}

export function useWorkbook(options: { ui: ComputedRef<UiMessages>; resetConversation: () => void }) {
  const workbook = ref<UploadedFileResponse | null>(null);
  const selectedSheetIndex = ref(1);
  const pendingSheetOverride = ref(false);
  const preview = ref<PreviewResponse | null>(null);
  const batchSelectedSheetIndexes = ref<number[]>([]);
  const recentBatchSheetIndexes = ref<number[]>([]);
  const uploadBusy = ref(false);
  const previewBusy = ref(false);
  const workbookOverviewBusy = ref(false);
  const workbookOverviewError = ref("");
  const errorMessage = ref("");

  const activeSheetLabel = computed(() => {
    const fallbackName =
      workbook.value?.sheets.find((sheet) => sheet.index === selectedSheetIndex.value)?.name || preview.value?.sheet_name || "";
    return formatSheetName(fallbackName, selectedSheetIndex.value);
  });

  const showActiveSheetNamePill = computed(() => {
    return Boolean(workbook.value && preview.value && activeSheetLabel.value !== workbook.value.file_name);
  });

  function availableBatchSheetIndexes(): number[] {
    return (workbook.value?.sheets || [])
      .map((sheet) => Number(sheet.index || 0))
      .filter((sheetIndex) => Number.isFinite(sheetIndex) && sheetIndex > 0);
  }

  function normalizeSheetIndexList(sheetIndexes: number[], availableSheetIndexes: number[]): number[] {
    return Array.from(
      new Set(
        sheetIndexes
          .map((item) => Number(item || 0))
          .filter((item) => item > 0 && availableSheetIndexes.includes(item)),
      ),
    );
  }

  function normalizeBatchSelection(options?: { defaultToAll?: boolean }): void {
    const availableSheetIndexes = availableBatchSheetIndexes();
    if (!availableSheetIndexes.length) {
      batchSelectedSheetIndexes.value = [];
      recentBatchSheetIndexes.value = [];
      return;
    }
    const selected = normalizeSheetIndexList(batchSelectedSheetIndexes.value, availableSheetIndexes);
    batchSelectedSheetIndexes.value =
      options?.defaultToAll && !selected.length ? [...availableSheetIndexes] : selected;
    recentBatchSheetIndexes.value = normalizeSheetIndexList(recentBatchSheetIndexes.value, availableSheetIndexes);
  }

  async function refreshWorkbookSummary(
    fileId: string,
    refreshOptions?: { defaultBatchSelection?: boolean },
  ): Promise<void> {
    workbookOverviewBusy.value = true;
    workbookOverviewError.value = "";
    try {
      const summary = await fetchWorkbookSheets(fileId);
      if (workbook.value && workbook.value.file_id !== fileId) {
        return;
      }
      workbook.value = summary;
      normalizeBatchSelection({ defaultToAll: refreshOptions?.defaultBatchSelection ?? false });
    } catch (error) {
      workbookOverviewError.value = formatPreviewError(error, options.ui.value);
    } finally {
      workbookOverviewBusy.value = false;
    }
  }

  async function loadPreview(
    sheetIndex: number,
    requestOptions?: { markManualOverride?: boolean; resetConversation?: boolean },
  ): Promise<void> {
    if (!workbook.value) {
      return;
    }

    if (requestOptions?.markManualOverride) {
      pendingSheetOverride.value = true;
    }

    if ((requestOptions?.resetConversation ?? true) && selectedSheetIndex.value !== sheetIndex) {
      options.resetConversation();
    }

    previewBusy.value = true;
    errorMessage.value = "";
    try {
      selectedSheetIndex.value = sheetIndex;
      preview.value = await fetchPreview(workbook.value.file_id, sheetIndex);
    } catch (error) {
      errorMessage.value = formatPreviewError(error, options.ui.value);
    } finally {
      previewBusy.value = false;
    }
  }

  async function handleManualSheetSelect(sheetIndex: number): Promise<void> {
    await loadPreview(sheetIndex, { markManualOverride: true });
  }

  function clearPendingSheetOverride(): void {
    pendingSheetOverride.value = false;
  }

  function setBatchSheetSelection(sheetIndexes: number[]): void {
    batchSelectedSheetIndexes.value = sheetIndexes;
    normalizeBatchSelection();
  }

  function invertBatchSheetSelection(): void {
    const available = availableBatchSheetIndexes();
    if (!available.length) {
      return;
    }
    const selectedSet = new Set(batchSelectedSheetIndexes.value);
    const inverted = available.filter((sheetIndex) => !selectedSet.has(sheetIndex));
    setBatchSheetSelection(inverted);
  }

  function applyRecentBatchSelection(): void {
    if (!recentBatchSheetIndexes.value.length) {
      return;
    }
    setBatchSheetSelection([...recentBatchSheetIndexes.value]);
  }

  function rememberRecentBatchSelection(sheetIndexes: number[]): void {
    const available = availableBatchSheetIndexes();
    if (!available.length) {
      recentBatchSheetIndexes.value = [];
      return;
    }
    const normalized = normalizeSheetIndexList(sheetIndexes, available);
    if (!normalized.length) {
      return;
    }
    recentBatchSheetIndexes.value = normalized;
  }

  async function handleFileSelection(event: Event): Promise<void> {
    const target = event.target as HTMLInputElement;
    const file = target.files?.[0];
    if (!file) {
      return;
    }

    uploadBusy.value = true;
    errorMessage.value = "";

    try {
      if (!isSupportedFile(file)) {
        throw new Error(options.ui.value.uploadInvalidFileError);
      }
      const uploaded = await uploadSpreadsheet(file);
      workbook.value = uploaded;
      preview.value = null;
      pendingSheetOverride.value = false;
      workbookOverviewError.value = "";
      options.resetConversation();
      await refreshWorkbookSummary(uploaded.file_id, { defaultBatchSelection: true });
      const firstSheet = workbook.value?.sheets[0]?.index || uploaded.sheets[0]?.index || 1;
      await loadPreview(firstSheet, { resetConversation: false });
    } catch (error) {
      if (error instanceof Error && error.message === options.ui.value.uploadInvalidFileError) {
        errorMessage.value = error.message;
      } else {
        errorMessage.value = formatUploadError(error, options.ui.value);
      }
    } finally {
      uploadBusy.value = false;
      target.value = "";
    }
  }

  function restoreState(snapshot: WorkbookSnapshot): void {
    workbook.value = snapshot.workbook;
    selectedSheetIndex.value = snapshot.selectedSheetIndex || 1;
    pendingSheetOverride.value = snapshot.pendingSheetOverride;
    preview.value = snapshot.preview;
    batchSelectedSheetIndexes.value = Array.isArray(snapshot.batchSelectedSheetIndexes)
      ? snapshot.batchSelectedSheetIndexes.map((item) => Number(item || 0)).filter((item) => item > 0)
      : [];
    recentBatchSheetIndexes.value = Array.isArray(snapshot.recentBatchSheetIndexes)
      ? snapshot.recentBatchSheetIndexes.map((item) => Number(item || 0)).filter((item) => item > 0)
      : [];
    normalizeBatchSelection();
    workbookOverviewError.value = "";
    errorMessage.value = "";
  }

  function snapshotState(): WorkbookSnapshot {
    return {
      workbook: workbook.value,
      selectedSheetIndex: selectedSheetIndex.value,
      pendingSheetOverride: pendingSheetOverride.value,
      preview: preview.value,
      batchSelectedSheetIndexes: [...batchSelectedSheetIndexes.value],
      recentBatchSheetIndexes: [...recentBatchSheetIndexes.value],
    };
  }

  function clearState(): void {
    workbook.value = null;
    selectedSheetIndex.value = 1;
    pendingSheetOverride.value = false;
    preview.value = null;
    batchSelectedSheetIndexes.value = [];
    recentBatchSheetIndexes.value = [];
    workbookOverviewBusy.value = false;
    workbookOverviewError.value = "";
    errorMessage.value = "";
  }

  async function revalidateRestoredState(): Promise<boolean> {
    if (!workbook.value) {
      return true;
    }

    previewBusy.value = true;
    errorMessage.value = "";
    try {
      preview.value = await fetchPreview(workbook.value.file_id, selectedSheetIndex.value);
      await refreshWorkbookSummary(workbook.value.file_id);
      return true;
    } catch (error) {
      if (isMissingWorkbookError(error)) {
        clearState();
        options.resetConversation();
        errorMessage.value = options.ui.value.restoreSessionExpiredError;
        return false;
      }
      errorMessage.value = formatPreviewError(error, options.ui.value);
      return false;
    } finally {
      previewBusy.value = false;
    }
  }

  return {
    workbook,
    selectedSheetIndex,
    pendingSheetOverride,
    preview,
    uploadBusy,
    previewBusy,
    workbookOverviewBusy,
    workbookOverviewError,
    errorMessage,
    activeSheetLabel,
    showActiveSheetNamePill,
    restoreState,
    snapshotState,
    clearState,
    revalidateRestoredState,
    refreshWorkbookSummary,
    loadPreview,
    handleManualSheetSelect,
    clearPendingSheetOverride,
    handleFileSelection,
    batchSelectedSheetIndexes,
    recentBatchSheetIndexes,
    setBatchSheetSelection,
    invertBatchSheetSelection,
    applyRecentBatchSelection,
    rememberRecentBatchSelection,
  };
}

export type WorkbookState = ReturnType<typeof useWorkbook>;
