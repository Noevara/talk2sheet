import { computed, ref, type ComputedRef } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import { fetchPreview, uploadSpreadsheet } from "../../../lib/api";
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
}

export function useWorkbook(options: { ui: ComputedRef<UiMessages>; resetConversation: () => void }) {
  const workbook = ref<UploadedFileResponse | null>(null);
  const selectedSheetIndex = ref(1);
  const pendingSheetOverride = ref(false);
  const preview = ref<PreviewResponse | null>(null);
  const uploadBusy = ref(false);
  const previewBusy = ref(false);
  const errorMessage = ref("");

  const activeSheetLabel = computed(() => {
    const fallbackName =
      workbook.value?.sheets.find((sheet) => sheet.index === selectedSheetIndex.value)?.name || preview.value?.sheet_name || "";
    return formatSheetName(fallbackName, selectedSheetIndex.value);
  });

  const showActiveSheetNamePill = computed(() => {
    return Boolean(workbook.value && preview.value && activeSheetLabel.value !== workbook.value.file_name);
  });

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
      options.resetConversation();
      const firstSheet = uploaded.sheets[0]?.index || 1;
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
    errorMessage.value = "";
  }

  function snapshotState(): WorkbookSnapshot {
    return {
      workbook: workbook.value,
      selectedSheetIndex: selectedSheetIndex.value,
      pendingSheetOverride: pendingSheetOverride.value,
      preview: preview.value,
    };
  }

  function clearState(): void {
    workbook.value = null;
    selectedSheetIndex.value = 1;
    pendingSheetOverride.value = false;
    preview.value = null;
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
    errorMessage,
    activeSheetLabel,
    showActiveSheetNamePill,
    restoreState,
    snapshotState,
    clearState,
    revalidateRestoredState,
    loadPreview,
    handleManualSheetSelect,
    clearPendingSheetOverride,
    handleFileSelection,
  };
}

export type WorkbookState = ReturnType<typeof useWorkbook>;
