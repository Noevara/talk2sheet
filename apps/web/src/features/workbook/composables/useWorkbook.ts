import { computed, ref, type ComputedRef } from "vue";

import type { UiMessages } from "../../../i18n/messages";
import { fetchPreview, uploadSpreadsheet } from "../../../lib/api";
import type { PreviewResponse, UploadedFileResponse } from "../../../types";

function extractError(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) {
    return `${fallback}: ${error.message}`;
  }
  return fallback;
}

function formatSheetName(sheetName: string, sheetIndex: number): string {
  const normalized = sheetName.trim();
  return normalized || `#${sheetIndex}`;
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
      errorMessage.value = extractError(error, options.ui.value.previewError);
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
      const uploaded = await uploadSpreadsheet(file);
      workbook.value = uploaded;
      preview.value = null;
      pendingSheetOverride.value = false;
      options.resetConversation();
      const firstSheet = uploaded.sheets[0]?.index || 1;
      await loadPreview(firstSheet, { resetConversation: false });
    } catch (error) {
      errorMessage.value = extractError(error, options.ui.value.uploadError);
    } finally {
      uploadBusy.value = false;
      target.value = "";
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
    loadPreview,
    handleManualSheetSelect,
    clearPendingSheetOverride,
    handleFileSelection,
  };
}

export type WorkbookState = ReturnType<typeof useWorkbook>;
