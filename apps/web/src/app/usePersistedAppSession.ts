import { onMounted, ref, watch, type Ref } from "vue";

import { clearPersistedSession, loadPersistedSession, savePersistedSession } from "../lib/sessionPersistence";
import type { Locale } from "../types";
import type { ConversationState } from "../features/conversation/composables/useConversation";
import type { WorkbookState } from "../features/workbook/composables/useWorkbook";

export function usePersistedAppSession(options: {
  locale: Ref<Locale>;
  workbookState: WorkbookState;
  conversation: ConversationState;
}) {
  const persistenceReady = ref(false);

  onMounted(async () => {
    const restored = loadPersistedSession();
    if (restored) {
      options.locale.value = restored.locale;
      options.workbookState.restoreState(restored.workbook);
      options.conversation.restoreState(restored.conversation);

      if (restored.workbook.workbook) {
        const restoredValid = await options.workbookState.revalidateRestoredState();
        if (!restoredValid && !options.workbookState.workbook.value) {
          clearPersistedSession();
        }
      }
    }
    persistenceReady.value = true;
  });

  watch(
    [
      options.locale,
      () => options.workbookState.snapshotState(),
      () => options.conversation.snapshotState(),
    ],
    () => {
      if (!persistenceReady.value) {
        return;
      }
      const workbookSnapshot = options.workbookState.snapshotState();
      if (!workbookSnapshot.workbook) {
        clearPersistedSession();
        return;
      }
      savePersistedSession({
        version: 1,
        locale: options.locale.value,
        workbook: workbookSnapshot,
        conversation: options.conversation.snapshotState(),
      });
    },
    { deep: true },
  );

  return {
    persistenceReady,
  };
}
