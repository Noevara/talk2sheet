import type { Locale } from "../types";
import type { WorkbookSnapshot } from "../features/workbook/composables/useWorkbook";
import type { ConversationSnapshot } from "../features/conversation/composables/useConversation";

const STORAGE_KEY = "talk2sheet.app-session.v1";
const STORAGE_VERSION = 1;

export interface PersistedAppSession {
  version: number;
  locale: Locale;
  workbook: WorkbookSnapshot;
  conversation: ConversationSnapshot;
}

function normalizeConversation(snapshot: ConversationSnapshot): ConversationSnapshot {
  return {
    ...snapshot,
    chatMessages: snapshot.chatMessages.map((message) => ({
      ...message,
      status: message.status === "streaming" ? "done" : message.status,
    })),
  };
}

export function loadPersistedSession(): PersistedAppSession | null {
  if (typeof localStorage === "undefined") {
    return null;
  }
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as PersistedAppSession;
    if (parsed.version !== STORAGE_VERSION) {
      return null;
    }
    if (!parsed.workbook || !parsed.conversation) {
      return null;
    }
    return {
      ...parsed,
      conversation: normalizeConversation(parsed.conversation),
    };
  } catch {
    return null;
  }
}

export function savePersistedSession(session: PersistedAppSession): void {
  if (typeof localStorage === "undefined") {
    return;
  }
  localStorage.setItem(STORAGE_KEY, JSON.stringify({
    ...session,
    version: STORAGE_VERSION,
    conversation: normalizeConversation(session.conversation),
  }));
}

export function clearPersistedSession(): void {
  if (typeof localStorage === "undefined") {
    return;
  }
  localStorage.removeItem(STORAGE_KEY);
}
