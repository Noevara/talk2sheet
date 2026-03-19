from __future__ import annotations

import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Any

from .store_types import ConversationSession, EnsureSessionResult


class InMemorySessionStore:
    def __init__(self, *, max_sessions: int, max_turns: int) -> None:
        self.max_sessions = max(1, int(max_sessions))
        self.max_turns = max(1, int(max_turns))
        self._sessions: OrderedDict[str, ConversationSession] = OrderedDict()
        self._lock = Lock()

    def ensure_session(
        self,
        *,
        conversation_id: str | None,
        file_id: str,
        sheet_index: int,
        locale: str,
    ) -> EnsureSessionResult:
        with self._lock:
            normalized_id = str(conversation_id or "").strip() or str(uuid.uuid4())
            session = self._sessions.get(normalized_id)
            reset = False

            if session is None:
                session = ConversationSession(
                    conversation_id=normalized_id,
                    file_id=file_id,
                    sheet_index=int(sheet_index),
                    locale=locale,
                    sheet_name="",
                )
                self._sessions[normalized_id] = session
            elif session.file_id != file_id:
                session.file_id = file_id
                session.sheet_index = int(sheet_index)
                session.sheet_name = ""
                session.locale = locale
                session.turns = []
                session.updated_at = time.time()
                reset = True
            else:
                session.sheet_index = int(sheet_index)
                session.locale = locale

            self._sessions.move_to_end(normalized_id)
            evicted_conversation_ids: list[str] = []
            while len(self._sessions) > self.max_sessions:
                evicted_conversation_id, _session = self._sessions.popitem(last=False)
                evicted_conversation_ids.append(evicted_conversation_id)

            return EnsureSessionResult(
                session=session,
                reset=reset,
                evicted_conversation_ids=evicted_conversation_ids,
            )

    def append_turn(self, session: ConversationSession, turn_summary: dict[str, Any]) -> None:
        with self._lock:
            session.turns.append(turn_summary)
            session.turns = session.turns[-self.max_turns :]
            session.updated_at = time.time()
            self._sessions[session.conversation_id] = session
            self._sessions.move_to_end(session.conversation_id)

    def get(self, conversation_id: str) -> ConversationSession | None:
        with self._lock:
            return self._sessions.get(str(conversation_id))

    def clear(self) -> list[str]:
        with self._lock:
            conversation_ids = list(self._sessions.keys())
            self._sessions.clear()
            return conversation_ids


@dataclass
class _CachedDataframeEntry:
    cache_token: str
    dataframe: Any
    sheet_name: str
    expires_at: float | None


class InMemoryDataframeCache:
    def __init__(self, *, ttl_seconds: int, max_entries: int) -> None:
        self.ttl_seconds = max(0, int(ttl_seconds))
        self.max_entries = max(1, int(max_entries))
        self._entries: OrderedDict[tuple[str, str], _CachedDataframeEntry] = OrderedDict()
        self._lock = Lock()

    def get(
        self,
        conversation_id: str,
        *,
        cache_key: str,
        cache_token: str,
    ) -> tuple[Any, str] | None:
        with self._lock:
            now = time.time()
            self._purge_expired(now)
            entry_key = (str(conversation_id), str(cache_key))
            entry = self._entries.get(entry_key)
            if entry is None:
                return None
            if entry.cache_token != cache_token:
                return None
            if self.ttl_seconds > 0:
                entry.expires_at = now + self.ttl_seconds
            self._entries.move_to_end(entry_key)
            return entry.dataframe, entry.sheet_name

    def set(
        self,
        conversation_id: str,
        *,
        cache_key: str,
        cache_token: str,
        dataframe: Any,
        sheet_name: str,
    ) -> None:
        with self._lock:
            now = time.time()
            self._purge_expired(now)
            entry_key = (str(conversation_id), str(cache_key))
            self._entries[entry_key] = _CachedDataframeEntry(
                cache_token=str(cache_token),
                dataframe=dataframe,
                sheet_name=str(sheet_name),
                expires_at=(now + self.ttl_seconds) if self.ttl_seconds > 0 else None,
            )
            self._entries.move_to_end(entry_key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def clear_conversation(self, conversation_id: str) -> None:
        with self._lock:
            normalized_id = str(conversation_id)
            for entry_key in list(self._entries.keys()):
                if entry_key[0] == normalized_id:
                    self._entries.pop(entry_key, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def _purge_expired(self, now: float) -> None:
        if self.ttl_seconds <= 0:
            return
        for entry_key, entry in list(self._entries.items()):
            if entry.expires_at is not None and entry.expires_at <= now:
                self._entries.pop(entry_key, None)
