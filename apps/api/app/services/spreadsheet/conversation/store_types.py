from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class ConversationSession:
    conversation_id: str
    file_id: str
    sheet_index: int
    locale: str
    sheet_name: str = ""
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    turns: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class EnsureSessionResult:
    session: ConversationSession
    reset: bool
    evicted_conversation_ids: list[str] = field(default_factory=list)


class SessionStore(Protocol):
    def ensure_session(
        self,
        *,
        conversation_id: str | None,
        file_id: str,
        sheet_index: int,
        locale: str,
    ) -> EnsureSessionResult: ...

    def append_turn(self, session: ConversationSession, turn_summary: dict[str, Any]) -> None: ...

    def get(self, conversation_id: str) -> ConversationSession | None: ...

    def clear(self) -> list[str]: ...


class DataframeCache(Protocol):
    def get(
        self,
        conversation_id: str,
        *,
        cache_key: str,
        cache_token: str,
    ) -> tuple[Any, str] | None: ...

    def set(
        self,
        conversation_id: str,
        *,
        cache_key: str,
        cache_token: str,
        dataframe: Any,
        sheet_name: str,
    ) -> None: ...

    def clear_conversation(self, conversation_id: str) -> None: ...

    def clear(self) -> None: ...
