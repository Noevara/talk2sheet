from __future__ import annotations

import re


def _contains_any(text: str, tokens: tuple[str, ...] | list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(token.lower() in lowered for token in tokens)


def _extract_top_k(chat_text: str, default: int = 5, upper: int = 50) -> int:
    match = re.search(
        r"top\s*(\d+)|前\s*(\d+)|最多\s*(\d+)|最大\s*(\d+)|最高\s*(\d+)|largest\s*(\d+)|biggest\s*(\d+)",
        str(chat_text or ""),
        flags=re.I,
    )
    if not match:
        return default
    for group in match.groups():
        if group:
            return max(1, min(int(group), upper))
    return default
