from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, TypeVar

from pydantic import BaseModel

from app.config import get_settings


T = TypeVar("T", bound=BaseModel)


class OpenAICompatibleError(RuntimeError):
    pass


class OpenAICompatibleJsonClient:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str = "",
        timeout_seconds: float = 30.0,
        max_tokens: int = 1800,
        temperature: float = 0.0,
        use_json_schema: bool = True,
    ) -> None:
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.use_json_schema = use_json_schema

    @property
    def enabled(self) -> bool:
        return bool(self.api_key and self.model)

    @lru_cache(maxsize=1)
    def _client(self) -> Any:
        if not self.enabled:
            raise OpenAICompatibleError("LLM client is not configured.")
        try:
            from openai import OpenAI
        except Exception as exc:
            raise OpenAICompatibleError("The `openai` package is not installed.") from exc

        kwargs: dict[str, Any] = {
            "api_key": self.api_key,
            "timeout": self.timeout_seconds,
        }
        if self.base_url:
            kwargs["base_url"] = self.base_url
        return OpenAI(**kwargs)

    def _request_json_schema(self, schema_model: type[T], system_prompt: str, user_prompt: str) -> T:
        client = self._client()
        schema_name = schema_model.__name__.lower()
        response = client.chat.completions.create(
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "strict": True,
                    "schema": schema_model.model_json_schema(),
                },
            },
        )
        return self._parse_response(schema_model, response)

    def _request_json_object(self, schema_model: type[T], system_prompt: str, user_prompt: str) -> T:
        client = self._client()
        response = client.chat.completions.create(
            model=self.model,
            temperature=self.temperature,
            max_tokens=self.max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
        )
        return self._parse_response(schema_model, response)

    def _parse_response(self, schema_model: type[T], response: Any) -> T:
        choice = response.choices[0]
        message = getattr(choice, "message", None)
        if message is None:
            raise OpenAICompatibleError("LLM response did not include a message.")
        refusal = getattr(message, "refusal", None)
        if refusal:
            raise OpenAICompatibleError(f"LLM refused the request: {refusal}")
        content = getattr(message, "content", None)
        if isinstance(content, list):
            text_parts = []
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    text_parts.append(str(part.get("text") or ""))
            content = "".join(text_parts)
        if not content or not str(content).strip():
            raise OpenAICompatibleError("LLM response content was empty.")
        try:
            return schema_model.model_validate_json(str(content))
        except Exception:
            return schema_model.model_validate(json.loads(str(content)))

    def generate_json(self, schema_model: type[T], *, system_prompt: str, user_prompt: str) -> T:
        if not self.enabled:
            raise OpenAICompatibleError("LLM client is not configured.")
        if self.use_json_schema:
            try:
                return self._request_json_schema(schema_model, system_prompt, user_prompt)
            except Exception:
                return self._request_json_object(schema_model, system_prompt, user_prompt)
        return self._request_json_object(schema_model, system_prompt, user_prompt)


def build_default_llm_client() -> OpenAICompatibleJsonClient:
    settings = get_settings()
    return OpenAICompatibleJsonClient(
        api_key=settings.llm_api_key,
        model=settings.llm_model,
        base_url=settings.llm_base_url,
        timeout_seconds=settings.llm_timeout_seconds,
        max_tokens=settings.llm_max_tokens,
        temperature=settings.llm_temperature,
        use_json_schema=settings.llm_use_json_schema,
    )
