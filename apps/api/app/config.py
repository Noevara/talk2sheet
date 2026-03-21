from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Talk2Sheet API"
    app_version: str = "0.1.0"
    api_prefix: str = "/api"
    data_dir: Path = Path(__file__).resolve().parents[1] / "data"
    upload_dir_name: str = "uploads"
    metadata_dir_name: str = "metadata"
    file_store_backend: str = "local"
    max_preview_rows: int = 200
    max_analysis_rows: int = 50000
    batch_max_parallel: int = 1
    allowed_origins: str = "http://localhost:5173,http://127.0.0.1:5173,http://localhost:8080,http://127.0.0.1:8080"
    planner_provider: str = "auto"
    context_interpreter_provider: str = "auto"
    repair_provider: str = "auto"
    answer_provider: str = "rule"
    llm_api_key: str = Field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    llm_base_url: str = Field(default_factory=lambda: os.getenv("OPENAI_BASE_URL", ""))
    llm_model: str = "gpt-4.1-mini"
    llm_timeout_seconds: float = 30.0
    llm_max_tokens: int = 1800
    llm_temperature: float = 0.0
    llm_use_json_schema: bool = True
    conversation_max_sessions: int = 200
    conversation_max_turns: int = 6
    conversation_cache_ttl_seconds: int = 1800
    conversation_cache_max_entries: int = 400

    model_config = SettingsConfigDict(
        env_prefix="TALK2SHEET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def upload_dir(self) -> Path:
        return self.data_dir / self.upload_dir_name

    @property
    def metadata_dir(self) -> Path:
        return self.data_dir / self.metadata_dir_name

    @property
    def cors_origins(self) -> list[str]:
        return [item.strip() for item in self.allowed_origins.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    settings.metadata_dir.mkdir(parents=True, exist_ok=True)
    return settings
