from __future__ import annotations

from app.config import get_settings


def _refresh_settings() -> None:
    get_settings.cache_clear()


def test_join_beta_defaults_enabled_in_dev_env(monkeypatch) -> None:
    monkeypatch.delenv("TALK2SHEET_ENABLE_JOIN_BETA", raising=False)
    monkeypatch.setenv("TALK2SHEET_APP_ENV", "dev")
    _refresh_settings()
    settings = get_settings()
    assert settings.join_beta_enabled is True
    _refresh_settings()


def test_join_beta_defaults_disabled_in_prod_env(monkeypatch) -> None:
    monkeypatch.delenv("TALK2SHEET_ENABLE_JOIN_BETA", raising=False)
    monkeypatch.setenv("TALK2SHEET_APP_ENV", "prod")
    _refresh_settings()
    settings = get_settings()
    assert settings.join_beta_enabled is False
    _refresh_settings()


def test_join_beta_explicit_flag_overrides_env_default(monkeypatch) -> None:
    monkeypatch.setenv("TALK2SHEET_APP_ENV", "prod")
    monkeypatch.setenv("TALK2SHEET_ENABLE_JOIN_BETA", "true")
    _refresh_settings()
    settings = get_settings()
    assert settings.join_beta_enabled is True
    _refresh_settings()
