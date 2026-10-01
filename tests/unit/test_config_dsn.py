"""Settings.postgres_dsn — the libpq DSN every psycopg2.connect() call in the code base relies on."""

from __future__ import annotations

import pytest
from psycopg2.extensions import parse_dsn

from src.common.config.settings import Settings, get_settings


@pytest.fixture()
def make_settings(monkeypatch: pytest.MonkeyPatch):
    def _make(**env: str) -> Settings:
        monkeypatch.setenv("ENVIRONMENT", "dev")
        monkeypatch.setenv("SECRETS_BACKEND", "env")
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        get_settings.cache_clear()
        return Settings()

    yield _make
    get_settings.cache_clear()


class TestPostgresDsn:
    def test_parses_to_expected_parts(self, make_settings):
        s = make_settings(
            POSTGRES_HOST="db.internal",
            POSTGRES_PORT="6543",
            POSTGRES_DB="lh",
            POSTGRES_USER="svc",
            POSTGRES_PASSWORD="plain",
        )
        assert parse_dsn(s.postgres_dsn) == {
            "host": "db.internal",
            "port": "6543",
            "dbname": "lh",
            "user": "svc",
            "password": "plain",
        }

    @pytest.mark.parametrize("password", ["pa'ss", "back\\slash", "sp ace", "eq=ual", "@:/?#[]"])
    def test_special_characters_round_trip(self, make_settings, password):
        s = make_settings(POSTGRES_PASSWORD=password)
        assert parse_dsn(s.postgres_dsn)["password"] == password

    def test_password_never_in_repr(self, make_settings):
        s = make_settings(POSTGRES_PASSWORD="hunter2-unique-marker")
        assert "hunter2-unique-marker" not in repr(s)
        assert "hunter2-unique-marker" not in str(s)

    def test_urls_are_excluded_from_repr_but_still_computed(self, make_settings):
        s = make_settings(POSTGRES_PASSWORD="hunter2-unique-marker")
        assert "hunter2-unique-marker" in s.postgres_sync_url  # still usable
        assert "postgres_sync_url" not in repr(s)
        assert "postgres_async_url" not in repr(s)
