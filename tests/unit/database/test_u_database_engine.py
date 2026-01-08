"""
:mod:`tests.unit.database.test_u_database_engine` module.

Unit tests for :mod:`etlplus.database.engine`.
"""

from __future__ import annotations

import importlib
from typing import Any
from typing import cast

import pytest

from etlplus.database.engine import load_database_url_from_config

# SECTIONS: HELPERS ========================================================= #


pytestmark = pytest.mark.unit

engine_mod = importlib.import_module('etlplus.database.engine')


# SECTION: TESTS ============================================================ #


class TestLoadDatabaseUrlFromConfig:
    """Unit tests for :func:`load_database_url_from_config`."""

    def test_loads_default_and_named_entries(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test extracting URLs from default and named entries including nested
        defaults.
        """
        config = {
            'databases': {
                'default': {
                    'default': {'connection_string': 'sqlite:///default.db'},
                },
                'reporting': {
                    'url': 'postgresql://reporting',
                },
            },
        }

        monkeypatch.setattr(
            engine_mod.File,
            'read_file',
            staticmethod(lambda path: config),
        )

        assert (
            load_database_url_from_config('cfg.yml') == 'sqlite:///default.db'
        )
        assert (
            load_database_url_from_config('cfg.yml', name='reporting')
            == 'postgresql://reporting'
        )

    @pytest.mark.parametrize(
        'payload, expected_exc',
        [
            ({}, KeyError),
            ({'databases': None}, KeyError),
            ({'databases': {'default': None}}, KeyError),
            ({'databases': {'default': 'dsn'}}, TypeError),
            ({'databases': {'default': {}}}, ValueError),
            ('not-a-mapping', TypeError),
        ],
    )
    def test_invalid_configs_raise(
        self,
        monkeypatch: pytest.MonkeyPatch,
        payload: Any,
        expected_exc: type[Exception],
    ) -> None:
        """Test that invalid structures surface helpful errors."""

        monkeypatch.setattr(
            engine_mod.File,
            'read_file',
            staticmethod(lambda path: payload),
        )

        with pytest.raises(expected_exc):
            load_database_url_from_config('bad.yml')


class TestMakeEngine:
    """Unit tests for :func:`make_engine` and module defaults."""

    def test_make_engine_uses_explicit_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that explicit URL is forwarded to create_engine with pre-ping
        enabled.
        """
        captured: list[tuple[str, dict[str, Any]]] = []

        def _fake_create_engine(url: str, **kwargs: Any) -> dict[str, Any]:
            captured.append((url, kwargs))
            return {'url': url, 'kwargs': kwargs}

        monkeypatch.setattr(engine_mod, 'create_engine', _fake_create_engine)

        eng = engine_mod.make_engine('sqlite:///explicit.db', echo=True)
        eng_dict = cast(dict[str, Any], eng)

        assert eng_dict['url'] == 'sqlite:///explicit.db'
        assert captured[0][1]['pool_pre_ping'] is True
        assert captured[0][1]['echo'] is True

    def test_default_url_reload_respects_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that reloading module picks up DATABASE_URL env and uses fake
        engine factory.
        """
        monkeypatch.setenv('DATABASE_URL', 'sqlite:///env.db')
        monkeypatch.delenv('DATABASE_DSN', raising=False)

        captured: list[tuple[str, dict[str, Any]]] = []

        def _fake_create_engine(url: str, **kwargs: Any) -> dict[str, Any]:
            captured.append((url, kwargs))
            return {'url': url, 'kwargs': kwargs}

        monkeypatch.setattr('sqlalchemy.create_engine', _fake_create_engine)

        reloaded = importlib.reload(engine_mod)
        default_engine = cast(dict[str, Any], reloaded.engine)

        assert reloaded.DATABASE_URL == 'sqlite:///env.db'
        assert default_engine['url'] == 'sqlite:///env.db'
        assert captured[0][1]['pool_pre_ping'] is True

        eng = reloaded.make_engine()
        eng_dict = cast(dict[str, Any], eng)
        assert eng_dict['url'] == 'sqlite:///env.db'
