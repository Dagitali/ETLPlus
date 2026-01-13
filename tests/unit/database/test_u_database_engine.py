"""
:mod:`tests.unit.database.test_u_database_engine` module.

Unit tests for :mod:`etlplus.database.engine`.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any
from typing import cast

import pytest

from etlplus.database.engine import load_database_url_from_config

# SECTIONS: HELPERS ========================================================= #


pytestmark = pytest.mark.unit

engine_mod = importlib.import_module('etlplus.database.engine')


# SECTION: TESTS ============================================================ #


class TestLoadDatabaseUrlFromConfig:
    """
    Unit test suite for :func:`load_database_url_from_config`.

    Notes
    -----
    Patches :class:`etlplus.file.File` to avoid disk IO and uses helper
    fixtures to keep tests DRY.
    """

    @pytest.fixture()
    def patch_read_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Callable[[Any], None]:
        """
        Return a helper that patches :meth:`read` to return a payload.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture for applying patches.

        Returns
        -------
        Callable[[Any], None]
            Function that patches ``File.read`` to return the payload.
        """

        def _apply(payload: Any) -> None:
            monkeypatch.setattr(
                engine_mod.File,
                'read',
                lambda self: payload,
            )

        return _apply

    def test_loads_default_and_named_entries(
        self,
        patch_read_file: Callable[[Any], None],
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

        patch_read_file(config)

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
        patch_read_file: Callable[[Any], None],
        payload: Any,
        expected_exc: type[Exception],
    ) -> None:
        """Test that invalid structures surface helpful errors."""
        patch_read_file(payload)

        with pytest.raises(expected_exc):
            load_database_url_from_config('bad.yml')


class TestMakeEngine:
    """Unit test suite for :func:`make_engine` and module defaults."""

    @pytest.fixture()
    def capture_create_engine(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Callable[..., dict[str, Any]]:
        """
        Patch ``create_engine`` to capture calls.

        Parameters
        ----------
        monkeypatch : pytest.MonkeyPatch
            Pytest monkeypatch fixture for applying patches.

        Returns
        -------
        Callable[..., dict[str, Any]]
            Fake ``create_engine`` that records arguments.
        """
        captured: list[tuple[str, dict[str, Any]]] = []

        def _fake_create_engine(url: str, **kwargs: Any) -> dict[str, Any]:
            captured.append((url, kwargs))
            return {'url': url, 'kwargs': kwargs}

        monkeypatch.setattr(engine_mod, 'create_engine', _fake_create_engine)

        def _factory(url: str, **kwargs: Any) -> dict[str, Any]:
            return _fake_create_engine(url, **kwargs)

        _factory.captured = captured  # type: ignore[attr-defined]
        return _factory

    def test_make_engine_uses_explicit_url(
        self,
        capture_create_engine: Callable[[str, Any], dict[str, Any]],
    ) -> None:
        """
        Test that explicit URL is forwarded to create_engine with pre-ping
        enabled.
        """
        eng = engine_mod.make_engine('sqlite:///explicit.db', echo=True)
        eng_dict = cast(dict[str, Any], eng)

        assert eng_dict['url'] == 'sqlite:///explicit.db'
        captured = capture_create_engine.captured  # type: ignore[attr-defined]
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
