"""
:mod:`tests.unit.database.test_u_database_engine` module.

Unit tests for :mod:`etlplus.database._engine`.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import cast

import pytest

from etlplus.database._engine import load_database_url_from_config

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTIONS: HELPERS ========================================================= #


# Directory-level marker for unit tests.
engine_mod = importlib.import_module('etlplus.database._engine')


@dataclass(slots=True)
class _CreateEngineSpy:
    """Callable create-engine spy that records invocation arguments."""

    captured: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    def __call__(self, url: str, **kwargs: Any) -> dict[str, Any]:
        """Record one create-engine call and return a lightweight engine stub."""
        self.captured.append((url, kwargs))
        return {'url': url, 'kwargs': kwargs}


# SECTION: TESTS ============================================================ #


class TestLoadDatabaseUrlFromConfig:
    """
    Unit tests for :func:`load_database_url_from_config`.

    Notes
    -----
    Patches :class:`etlplus.file.File` to avoid disk IO and uses helper
    fixtures to keep tests DRY.
    """

    @pytest.mark.parametrize(
        ('name', 'expected'),
        [
            pytest.param(None, 'sqlite:///default.db', id='default'),
            pytest.param('reporting', 'postgresql://reporting', id='named'),
        ],
    )
    def test_loads_default_and_named_entries(
        self,
        patch_file_read: Callable[[Any, Any], None],
        name: str | None,
        expected: str,
    ) -> None:
        """
        Test extracting URLs from default and named entries, including nested
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

        patch_file_read(engine_mod.File, config)

        assert load_database_url_from_config('cfg.yml', name=name) == expected

    @pytest.mark.parametrize(
        ('payload', 'expected_exc'),
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
        patch_file_read: Callable[[Any, Any], None],
        payload: Any,
        expected_exc: type[Exception],
    ) -> None:
        """Test that invalid structures surface helpful errors."""
        patch_file_read(engine_mod.File, payload)

        with pytest.raises(expected_exc):
            load_database_url_from_config('bad.yml')


class TestMakeEngine:
    """Unit tests for :func:`make_engine` and module defaults."""

    @pytest.fixture
    def capture_create_engine(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> _CreateEngineSpy:
        """
        Patch ``create_engine`` to capture calls.
        """
        spy = _CreateEngineSpy()
        monkeypatch.setattr(engine_mod, 'create_engine', spy)
        return spy

    def test_make_engine_uses_explicit_url(
        self,
        capture_create_engine: _CreateEngineSpy,
    ) -> None:
        """
        Test that explicit URL is forwarded to create_engine with pre-ping
        enabled.
        """
        eng = engine_mod.make_engine('sqlite:///explicit.db', echo=True)
        eng_dict = cast(dict[str, Any], eng)

        assert eng_dict['url'] == 'sqlite:///explicit.db'
        assert capture_create_engine.captured[0][1]['pool_pre_ping'] is True
        assert capture_create_engine.captured[0][1]['echo'] is True

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

        try:
            reloaded = importlib.reload(engine_mod)
            default_engine = cast(dict[str, Any], reloaded.engine)

            assert reloaded.DATABASE_URL == 'sqlite:///env.db'
            assert default_engine['url'] == 'sqlite:///env.db'
            assert captured[0][1]['pool_pre_ping'] is True

            eng = reloaded.make_engine()
            eng_dict = cast(dict[str, Any], eng)
            assert eng_dict['url'] == 'sqlite:///env.db'
        finally:
            monkeypatch.undo()
            importlib.reload(engine_mod)
