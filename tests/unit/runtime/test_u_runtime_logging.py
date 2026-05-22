"""
:mod:`tests.unit.runtime.test_u_runtime_logging` module.

Unit tests for :mod:`etlplus.runtime._logging`.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Mapping
from io import StringIO
from typing import TypedDict

import pytest

import etlplus.runtime._logging as logging_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


class _ResolveLogLevelKwargs(TypedDict, total=False):
    """
    Typed kwargs shape for :meth:`RuntimeLoggingPolicy.resolve_level` tables.
    """

    env: Mapping[str, str]
    quiet: bool
    verbose: bool


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='logging_setup_calls')
def logging_setup_calls_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    """Patch process-wide logging setup hooks and capture their calls."""
    calls: dict[str, object] = {}
    monkeypatch.setattr(
        logging_mod.logging,
        'basicConfig',
        lambda **kwargs: calls.setdefault('basicConfig', kwargs),
    )
    monkeypatch.setattr(
        logging_mod.logging,
        'captureWarnings',
        lambda enabled: calls.setdefault('captureWarnings', enabled),
    )
    return calls


# SECTION: TESTS ============================================================ #


class TestConfigureLogging:
    """Unit tests for process-wide logging configuration."""

    def test_configure_logging_defaults_stream_to_stderr(
        self,
        logging_setup_calls: dict[str, object],
    ) -> None:
        """Test that STDERR is used when no explicit stream is supplied."""
        logging_mod.RuntimeLoggingPolicy.configure(env={})

        basic_config = logging_setup_calls['basicConfig']
        assert isinstance(basic_config, dict)
        assert basic_config['stream'] is sys.stderr

    def test_configure_logging_passes_expected_arguments(
        self,
        logging_setup_calls: dict[str, object],
    ) -> None:
        """Test that configuration forwards the resolved level and options."""
        stream = StringIO()

        level = logging_mod.RuntimeLoggingPolicy.configure(
            stream=stream,
            force=True,
            env={'ETLPLUS_LOG_LEVEL': 'error'},
        )

        assert level == logging.ERROR
        assert logging_setup_calls == {
            'basicConfig': {
                'force': True,
                'format': '%(levelname)s %(name)s: %(message)s',
                'level': logging.ERROR,
                'stream': stream,
            },
            'captureWarnings': True,
        }


class TestResolveLogLevel:
    """Unit tests for runtime log-level resolution."""

    def test_env_defaults_to_os_environ_when_not_supplied(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that resolution reads the process environment by default."""
        monkeypatch.setenv('ETLPLUS_LOG_LEVEL', 'info')

        assert logging_mod.RuntimeLoggingPolicy.resolve_level() == logging.INFO

    @pytest.mark.parametrize(
        ('kwargs', 'expected'),
        [
            pytest.param(
                {'env': {'ETLPLUS_LOG_LEVEL': 'trace'}},
                logging.WARNING,
                id='invalid-explicit-level',
            ),
            pytest.param(
                {
                    'quiet': True,
                    'verbose': True,
                    'env': {'ETLPLUS_LOG_LEVEL': 'debug'},
                },
                logging.DEBUG,
                id='explicit-env-overrides-flags',
            ),
            pytest.param(
                {'quiet': True, 'verbose': True, 'env': {}},
                logging.ERROR,
                id='quiet-precedence',
            ),
            pytest.param(
                {'verbose': True, 'env': {}},
                logging.INFO,
                id='verbose-without-quiet',
            ),
        ],
    )
    def test_resolve_log_level_precedence(
        self,
        kwargs: _ResolveLogLevelKwargs,
        expected: int,
    ) -> None:
        """Log-level resolution should honor env overrides and CLI flags."""
        assert logging_mod.RuntimeLoggingPolicy.resolve_level(**kwargs) == expected
