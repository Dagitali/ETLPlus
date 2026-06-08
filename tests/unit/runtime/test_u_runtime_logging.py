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


class _LoggingSetupRecorder:
    """Record process-wide logging setup hook calls."""

    def __init__(self) -> None:
        self.calls: dict[str, object] = {}

    def basic_config(self, **kwargs: object) -> None:
        """Record one ``logging.basicConfig`` call."""
        self.calls['basicConfig'] = kwargs

    def capture_warnings(self, enabled: bool) -> None:
        """Record one ``logging.captureWarnings`` call."""
        self.calls['captureWarnings'] = enabled


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='logging_setup_calls')
def logging_setup_calls_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, object]:
    """Patch process-wide logging setup hooks and capture their calls."""
    recorder = _LoggingSetupRecorder()

    monkeypatch.setattr(
        logging_mod.logging,
        'basicConfig',
        recorder.basic_config,
    )
    monkeypatch.setattr(
        logging_mod.logging,
        'captureWarnings',
        recorder.capture_warnings,
    )
    return recorder.calls


# SECTION: TESTS ============================================================ #


class TestConfigureLogging:
    """Unit tests for process-wide logging configuration."""

    @pytest.mark.parametrize(
        'check_name',
        [
            pytest.param('type', id='type'),
            pytest.param('stream', id='stream'),
        ],
    )
    def test_configure_logging_defaults_stream_to_stderr(
        self,
        logging_setup_calls: dict[str, object],
        check_name: str,
    ) -> None:
        """Test that STDERR is used when no explicit stream is supplied."""
        logging_mod.RuntimeLoggingPolicy.configure(env={})

        basic_config = logging_setup_calls['basicConfig']
        match check_name:
            case 'type':
                assert isinstance(basic_config, dict)
            case 'stream':
                assert basic_config['stream'] is sys.stderr
            case _:
                pytest.fail(f'unhandled check: {check_name}')

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('level', logging.ERROR, id='level'),
            pytest.param('basicConfig.force', True, id='force'),
            pytest.param(
                'basicConfig.format',
                '%(levelname)s %(name)s: %(message)s',
                id='format',
            ),
            pytest.param('basicConfig.level', logging.ERROR, id='basic-level'),
            pytest.param('basicConfig.stream', None, id='stream'),
            pytest.param('captureWarnings', True, id='capture-warnings'),
        ],
    )
    def test_configure_logging_passes_expected_arguments(
        self,
        logging_setup_calls: dict[str, object],
        field: str,
        expected: object,
    ) -> None:
        """Test that configuration forwards the resolved level and options."""
        stream = StringIO()

        level = logging_mod.RuntimeLoggingPolicy.configure(
            stream=stream,
            force=True,
            env={'ETLPLUS_LOG_LEVEL': 'error'},
        )

        basic_config = logging_setup_calls['basicConfig']
        assert isinstance(basic_config, dict)
        match field.split('.'):
            case ['level']:
                actual = level
            case ['basicConfig', 'stream']:
                actual = basic_config['stream']
                expected = stream
            case ['basicConfig', key]:
                actual = basic_config[key]
            case ['captureWarnings']:
                actual = logging_setup_calls['captureWarnings']
            case _:
                pytest.fail(f'Unsupported field path: {field}')
        assert actual == expected


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
