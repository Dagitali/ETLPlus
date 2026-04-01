"""
:mod:`tests.unit.runtime.test_u_runtime_logging` module.

Unit tests for :mod:`etlplus.runtime._logging`.
"""

from __future__ import annotations

import logging
import sys
from io import StringIO
from typing import cast

import pytest

import etlplus.runtime._logging as logging_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConfigureLogging:
    """Unit tests for process-wide logging configuration."""

    def test_configure_logging_defaults_stream_to_stderr(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that STDERR is used when no explicit stream is supplied."""
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

        logging_mod.configure_logging(env={})

        basic_config = cast(dict[str, object], calls['basicConfig'])
        assert basic_config['stream'] is sys.stderr

    def test_configure_logging_passes_expected_arguments(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that configuration forwards the resolved level and options."""
        calls: dict[str, object] = {}
        stream = StringIO()

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

        level = logging_mod.configure_logging(
            stream=stream,
            force=True,
            env={'ETLPLUS_LOG_LEVEL': 'error'},
        )

        assert level == logging.ERROR
        assert calls == {
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

    def test_default_level_is_used_for_invalid_explicit_level(self) -> None:
        """Test fallback to the default level for unsupported env values."""
        assert logging_mod.resolve_log_level(env={'ETLPLUS_LOG_LEVEL': 'trace'}) == (
            logging.WARNING
        )

    def test_env_defaults_to_os_environ_when_not_supplied(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that resolution reads the process environment by default."""
        monkeypatch.setenv('ETLPLUS_LOG_LEVEL', 'info')

        assert logging_mod.resolve_log_level() == logging.INFO

    def test_explicit_env_level_overrides_quiet_and_verbose(self) -> None:
        """Test that an explicit environment level wins over CLI flags."""
        level = logging_mod.resolve_log_level(
            quiet=True,
            verbose=True,
            env={'ETLPLUS_LOG_LEVEL': 'debug'},
        )

        assert level == logging.DEBUG

    def test_quiet_level_takes_precedence_without_explicit_env(self) -> None:
        """Test quiet-mode resolution without an explicit environment level."""
        assert logging_mod.resolve_log_level(quiet=True, verbose=True, env={}) == (
            logging.ERROR
        )

    def test_verbose_level_is_used_without_quiet_or_explicit_env(self) -> None:
        """Test verbose-mode resolution without quiet mode or env override."""
        assert logging_mod.resolve_log_level(verbose=True, env={}) == logging.INFO
