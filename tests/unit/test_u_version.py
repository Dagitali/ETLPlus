"""
:mod:`tests.unit.test_u_version` module.

Unit tests for :mod:`etlplus.__version__`.
"""

from __future__ import annotations

import importlib
import importlib.metadata
from types import ModuleType

import pytest

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _patch_version_lookup(
    monkeypatch: pytest.MonkeyPatch,
    metadata_version: str | None,
) -> None:
    """Patch :mod:`importlib.metadata` version lookup for one test case."""
    if metadata_version is not None:
        monkeypatch.setattr(
            importlib.metadata,
            'version',
            lambda _pkg: metadata_version,
        )
        return

    class FakePackageNotFoundError(Exception):
        """Sentinel replacement for :class:`PackageNotFoundError`."""

    monkeypatch.setattr(
        importlib.metadata,
        'PackageNotFoundError',
        FakePackageNotFoundError,
    )

    def _raise(_pkg: str) -> str:
        raise FakePackageNotFoundError()

    monkeypatch.setattr(importlib.metadata, 'version', _raise)


def _reload_version_module() -> ModuleType:
    """Reload and return the :mod:`etlplus.__version__` module."""
    version_mod = importlib.import_module('etlplus.__version__')
    return importlib.reload(version_mod)


# SECTION: TESTS ============================================================ #


class TestVersionModule:
    """Unit tests for package version detection."""

    @pytest.mark.parametrize(
        ('metadata_version', 'expected_version'),
        [
            pytest.param('1.2.3', '1.2.3', id='metadata-version'),
            pytest.param(None, '0.0.0', id='fallback-version'),
        ],
    )
    def test_version_module_reports_expected_version(
        self,
        monkeypatch: pytest.MonkeyPatch,
        metadata_version: str | None,
        expected_version: str,
    ) -> None:
        """
        Test that :mod:`etlplus.__version__` uses installed metadata or its
        fallback.
        """
        _patch_version_lookup(monkeypatch, metadata_version)

        assert _reload_version_module().__version__ == expected_version
