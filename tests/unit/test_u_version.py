"""
:mod:`tests.unit.test_u_version` module.

Unit tests for :mod:`etlplus.__version__`.

Notes
-----
- Covers version detection and fallback logic.
"""

from __future__ import annotations

import importlib
import importlib.metadata
from types import ModuleType

import pytest

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


def _reload_version_module() -> ModuleType:
    """Reload and return the :mod:`etlplus.__version__` module."""
    version_mod = importlib.import_module('etlplus.__version__')
    return importlib.reload(version_mod)


# SECTION: TESTS ============================================================ #


def test_version_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that version is correctly retrieved from package metadata."""
    # Simulate importlib.metadata.version returning a real version.
    monkeypatch.setattr(importlib.metadata, 'version', lambda _pkg: '1.2.3')
    version_mod = _reload_version_module()
    assert version_mod.__version__ == '1.2.3'


def test_version_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that fallback version is used when metadata is unavailable."""

    # Simulate importlib.metadata.version raising PackageNotFoundError.
    class FakeError(Exception):
        """Fake :class:`PackageNotFoundError` exception."""

    monkeypatch.setattr(importlib.metadata, 'PackageNotFoundError', FakeError)

    def _raise(_pkg: str) -> str:
        """Raise the sentinel package-not-found error."""
        raise FakeError()

    monkeypatch.setattr(importlib.metadata, 'version', _raise)
    version_mod = _reload_version_module()
    assert version_mod.__version__ == '0.0.0'
