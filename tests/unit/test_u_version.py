"""
:mod:`tests.unit.test_u_version` module.

Unit tests for :mod:`etlplus.__version__`.

Notes
-----
- Covers version detection and fallback logic.
"""

import importlib
import importlib.metadata

import pytest

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


def test_version_metadata(
    monkeypatch,
):
    """ "Test that version is correctly retrieved from package metadata."""
    # Simulate importlib.metadata.version returning a real version.
    monkeypatch.setattr(importlib.metadata, 'version', lambda pkg: '1.2.3')
    version_mod = importlib.import_module('etlplus.__version__')
    importlib.reload(version_mod)
    assert version_mod.__version__ == '1.2.3'


def test_version_fallback(
    monkeypatch,
):
    """Test that fallback version is used when metadata is unavailable."""

    # Simulate importlib.metadata.version raising PackageNotFoundError.
    class FakeError(Exception):
        """Fake :class:`PackageNotFoundError` exception."""

    monkeypatch.setattr(importlib.metadata, 'PackageNotFoundError', FakeError)
    monkeypatch.setattr(
        importlib.metadata,
        'version',
        lambda pkg: (_ for _ in ()).throw(FakeError()),
    )
    version_mod = importlib.import_module('etlplus.__version__')
    importlib.reload(version_mod)
    assert version_mod.__version__ == '0.0.0'
