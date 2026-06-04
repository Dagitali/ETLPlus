"""
:mod:`tests.unit.storage.pytest_storage_support` module.

Shared test doubles for storage unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

# SECTION: FUNCTIONS ======================================================== #


def clear_azure_storage_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear Azure storage configuration environment variables."""
    for name in (
        'AZURE_STORAGE_CONNECTION_STRING',
        'AZURE_STORAGE_ACCOUNT_URL',
        'AZURE_STORAGE_CREDENTIAL',
    ):
        monkeypatch.delenv(name, raising=False)


# SECTION: DATA CLASSES ===================================================== #


@dataclass(slots=True)
class FakeContentSettings:
    """Minimal content-settings test double."""

    content_type: str
