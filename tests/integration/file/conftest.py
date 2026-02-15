"""
:mod:`tests.integration.file.conftest` module.

Shared integration tests for :mod:`etlplus.file`.
"""

from __future__ import annotations

import pytest

from .pytest_smoke_file_contracts import SmokeRoundtripModuleContract
from .pytest_smoke_file_contracts import run_file_smoke

# SECTION: MARKS ============================================================ #


# Directory-level markers for integration tests.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'SmokeRoundtripModuleContract',
    'run_file_smoke',
]
