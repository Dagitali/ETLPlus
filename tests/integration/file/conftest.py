"""
:mod:`tests.integration.file.conftest` module.

Shared fixtures and helpers for pytest-based integration tests of
:mod:`etlplus.file` modules.
"""

from __future__ import annotations

import pytest

from .pytest_smoke_file_contracts import FILE_SMOKE_CASES
from .pytest_smoke_file_contracts import FileSmokeCase
from .pytest_smoke_file_contracts import run_file_smoke

# SECTION: MARKS ============================================================ #


# Directory-level markers for integration tests.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'FileSmokeCase',
    # Constants
    'FILE_SMOKE_CASES',
    # Functions
    'run_file_smoke',
]
