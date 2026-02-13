"""
:mod:`tests.smoke.file.conftest` module.

Shared smoke contracts for pytest-based tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

import pytest

from tests.smoke.file.pytest_smoke_file_contracts import FileModule
from tests.smoke.file.pytest_smoke_file_contracts import (
    SmokeRoundtripModuleContract,
)
from tests.smoke.file.pytest_smoke_file_contracts import run_file_smoke

# Directory-level marker for smoke tests.
pytestmark = pytest.mark.smoke

__all__ = [
    'FileModule',
    'SmokeRoundtripModuleContract',
    'run_file_smoke',
]
