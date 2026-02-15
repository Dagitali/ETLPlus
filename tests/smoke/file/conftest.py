"""
:mod:`tests.smoke.file.conftest` module.

Shared smoke contracts for pytest-based tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

import pytest

from .pytest_smoke_file_contracts import SmokeRoundtripModuleContract
from .pytest_smoke_file_contracts import run_file_smoke

# Directory-level marker for smoke tests.
# Legacy-path smoke tests here are integration-scope checks.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]

__all__ = [
    'SmokeRoundtripModuleContract',
    'run_file_smoke',
]
