"""
:mod:`tests.unit.utils.conftest` module.

Shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.utils` modules.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit
