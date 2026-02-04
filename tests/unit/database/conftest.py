"""
:mod:`tests.unit.database.conftest` module.

Define shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.database`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit
