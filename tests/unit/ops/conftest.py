"""
:mod:`tests.unit.ops.conftest` module.

Define shared fixtures and helpers for pytest-based unit tests of
:mod:`etlplus.ops`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit
