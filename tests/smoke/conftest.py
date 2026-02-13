"""
:mod:`tests.smoke.conftest` module.

Define shared fixtures and helpers for pytest-based smoke tests of
:mod:`etlplus`.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for smoke tests.
pytestmark = pytest.mark.smoke
