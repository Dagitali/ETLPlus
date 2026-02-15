"""
:mod:`tests.smoke.conftest` module.

Define shared fixtures and helpers for legacy-path smoke tests.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for legacy smoke-path tests.
pytestmark = pytest.mark.smoke
