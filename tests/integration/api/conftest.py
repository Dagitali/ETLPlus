"""
:mod:`tests.integration.api.conftest` module.

Define shared fixtures and helpers for pytest-based integration tests of
:mod:`etlplus.api`.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for integration tests.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]
