"""
:mod:`tests.integration.api.conftest` module.

Shared fixtures and helpers for pytest-based integration tests of
:mod:`etlplus.api` modules.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for integration tests.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]
