"""
:mod:`tests.integration.database.conftest` module.

Shared fixtures and helpers for pytest-based integration tests of
:mod:`etlplus.database` modules.
"""

from __future__ import annotations

import pytest

# SECTION: MARKS ============================================================ #


# Directory-level markers for integration tests.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]
