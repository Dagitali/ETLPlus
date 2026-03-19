"""
:mod:`tests.integration.storage.conftest` module.

Shared fixtures and helpers for pytest-based integration tests of
:mod:`etlplus.storage` modules.
"""

from __future__ import annotations

import pytest

# SECTION: MARKS ============================================================ #


# Directory-level markers for integration tests.
pytestmark = pytest.mark.integration
