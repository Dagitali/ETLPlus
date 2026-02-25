"""
:mod:`tests.integration.database.conftest` module.

Shared integration smoke tests for :mod:`etlplus.database`.
"""

from __future__ import annotations

import pytest

# SECTION: MARKS ============================================================ #


# Directory-level markers for integration tests.
pytestmark = [pytest.mark.integration, pytest.mark.smoke]
