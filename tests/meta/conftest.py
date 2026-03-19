"""
:mod:`tests.meta.conftest` module.

Shared pytest configuration for meta-level repository guardrail tests.
"""

from __future__ import annotations

import pytest

# SECTION: MARKERS ========================================================== #


# Directory-level marker for meta tests.
pytestmark = pytest.mark.meta
