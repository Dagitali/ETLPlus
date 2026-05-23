"""
:mod:`tests.conftest` module.

Top-level pytest configuration and compatibility exports for shared test types.
"""

from __future__ import annotations

# SECTION: PLUG-INS ========================================================= #


pytest_plugins = [
    'tests.pytest_shared_fixtures',
]
