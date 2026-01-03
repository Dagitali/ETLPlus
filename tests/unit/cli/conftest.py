"""
:mod:`tests.unit.cli.conftest` module.

Configures pytest-based unit tests and provides shared fixtures for
:mod:`etlplus.cli` unit tests.

Notes
-----
- Fixtures are designed for reuse and DRY test setup.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='runner')
def runner_fixture() -> CliRunner:
    """Return a reusable Typer CLI runner.

    Returns
    -------
    CliRunner
        The Typer testing runner instance reused across CLI suites.
    """
    return CliRunner()
