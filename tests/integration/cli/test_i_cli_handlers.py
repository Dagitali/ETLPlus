"""
:mod:`tests.integration.cli.test_i_cli_handlers` module.

Integration-scope smoke tests for CLI handler wiring.
"""

from __future__ import annotations

from types import ModuleType

import pytest

from etlplus.cli._handlers import dataops as dataops_mod
from etlplus.cli._handlers import run as run_mod

from ...pytest_shared_support import CaptureHandler

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: CONSTANTS ======================================================== #


HANDLER_SMOKE_CASES = (
    pytest.param(
        run_mod,
        'run_handler',
        {'config': 'pipeline.yml', 'job': 'job1', 'pretty': True},
        ('config', 'job', 'pretty'),
        id='run-handler-smoke',
    ),
    pytest.param(
        dataops_mod,
        'transform_handler',
        {
            'source': 'data.json',
            'operations': '{"select": ["id"]}',
            'pretty': True,
        },
        ('source', 'operations', 'pretty'),
        id='transform-handler-smoke',
    ),
    pytest.param(
        dataops_mod,
        'validate_handler',
        {
            'source': 'data.json',
            'rules': '{"required": ["id"]}',
            'pretty': True,
        },
        ('source', 'rules', 'pretty'),
        id='validate-handler-smoke',
    ),
)

# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('module', 'attr', 'kwargs', 'expected_keys'),
    HANDLER_SMOKE_CASES,
)
def test_handler_smoke(
    capture_handler: CaptureHandler,
    module: ModuleType,
    attr: str,
    kwargs: dict[str, str | bool],
    expected_keys: tuple[str, ...],
) -> None:
    """
    Test that CLI handlers accept kwargs and call underlying logic.
    """
    calls = capture_handler(module, attr)
    result = getattr(module, attr)(**kwargs)

    assert result == 0
    for key in expected_keys:
        assert key in calls
