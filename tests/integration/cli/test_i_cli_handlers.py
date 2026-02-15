"""
:mod:`tests.integration.cli.test_i_cli_handlers` module.

Integration-scope smoke tests for CLI handler wiring.
"""

from __future__ import annotations

import pytest

from etlplus.cli import handlers

from ...conftest import CaptureHandler

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('kwargs', 'expected_keys'),
    (
        pytest.param(
            {'config': 'pipeline.yml', 'job': 'job1', 'pretty': True},
            ['config', 'job', 'pretty'],
            id='run-handler-smoke',
        ),
        pytest.param(
            {
                'source': 'data.json',
                'operations': '{"select": ["id"]}',
                'pretty': True,
            },
            ['source', 'operations', 'pretty'],
            id='transform-handler-smoke',
        ),
        pytest.param(
            {
                'source': 'data.json',
                'rules': '{"required": ["id"]}',
                'pretty': True,
            },
            ['source', 'rules', 'pretty'],
            id='validate-handler-smoke',
        ),
    ),
)
def test_handler_smoke(
    capture_handler: CaptureHandler,
    kwargs: dict[str, str | bool],
    expected_keys: list[str],
) -> None:
    """
    Test that CLI handlers accept kwargs and call underlying logic.
    """
    if 'job' in kwargs:
        module, attr = handlers, 'run_handler'
    elif 'operations' in kwargs:
        module, attr = handlers, 'transform_handler'
    elif 'rules' in kwargs:
        module, attr = handlers, 'validate_handler'
    else:
        pytest.skip('Unknown handler')
        return
    calls = capture_handler(module, attr)
    result = getattr(module, attr)(**kwargs)
    assert result == 0
    for key in expected_keys:
        assert key in calls
