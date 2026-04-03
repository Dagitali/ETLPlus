"""
:mod:`tests.unit.workflow.test_u_workflow_profile` module.

Unit tests for :mod:`etlplus.workflow._profile`.
"""

from __future__ import annotations

from typing import Any
from typing import cast

import pytest

from etlplus.workflow._profile import ProfileConfig

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    ('payload', 'expected_env'),
    [
        pytest.param(
            {
                'default_target': 'warehouse',
                'env': {'INT': 1, 'BOOL': False, 'TXT': 'x'},
            },
            {'INT': '1', 'BOOL': 'False', 'TXT': 'x'},
            id='coerces-env-values',
        ),
        pytest.param(
            {'default_target': 'warehouse', 'env': ['bad']},
            {},
            id='ignores-non-mapping-env',
        ),
    ],
)
def test_from_obj_normalizes_env_payload(
    payload: dict[str, object],
    expected_env: dict[str, str],
) -> None:
    """Profile parsing should normalize environment payloads into string mappings."""
    cfg = ProfileConfig.from_obj(payload)
    assert cfg.default_target == 'warehouse'
    assert cfg.env == expected_env


@pytest.mark.parametrize(
    'payload',
    [
        pytest.param('not-a-mapping', id='non-mapping'),
        pytest.param(None, id='none'),
    ],
)
def test_from_obj_returns_defaults_for_non_mappings(
    payload: object | None,
) -> None:
    """Non-mapping profile payloads should produce a default config."""
    assert ProfileConfig.from_obj(cast(Any, payload)) == ProfileConfig()
