"""
:mod:`tests.unit.workflow.test_u_workflow_profile` module.

Unit tests for :mod:`etlplus.workflow._profile`.
"""

from __future__ import annotations

import pytest

from etlplus.workflow._profile import ProfileConfig

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestProfileConfig:
    """Unit tests for workflow profile parsing."""

    @pytest.mark.parametrize(
        ('payload', 'expected_default_target', 'expected_env'),
        [
            pytest.param(
                {
                    'default_target': 'warehouse',
                    'env': {'INT': 1, 'BOOL': False, 'TXT': 'x'},
                },
                'warehouse',
                {'INT': '1', 'BOOL': 'False', 'TXT': 'x'},
                id='coerces-env-values',
            ),
            pytest.param(
                {'default_target': 'warehouse', 'env': ['bad']},
                'warehouse',
                {},
                id='ignores-non-mapping-env',
            ),
            pytest.param(
                {'env': {'TXT': 'x'}},
                None,
                {'TXT': 'x'},
                id='handles-missing-default-target-with-env',
            ),
            pytest.param(
                {'default_target': '  warehouse  '},
                'warehouse',
                {},
                id='strips-default-target',
            ),
            pytest.param(
                {'default_target': '', 'env': {'TXT': 'x'}},
                None,
                {'TXT': 'x'},
                id='empty-default-target',
            ),
            pytest.param(
                {'default_target': '   ', 'env': {'TXT': 'x'}},
                None,
                {'TXT': 'x'},
                id='whitespace-default-target',
            ),
        ],
    )
    def test_from_obj_normalizes_mapping_payload(
        self,
        payload: dict[str, object],
        expected_default_target: str | None,
        expected_env: dict[str, str],
    ) -> None:
        """
        Test that profile parsing normalizes mapping payloads into concrete
        profile fields.
        """
        cfg = ProfileConfig.from_obj(payload)
        assert cfg.default_target == expected_default_target
        assert cfg.env == expected_env

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param('not-a-mapping', id='non-mapping'),
            pytest.param(None, id='none'),
        ],
    )
    def test_from_obj_returns_defaults_for_non_mappings(
        self,
        payload: object | None,
    ) -> None:
        """
        Test that non-mapping profile payloads produce a default config.
        """
        assert ProfileConfig.from_obj(payload) == ProfileConfig()
