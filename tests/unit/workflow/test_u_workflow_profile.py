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
        self,
        payload: dict[str, object],
        expected_env: dict[str, str],
    ) -> None:
        """
        Test that profile parsing normalizes ``env`` payloads into string
        mappings.
        """
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
        self,
        payload: object | None,
    ) -> None:
        """
        Test that non-mapping profile payloads produce a default config.
        """
        assert ProfileConfig.from_obj(payload) == ProfileConfig()

    def test_from_obj_strips_default_target(self) -> None:
        """Padded default target names should normalize to usable identifiers."""
        cfg = ProfileConfig.from_obj({'default_target': '  warehouse  '})

        assert cfg.default_target == 'warehouse'

    @pytest.mark.parametrize(
        'default_target',
        [
            pytest.param('', id='empty'),
            pytest.param('   ', id='whitespace'),
        ],
    )
    def test_from_obj_treats_blank_default_target_as_absent(
        self,
        default_target: str,
    ) -> None:
        """Blank default target names should parse as missing identifiers."""
        cfg = ProfileConfig.from_obj(
            {'default_target': default_target, 'env': {'TXT': 'x'}},
        )

        assert cfg.default_target is None
        assert cfg.env == {'TXT': 'x'}
