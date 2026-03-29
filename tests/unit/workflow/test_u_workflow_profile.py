"""
:mod:`tests.unit.workflow.test_u_workflow_profile` module.

Unit tests for :mod:`etlplus.workflow._profile`.
"""

from __future__ import annotations

import importlib

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


profile = importlib.import_module('etlplus.workflow._profile')


# SECTION: TESTS ============================================================ #


def test_from_obj_coerces_env_values_to_strings() -> None:
    """Test that environment values normalize to strings."""
    cfg = profile.ProfileConfig.from_obj(
        {
            'default_target': 'warehouse',
            'env': {'INT': 1, 'BOOL': False, 'TXT': 'x'},
        },
    )
    assert cfg.default_target == 'warehouse'
    assert cfg.env == {'INT': '1', 'BOOL': 'False', 'TXT': 'x'}


def test_from_obj_ignores_non_mapping_env_values() -> None:
    """Test that non-mapping env payloads normalize to an empty mapping."""
    cfg = profile.ProfileConfig.from_obj(
        {'default_target': 'warehouse', 'env': ['bad']},
    )
    assert cfg.default_target == 'warehouse'
    assert cfg.env == {}


def test_from_obj_returns_defaults_for_non_mappings() -> None:
    """Test that non-mapping payloads produce a default profile config."""
    cfg = profile.ProfileConfig.from_obj('not-a-mapping')
    assert cfg.default_target is None
    assert cfg.env == {}


def test_from_obj_returns_defaults_for_none() -> None:
    """Test that a ``None`` payload produces a default profile config."""
    cfg = profile.ProfileConfig.from_obj(None)
    assert cfg == profile.ProfileConfig()
