"""
:mod:`tests.unit.history.test_u_history_config` module.

Unit tests for :mod:`etlplus.history._config`.
"""

from __future__ import annotations

from pathlib import Path

import etlplus.history._config as history_config_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHistoryConfig:
    """Unit tests for :class:`HistoryConfig`."""

    def test_from_obj_returns_defaults_for_non_mapping_input(self) -> None:
        """Non-mapping inputs should fall back to the default config object."""
        assert history_config_mod.HistoryConfig.from_obj(None) == (
            history_config_mod.HistoryConfig()
        )

    def test_from_obj_coerces_pathlike_state_dir_and_string_flags(self) -> None:
        """Parsing should stringify paths and parse common string booleans."""
        config = history_config_mod.HistoryConfig.from_obj(
            {
                'enabled': 'yes',
                'backend': 'JSONL',
                'state_dir': Path('state-dir'),
                'capture_tracebacks': 'no',
            },
        )

        assert config == history_config_mod.HistoryConfig(
            enabled=True,
            backend='jsonl',
            state_dir=str(Path('state-dir')),
            capture_tracebacks=False,
        )


class TestHistoryConfigModuleHelpers:
    """Unit tests for :mod:`etlplus.history._config` helper functions."""

    def test_resolve_normalizes_explicit_pathlike_state_dir(self) -> None:
        """Explicit state directories should normalize through :class:`Path`."""
        resolved = history_config_mod.ResolvedHistoryConfig.resolve(
            None,
            state_dir=Path('~/etlplus-state'),
        )

        assert resolved.state_dir == Path('~/etlplus-state').expanduser()
