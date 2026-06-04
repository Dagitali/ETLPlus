"""
:mod:`tests.unit.history.test_u_history_config` module.

Unit tests for :mod:`etlplus.history._config`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

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

    def test_from_obj_strips_blank_string_state_dir(self) -> None:
        """Blank string state directories should parse as absent."""
        config = history_config_mod.HistoryConfig.from_obj(
            {'state_dir': '   ', 'backend': 'jsonl'},
        )

        assert config.state_dir is None
        assert config.backend == 'jsonl'

    def test_from_obj_strips_string_state_dir(self) -> None:
        """String state directories should trim accidental outer whitespace."""
        config = history_config_mod.HistoryConfig.from_obj(
            {'state_dir': '  ./.etlplus-state  '},
        )

        assert config.state_dir == './.etlplus-state'


class TestHistoryConfigModuleHelpers:
    """Unit tests for :mod:`etlplus.history._config` helper functions."""

    @pytest.mark.parametrize(
        ('state_dir', 'expected'),
        [
            (None, history_config_mod.DEFAULT_STATE_DIR),
            ('   ', history_config_mod.DEFAULT_STATE_DIR),
            (Path('~/etlplus-state'), Path('~/etlplus-state').expanduser()),
            ('  ~/etlplus-state  ', Path('~/etlplus-state').expanduser()),
        ],
    )
    def test_resolve_normalizes_explicit_state_dir(
        self,
        state_dir: str | Path | None,
        expected: Path,
    ) -> None:
        """Explicit state directories should normalize consistently."""
        resolved = history_config_mod.ResolvedHistoryConfig.resolve(
            None,
            state_dir=state_dir,
        )

        assert resolved.state_dir == expected
