"""
:mod:`tests.unit.cli.test_u_cli_types` module.

Unit tests for :mod:`etlplus.cli._commands._types`.
"""

from __future__ import annotations

from typing import get_args

from etlplus.cli._commands._types import DataConnectorContext

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


def test_data_connector_context_alias_members() -> None:
    """Test that type alias enumerates ``source`` and ``target`` values."""
    alias_value = getattr(
        DataConnectorContext,
        '__value__',
        DataConnectorContext,
    )
    assert set(get_args(alias_value)) == {'source', 'target'}
