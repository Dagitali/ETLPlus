"""
:mod:`tests.unit.cli.test_u_cli_types` module.

Unit tests for :mod:`etlplus.cli.types`.
"""

from __future__ import annotations

from typing import get_args

from etlplus.cli.types import DataConnectorContext

# SECTION: TESTS ============================================================ #


def test_data_connector_context_alias_members() -> None:
    """Type alias should enumerate ``source`` and ``target`` values."""
    alias_value = DataConnectorContext.__value__
    assert set(get_args(alias_value)) == {'source', 'target'}
