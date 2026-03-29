"""
:mod:`tests.unit.connector.test_u_connector_core` module.

Unit tests for :mod:`etlplus.connector._core`.
"""

from __future__ import annotations

import pytest

from etlplus.connector import ConnectorFile
from etlplus.connector._core import ConnectorBase
from etlplus.connector._core import ConnectorProtocol

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorCore:
    """Unit tests for connector base/protocol helpers."""

    @pytest.mark.parametrize(
        'payload',
        [
            {},
            {'name': None},
            {'name': 42},
        ],
    )
    def test_require_name_raises_for_invalid_name(
        self,
        payload: dict[str, object],
    ) -> None:
        """
        Test that missing or non-string name values raise :class:`TypeError`.
        """
        with pytest.raises(TypeError, match='ConnectorFile requires a "name"'):
            ConnectorBase._require_name(payload, kind='File')

    def test_require_name_returns_name_when_valid(self) -> None:
        """Test that a valid connector name is returned unchanged."""
        resolved = ConnectorBase._require_name(
            {'name': 'src_file'},
            kind='File',
        )
        assert resolved == 'src_file'

    def test_runtime_protocol_check(self) -> None:
        """
        Test that concrete connector dataclasses satisfy the runtime protocol.
        """
        connector = ConnectorFile.from_obj(
            {
                'name': 'payload_file',
                'type': 'file',
                'path': '/tmp/in.json',
            },
        )
        assert isinstance(connector, ConnectorProtocol)
