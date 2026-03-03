"""
:mod:`tests.unit.connector.test_u_connector_core` module.

Unit tests for :mod:`etlplus.connector.core`.
"""

from __future__ import annotations

import pytest

from etlplus.connector.core import ConnectorBase
from etlplus.connector.core import ConnectorProtocol
from etlplus.connector.file import ConnectorFile

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
        """Missing or non-string name values should raise ``TypeError``."""
        with pytest.raises(TypeError, match='ConnectorFile requires a "name"'):
            ConnectorBase._require_name(payload, kind='File')

    def test_require_name_returns_name_when_valid(self) -> None:
        """Valid connector name should be returned unchanged."""
        resolved = ConnectorBase._require_name(
            {'name': 'src_file'},
            kind='File',
        )
        assert resolved == 'src_file'

    def test_runtime_protocol_check(self) -> None:
        """Concrete connector dataclasses satisfy the runtime protocol."""
        connector = ConnectorFile.from_obj(
            {
                'name': 'payload_file',
                'type': 'file',
                'path': '/tmp/in.json',
            },
        )
        assert isinstance(connector, ConnectorProtocol)
