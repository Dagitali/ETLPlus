"""
:mod:`tests.unit.connector.test_u_connector_core` module.

Unit tests for :mod:`etlplus.connector._core`.
"""

from __future__ import annotations

from etlplus.connector import ConnectorFile
from etlplus.connector._core import ConnectorProtocol

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorCore:
    """Unit tests for connector base/protocol helpers."""

    def test_concrete_connector_satisfies_runtime_protocol(self) -> None:
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
