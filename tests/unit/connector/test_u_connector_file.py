"""
:mod:`tests.unit.connector.test_u_connector_file` module.

Unit tests for :mod:`etlplus.connector.file`.
"""

from __future__ import annotations

import pytest

from etlplus.connector.enums import DataConnectorType
from etlplus.connector.file import ConnectorFile

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestConnectorFile:
    """Unit tests for :class:`ConnectorFile`."""

    def test_from_obj_accepts_mapping_options(self) -> None:
        """Mapping options should be copied into plain dict form."""
        connector = ConnectorFile.from_obj(
            {
                'name': 'input_json',
                'type': 'file',
                'options': {'encoding': 'utf-8'},
            },
        )
        assert connector.options == {'encoding': 'utf-8'}

    def test_from_obj_parses_file_fields_and_coerces_options(self) -> None:
        """from_obj should parse fields and coerce non-mapping options."""
        connector = ConnectorFile.from_obj(
            {
                'name': 'input_csv',
                'type': 'file',
                'format': 'csv',
                'path': '/tmp/input.csv',
                'options': [('delimiter', ',')],
            },
        )

        assert connector.type is DataConnectorType.FILE
        assert connector.name == 'input_csv'
        assert connector.format == 'csv'
        assert connector.path == '/tmp/input.csv'
        assert not connector.options

    def test_from_obj_requires_name(self) -> None:
        """from_obj should reject mappings without a valid name."""
        with pytest.raises(TypeError, match='ConnectorFile requires a "name"'):
            ConnectorFile.from_obj({'type': 'file'})
