"""
:mod:`tests.unit.connector.test_u_connector_file` module.

Unit tests for :mod:`etlplus.connector._file`.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from etlplus.connector._enums import DataConnectorType
from etlplus.connector._file import ConnectorFile

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _assert_fields(actual: object, expected: Mapping[str, object]) -> None:
    """Assert that *actual* exposes the expected field values."""
    for field, value in expected.items():
        assert getattr(actual, field) == value


# SECTION: TESTS ============================================================ #


class TestConnectorFile:
    """Unit tests for :class:`ConnectorFile`."""

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(
                {
                    'name': 'input_json',
                    'type': 'file',
                    'options': {'encoding': 'utf-8'},
                },
                {
                    'type': DataConnectorType.FILE,
                    'name': 'input_json',
                    'format': None,
                    'path': None,
                    'options': {'encoding': 'utf-8'},
                },
                id='mapping-options',
            ),
            pytest.param(
                {
                    'name': 'input_csv',
                    'type': 'file',
                    'format': 'csv',
                    'path': '/tmp/input.csv',
                    'options': [('delimiter', ',')],
                },
                {
                    'type': DataConnectorType.FILE,
                    'name': 'input_csv',
                    'format': 'csv',
                    'path': '/tmp/input.csv',
                    'options': {},
                },
                id='coerces-non-mapping-options',
            ),
        ],
    )
    def test_from_obj_normalizes_file_fields(
        self,
        payload: dict[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that :meth:`from_obj` preserves fields and normalizes options.
        """
        connector = ConnectorFile.from_obj(payload)
        _assert_fields(connector, expected)

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param({'type': 'file'}, id='missing-name'),
            pytest.param({'name': None, 'type': 'file'}, id='non-string-name'),
        ],
    )
    def test_from_obj_requires_name(
        self,
        payload: dict[str, object],
    ) -> None:
        """
        Test that :meth:`from_obj` rejects mappings with missing or invalid names.
        """
        with pytest.raises(TypeError, match='ConnectorFile requires a "name"'):
            ConnectorFile.from_obj(payload)
