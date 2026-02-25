"""
:mod:`tests.unit.test_u_types` module.

Unit tests for :mod:`etlplus.types`.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from etlplus import types as core_types


class TestTypesModule:
    """Unit tests for exported aliases in :mod:`etlplus.types`."""

    def test_aliases_are_usable_in_annotations(self) -> None:
        """Aliases should be importable and usable in typed values."""
        record: core_types.Record = {'id': 1}
        records: core_types.Records = [record]
        path: core_types.StrPath = Path('in/data.json')
        template: core_types.TemplateKey = 'ddl'

        assert records[0]['id'] == 1
        assert Path(path).suffix == '.json'
        assert template == 'ddl'

    def test_exports_include_expected_aliases(self) -> None:
        """__all__ should include the documented public aliases."""
        expected = {
            'JSONData',
            'JSONDict',
            'JSONList',
            'JSONRecord',
            'JSONRecords',
            'JSONScalar',
            'JSONValue',
            'Record',
            'Records',
            'Sleeper',
            'StrAnyMap',
            'StrPath',
            'StrSeqMap',
            'StrStrMap',
            'TemplateKey',
            'Timeout',
        }
        assert expected.issubset(set(core_types.__all__))

    def test_json_value_supports_nested_structures(self) -> None:
        """JSONValue alias should support recursive list/object values."""
        value: core_types.JSONValue = {
            'a': [1, 'x', {'b': True}],
            'c': None,
        }
        typed = cast(dict[str, object], value)
        assert typed['c'] is None
