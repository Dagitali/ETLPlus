"""
:mod:`tests.unit.utils.test_u_utils_types` module.

Unit tests for :mod:`etlplus.utils._types`.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest
from pydantic import BaseModel
from pydantic import ValidationError

from etlplus.utils import _types as core_types

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestTypesModule:
    """Unit tests for exported aliases in :mod:`etlplus.utils._types`."""

    def test_aliases_are_usable_in_annotations(self) -> None:
        """Test that aliases are importable and usable in typed values."""
        record: core_types.Record = {'id': 1}
        records: core_types.Records = [record]
        path: core_types.StrPath = Path('in/data.json')
        template: core_types.TemplateKey = 'ddl'

        assert records[0]['id'] == 1
        assert Path(path).suffix == '.json'
        assert template == 'ddl'

    def test_exports_include_expected_aliases(self) -> None:
        """Test that ``__all__`` includes the documented public aliases."""
        expected = {
            'JSONData',
            'JSONDict',
            'JSONList',
            'JSONRecord',
            'JSONRecords',
            'JSONScalar',
            'JSONValue',
            'NonEmptyStr',
            'NonEmptyStrList',
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
        """
        Test that ``JSONValue`` alias supports recursive list/object values.
        """
        value: core_types.JSONValue = {
            'a': [1, 'x', {'b': True}],
            'c': None,
        }
        typed = cast(dict[str, object], value)
        assert typed['c'] is None

    def test_non_empty_validation_aliases_work_with_pydantic(self) -> None:
        """Test shared non-empty aliases enforce Pydantic constraints."""

        class Model(BaseModel):
            name: core_types.NonEmptyStr
            columns: core_types.NonEmptyStrList

        model = Model.model_validate({'name': 'users', 'columns': ['id']})

        assert model.name == 'users'
        assert model.columns == ['id']

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param({'name': '', 'columns': ['id']}, id='empty-string'),
            pytest.param({'name': 'users', 'columns': []}, id='empty-list'),
            pytest.param({'name': 'users', 'columns': ['']}, id='empty-item'),
        ],
    )
    def test_non_empty_validation_aliases_reject_invalid_values(
        self,
        payload: dict[str, object],
    ) -> None:
        """Test shared non-empty aliases reject blank or empty values."""

        class Model(BaseModel):
            name: core_types.NonEmptyStr
            columns: core_types.NonEmptyStrList

        with pytest.raises(ValidationError):
            Model.model_validate(payload)
