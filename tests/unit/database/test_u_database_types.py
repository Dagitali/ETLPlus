"""
:mod:`tests.unit.database.test_u_database_types` module.

Unit tests for :mod:`etlplus.database._types`.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from sqlalchemy import String
from sqlalchemy.types import TypeEngine

import etlplus.database._types as database_types

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def _build_string_type(params: Sequence[int]) -> TypeEngine[Any]:
    """Build a SQLAlchemy string type from parsed integer parameters."""
    return String(length=params[0] if params else None)


# SECTION: TESTS ============================================================ #


class TestDatabaseTypesModule:
    """Unit tests for database type aliases module."""

    def test_exports_include_type_aliases(self) -> None:
        """Test that module exports both public type aliases."""
        assert set(database_types.__all__) == {'ModelRegistry', 'TypeFactory'}

    @pytest.mark.parametrize(
        ('field', 'expected'),
        [
            pytest.param('registry-empty', True, id='registry-empty'),
            pytest.param('type-class', String, id='type-class'),
            pytest.param('type-length', 10, id='type-length'),
        ],
    )
    def test_type_aliases_are_usable_in_runtime_annotations(
        self,
        field: str,
        expected: object,
    ) -> None:
        """Test that aliases are importable and usable in annotated values."""
        registry: database_types.ModelRegistry = {}
        factory: database_types.TypeFactory = _build_string_type

        resolved_type = factory([10])
        match field:
            case 'registry-empty':
                actual = not registry
            case 'type-class':
                actual = type(resolved_type)
            case 'type-length':
                actual = resolved_type.length
            case _:
                pytest.fail(f'Unsupported field: {field}')
        assert actual == expected
