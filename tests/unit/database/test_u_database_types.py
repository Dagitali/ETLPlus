"""
:mod:`tests.unit.database.test_u_database_types` module.

Unit tests for :mod:`etlplus.database.types`.
"""

from __future__ import annotations

from sqlalchemy import String

from etlplus.database import types as database_types

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


class TestDatabaseTypesModule:
    """Unit tests for database type aliases module."""

    def test_exports_include_type_aliases(self) -> None:
        """Module should export both public type aliases."""
        assert set(database_types.__all__) == {'ModelRegistry', 'TypeFactory'}

    def test_type_aliases_are_usable_in_runtime_annotations(self) -> None:
        """Aliases should be importable and usable in annotated values."""
        registry: database_types.ModelRegistry = {}

        def build_type(params: list[int]) -> String:
            length = params[0] if params else None
            return String(length=length)

        factory: database_types.TypeFactory = build_type

        assert not registry
        resolved_type = factory([10])
        assert isinstance(resolved_type, String)
        assert resolved_type.length == 10
