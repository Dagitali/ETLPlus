"""
:mod:`tests.unit.database.test_u_database_schema` module.

Unit tests for :mod:`etlplus.database._schema`.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import etlplus.database._schema as schema_mod
from etlplus.database._schema import ColumnSpec
from etlplus.database._schema import ForeignKeySpec
from etlplus.database._schema import IdentitySpec
from etlplus.database._schema import TableSpec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTIONS: HELPERS ========================================================= #


PayloadFactory = Callable[[dict[str, object]], object]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='sample_spec')
def sample_spec_fixture() -> dict[str, object]:
    """Return a representative table specification mapping."""
    return {
        'name': 'users',
        'schema': 'public',
        'create_schema': True,
        'columns': [
            {
                'name': 'id',
                'type': 'INT',
                'nullable': False,
                'identity': {'seed': 1, 'increment': 1},
            },
            {
                'name': 'email',
                'type': 'VARCHAR(255)',
                'nullable': False,
                'unique': True,
            },
        ],
        'primary_key': {'columns': ['id']},
        'unique_constraints': [
            {'columns': ['email'], 'name': 'uq_users_email'},
        ],
        'indexes': [{'name': 'ix_users_email', 'columns': ['email']}],
        'foreign_keys': [
            {
                'columns': ['id'],
                'ref_table': 'accounts',
                'ref_columns': ['id'],
                'ondelete': 'CASCADE',
            },
        ],
    }


# SECTION: TESTS ============================================================ #


class TestLoadTableSpecs:
    """
    Unit tests for :func:`load_table_specs`.

    Notes
    -----
    Reuses a helper fixture to patch :meth:`File.read` and avoid disk IO.
    """

    @pytest.fixture
    def patch_read_file(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Callable[[Any], None]:
        """
        Return helper that patches the :meth:`read` instance method to return a
        payload.
        """

        def _apply(payload: Any) -> None:
            """Apply the patch to :meth:`File.read` to return the payload."""

            def fake_read(self, *args, **kwargs):
                """Fake :meth:`File.read` method returning the payload."""
                return payload(self.path) if callable(payload) else payload

            monkeypatch.setattr(schema_mod.File, 'read', fake_read)

        return _apply

    def test_empty_payload(
        self,
        patch_read_file: Callable[[Any], None],
    ) -> None:
        """Test that an empty list is returned when the file is empty."""
        patch_read_file(None)
        assert schema_mod.load_table_specs('missing.yml') == []

    def test_empty_table_schemas_payload(
        self,
        patch_read_file: Callable[[Any], None],
    ) -> None:
        """Test that null ``table_schemas`` yields no specs."""
        patch_read_file({'table_schemas': None})
        assert schema_mod.load_table_specs('empty.yml') == []

    @pytest.mark.parametrize(
        ('payload_factory', 'expected_names'),
        [
            (lambda spec: {'table_schemas': [spec]}, ['users']),
            (
                lambda spec: [spec, {**spec, 'name': 'orders'}],
                ['users', 'orders'],
            ),
            (lambda spec: spec, ['users']),
            (
                lambda spec: {**spec},  # dict without table_schemas wrapper
                ['users'],
            ),
        ],
    )
    def test_shapes(
        self,
        payload_factory: PayloadFactory,
        expected_names: list[str],
        sample_spec: dict[str, object],
        patch_read_file: Callable[[Any], None],
    ) -> None:
        """
        Test that supported input shapes coerce to :class:`TableSpec` list.
        """
        captured_paths: list[Path] = []

        def _fake_read_file(path: Path) -> object:
            captured_paths.append(path)
            return payload_factory(deepcopy(sample_spec))

        patch_read_file(_fake_read_file)  # type: ignore[arg-type]

        specs = schema_mod.load_table_specs('input.yml')

        assert [spec.table for spec in specs] == expected_names
        assert captured_paths[0] == Path('input.yml')

    def test_table_schemas_requires_list(
        self,
        patch_read_file: Callable[[Any], None],
    ) -> None:
        """Test that wrapped table schemas must be list-shaped."""
        patch_read_file({'table_schemas': {'name': 'users'}})

        with pytest.raises(TypeError, match='table_schemas must be a list'):
            schema_mod.load_table_specs('bad.yml')


class TestModels:
    """Unit tests for Pydantic models in :mod:`schema`."""

    def test_column_spec_forbids_extra_fields(self) -> None:
        """Test that extra fields are rejected due to ``extra='forbid'``."""
        with pytest.raises(ValidationError):
            ColumnSpec.model_validate(
                {
                    'name': 'id',
                    'type': 'INT',
                    'nullable': False,
                    'unexpected': True,
                },
            )

    @pytest.mark.parametrize(
        ('raw', 'expected'),
        [
            ('set_null', 'SET NULL'),
            ('noaction', 'NO ACTION'),
            ('restrict', 'RESTRICT'),
        ],
    )
    def test_foreign_key_spec_normalizes_ondelete(
        self,
        raw: str,
        expected: str,
    ) -> None:
        """Test that foreign key referential actions normalize to SQL form."""
        spec = ForeignKeySpec.model_validate(
            {
                'columns': ['account_id'],
                'ref_table': 'accounts',
                'ref_columns': ['id'],
                'ondelete': raw,
            },
        )

        assert spec.ondelete == expected

    def test_foreign_key_spec_rejects_invalid_ondelete(self) -> None:
        """Test that unsupported referential actions fail validation."""
        with pytest.raises(ValidationError):
            ForeignKeySpec.model_validate(
                {
                    'columns': ['account_id'],
                    'ref_table': 'accounts',
                    'ref_columns': ['id'],
                    'ondelete': 'explode',
                },
            )

    @pytest.mark.parametrize(
        ('field', 'value'),
        [('seed', 0), ('increment', 0)],
    )
    def test_identity_spec_requires_positive_values(
        self,
        field: str,
        value: int,
    ) -> None:
        """Test that identity seed/increment must be positive integers."""
        payload = {'seed': 1, 'increment': 1}
        payload[field] = value

        with pytest.raises(ValidationError):
            IdentitySpec.model_validate(payload)

    def test_table_spec_aliases_name_and_schema(
        self,
        sample_spec: dict[str, object],
    ) -> None:
        """
        Test that incoming aliases map to attributes with expected defaults.
        """
        spec = TableSpec.model_validate(deepcopy(sample_spec))

        assert spec.table == 'users'
        assert spec.schema_name == 'public'
        assert spec.columns[0].identity is not None
        assert spec.columns[1].unique is True
        assert spec.primary_key is not None
        assert spec.foreign_keys[0].ondelete == 'CASCADE'

    def test_table_spec_defaults_populate_lists(self) -> None:
        """
        Test that optional collections default to empty lists and flags to
        ``False``.
        """
        minimal = TableSpec.model_validate(
            {
                'name': 'events',
                'columns': [{'name': 'id', 'type': 'INTEGER'}],
            },
        )

        assert minimal.create_schema is False
        assert minimal.unique_constraints == []
        assert minimal.indexes == []
        assert minimal.foreign_keys == []
        assert minimal.primary_key is None

    @pytest.mark.parametrize(
        ('schema_name', 'expected'),
        [('public', 'public.users'), (None, 'users')],
    )
    def test_table_spec_fq_name(
        self,
        schema_name: str | None,
        expected: str,
        sample_spec: dict[str, object],
    ) -> None:
        """Test that ``fq_name`` includes schema when provided."""
        spec_data = deepcopy(sample_spec)
        spec_data['schema'] = schema_name
        spec = TableSpec.model_validate(spec_data)

        assert spec.fq_name == expected
        assert spec.create_schema is True
