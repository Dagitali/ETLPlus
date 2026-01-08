"""
:mod:`tests.unit.database.test_u_database_orm` module.

Unit tests for :mod:`etlplus.database.orm`.
"""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import TypeVar
from typing import cast

import pytest
from sqlalchemy import CheckConstraint
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.sql.schema import Constraint
from sqlalchemy.sql.schema import ForeignKeyConstraint
from sqlalchemy.sql.schema import Table
from sqlalchemy.types import Enum as SAEnum
from sqlalchemy.types import Numeric
from sqlalchemy.types import String
from sqlalchemy.types import Text

from etlplus.database import orm as orm_mod
from etlplus.database.orm import Base
from etlplus.database.orm import build_models
from etlplus.database.orm import load_and_build_models
from etlplus.database.orm import resolve_type
from etlplus.database.schema import TableSpec

# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.unit


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='rich_spec')
def rich_spec_fixture() -> TableSpec:
    """Return a rich table spec covering constraints and options."""

    data = {
        'name': 'orders',
        'schema': 'analytics',
        'create_schema': True,
        'columns': [
            {
                'name': 'id',
                'type': 'BIGINT',
                'nullable': False,
                'identity': {'seed': 1, 'increment': 1},
            },
            {
                'name': 'order_no',
                'type': 'VARCHAR(20)',
                'nullable': False,
                'unique': True,
            },
            {
                'name': 'region',
                'type': 'VARCHAR(2)',
                'check': "region in ('US','EU')",
            },
            {
                'name': 'status',
                'type': 'TEXT',
                'default': "'pending'",
                'enum': ['pending', 'shipped'],
            },
            {
                'name': 'customer_id',
                'type': 'UUID',
                'nullable': False,
            },
        ],
        'primary_key': {'columns': ['id', 'order_no'], 'name': 'pk_orders'},
        'unique_constraints': [
            {'columns': ['order_no'], 'name': 'uq_order_no'},
        ],
        'indexes': [
            {
                'name': 'ix_region',
                'columns': ['region'],
                'unique': False,
                'where': "region = 'US'",
            },
        ],
        'foreign_keys': [
            {
                'columns': ['customer_id'],
                'ref_table': 'customers',
                'ref_columns': ['id'],
                'ondelete': 'CASCADE',
            },
            {
                'columns': ['region', 'order_no'],
                'ref_table': 'regions',
                'ref_columns': ['code', 'order_no'],
                'ondelete': None,
            },
        ],
    }
    return TableSpec.model_validate(data)


@pytest.fixture(name='simple_spec')
def simple_spec_fixture() -> TableSpec:
    """Return a minimal table spec for single-column PK testing."""

    data = {
        'name': 'widgets',
        'columns': [
            {'name': 'id', 'type': 'INT', 'nullable': False},
            {'name': 'name', 'type': 'VARCHAR(50)', 'nullable': False},
        ],
        'primary_key': {'columns': ['id']},
    }
    return TableSpec.model_validate(data)


# SECTION: TESTS ============================================================ #


class TestHelpers:
    """Unit test suite for helper utilities in :mod:`orm`."""

    @pytest.mark.parametrize(
        'type_decl, expected_type, expected_attr',
        [
            ('VARCHAR(255)', String, {'length': 255}),
            ('numeric(10,2)', Numeric, {'precision': 10, 'scale': 2}),
            ('uuid', PG_UUID, {'as_uuid': True}),
            ('unknown_type', Text, {}),
        ],
    )
    def test_resolve_type_mapping(
        self,
        type_decl: str,
        expected_type: type,
        expected_attr: dict[str, object],
    ) -> None:
        """resolve_type returns expected SQLAlchemy types and params."""

        resolved = resolve_type(type_decl)

        assert isinstance(resolved, expected_type)
        for attr, value in expected_attr.items():
            assert getattr(resolved, attr) == value

    @pytest.mark.parametrize(
        'type_str, name, params',
        [
            ('VARCHAR(12)', 'varchar', [12]),
            ('decimal(10, 4)', 'decimal', [10, 4]),
            ('text', 'text', []),
            ('invalid(type', 'invalid(type', []),
        ],
    )
    def test_parse_type_decl_and_class_name(
        self,
        type_str: str,
        name: str,
        params: list[int],
    ) -> None:
        """
        Type parsing returns expected names/params and class names PascalCase.
        """
        # pylint: disable=protected-access

        parsed_name, parsed_params = orm_mod._parse_type_decl(type_str)
        assert parsed_name == name
        assert parsed_params == params

        assert orm_mod._class_name('sales_orders') == 'SalesOrders'
        assert orm_mod._class_name('dbo.Customers') == 'DboCustomers'


class TestBuildModels:
    """Unit tests for :func:`build_models`."""

    _TConstraint = TypeVar('_TConstraint', bound=Constraint)

    def _get_constraint(
        self,
        table: Table,
        constraint_type: type[_TConstraint],
    ) -> list[_TConstraint]:
        return [c for c in table.constraints if isinstance(c, constraint_type)]

    def test_build_models_populates_constraints_and_columns(
        self,
        rich_spec: TableSpec,
    ) -> None:
        """Composite keys, constraints, enums, defaults, and FKs are mapped."""

        registry = build_models([rich_spec])
        model = registry['analytics.orders']
        table = cast(Table, model.__table__)

        assert model.__name__ == 'Orders'
        assert table.schema == 'analytics'
        assert {col.name for col in table.columns} >= {
            'id',
            'order_no',
            'region',
            'status',
            'customer_id',
        }

        pk_constraints = self._get_constraint(table, PrimaryKeyConstraint)
        assert pk_constraints
        assert {col.name for col in pk_constraints[0].columns} == {
            'id',
            'order_no',
        }

        unique_constraints = self._get_constraint(table, UniqueConstraint)
        assert any(c.name == 'uq_order_no' for c in unique_constraints)

        indexes = {str(idx.name): idx for idx in table.indexes}
        assert 'ix_region' in indexes
        assert indexes['ix_region'].unique is False
        assert (
            str(indexes['ix_region'].dialect_options['postgresql']['where'])
            == "region = 'US'"
        )

        check_constraints = self._get_constraint(table, CheckConstraint)
        assert any('ck_orders_region' == c.name for c in check_constraints)

        fk_constraints = self._get_constraint(table, ForeignKeyConstraint)
        assert any(
            {'region', 'order_no'} == {col.name for col in fk.columns}
            for fk in fk_constraints
        )

        customer_fk = next(iter(table.columns['customer_id'].foreign_keys))
        assert customer_fk.target_fullname == 'customers.id'
        assert customer_fk.ondelete == 'CASCADE'

        status_col = table.columns['status']
        assert isinstance(status_col.type, SAEnum)
        assert status_col.type.name == 'orders_status_enum'
        assert status_col.server_default is not None

        id_col = table.columns['id']
        assert id_col.autoincrement is True

    def test_build_models_single_pk_sets_flag_on_column(
        self,
        simple_spec: TableSpec,
    ) -> None:
        """Single-column PK marks the column as primary_key and in registry."""

        registry = build_models([simple_spec])
        model = registry['widgets']
        table = model.__table__

        assert model.__name__ == 'Widgets'
        assert table.primary_key is not None
        assert {col.name for col in table.primary_key} == {'id'}
        assert table.columns['id'].primary_key is True
        assert table.columns['name'].unique is False


class TestLoadAndBuildModels:
    """Unit tests for :func:`load_and_build_models`."""

    def test_load_and_build_models_uses_loader_and_base(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        load_and_build_models delegates to load_table_specs and honors base.
        """

        captured_paths: list[Path] = []

        sample_spec = TableSpec.model_validate(
            {
                'name': 'events',
                'columns': [{'name': 'id', 'type': 'INT', 'nullable': False}],
                'primary_key': {'columns': ['id']},
            },
        )

        def _fake_loader(path: Path) -> list[TableSpec]:
            captured_paths.append(Path(path))
            return [deepcopy(sample_spec)]

        monkeypatch.setattr(orm_mod, 'load_table_specs', _fake_loader)

        class CustomBase(Base):
            """Custom Declarative base for testing base override."""

            __abstract__ = True

        registry = load_and_build_models('events.yml', base=CustomBase)
        model = registry['events']

        assert captured_paths == [Path('events.yml')]
        assert issubclass(model, CustomBase)
        assert model.__tablename__ == 'events'
