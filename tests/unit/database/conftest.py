"""
:mod:`tests.unit.database.conftest` module.

Shared fixtures for database unit tests.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from etlplus.database._schema import TableSpec

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: TYPE ALIASES ===================================================== #


type FileReadPatcher = Callable[[Any, Any], None]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='ddl_sample_spec')
def ddl_sample_spec_fixture() -> dict[str, object]:
    """Return a representative DDL table specification mapping."""
    return {
        'schema': 'dbo',
        'table': 'widgets',
        'create_schema': False,
        'columns': [
            {
                'name': 'id',
                'type': 'INT',
                'nullable': False,
                'identity': {'seed': 1, 'increment': 1},
            },
            {'name': 'name', 'type': 'NVARCHAR(255)', 'nullable': True},
        ],
        'primary_key': {'columns': ['id']},
        'indexes': [
            {'name': 'IX_widgets_name', 'columns': ['name'], 'unique': True},
        ],
        'foreign_keys': [],
    }


@pytest.fixture(name='patch_file_read')
def patch_file_read_fixture(monkeypatch: pytest.MonkeyPatch) -> FileReadPatcher:
    """Patch a file wrapper class so ``read`` returns a test payload."""

    def _patch(file_cls: Any, payload: Any) -> None:
        def fake_read(self: Any, *_args: Any, **_kwargs: Any) -> Any:
            return payload(self.path) if callable(payload) else payload

        monkeypatch.setattr(file_cls, 'read', fake_read)

    return _patch


@pytest.fixture(name='rich_spec')
def rich_spec_fixture() -> TableSpec:
    """Return a rich table spec covering constraints and options."""
    return TableSpec.model_validate(
        {
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
                {'name': 'customer_id', 'type': 'UUID', 'nullable': False},
            ],
            'primary_key': {'columns': ['id', 'order_no'], 'name': 'pk_orders'},
            'unique_constraints': [{'columns': ['order_no'], 'name': 'uq_order_no'}],
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
        },
    )


@pytest.fixture(name='schema_sample_spec')
def schema_sample_spec_fixture() -> dict[str, object]:
    """Return a representative schema table specification mapping."""
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
        'unique_constraints': [{'columns': ['email'], 'name': 'uq_users_email'}],
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


@pytest.fixture(name='simple_spec')
def simple_spec_fixture() -> TableSpec:
    """Return a minimal table spec for single-column PK testing."""
    return TableSpec.model_validate(
        {
            'name': 'widgets',
            'columns': [
                {'name': 'id', 'type': 'INT', 'nullable': False},
                {'name': 'name', 'type': 'VARCHAR(50)', 'nullable': False},
            ],
            'primary_key': {'columns': ['id']},
        },
    )
