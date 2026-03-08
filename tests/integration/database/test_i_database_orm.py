"""
:mod:`tests.integration.database.test_i_database_orm` module.

Integration sanity tests for :mod:`etlplus.database.orm`.
"""

from __future__ import annotations

from typing import Any
from typing import cast

import pytest
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

from etlplus.database.engine import make_engine
from etlplus.database.orm import build_models
from etlplus.database.schema import TableSpec

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKERS ========================================================== #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: CLASSES ========================================================== #


class _IntegrationBase(DeclarativeBase):
    """Local declarative base to isolate integration metadata."""

    __abstract__ = True


# SECTION: TESTS ============================================================ #


def test_build_model_create_table_and_roundtrip_row() -> None:
    """
    Test dynamically building a model, creating the table, and round-tripping
    1 row.
    """
    table_spec = TableSpec.model_validate(
        {
            'name': 'widgets',
            'columns': [
                {'name': 'id', 'type': 'INT', 'nullable': False},
                {'name': 'name', 'type': 'VARCHAR(64)', 'nullable': False},
            ],
            'primary_key': {'columns': ['id']},
        },
    )
    registry = build_models([table_spec], base=_IntegrationBase)
    widgets_model = cast(Any, registry['widgets'])

    engine = make_engine('sqlite+pysqlite:///:memory:')
    _IntegrationBase.metadata.create_all(engine)
    session_factory = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
    )

    with session_factory() as db_session:
        db_session.add(widgets_model(id=1, name='alpha'))
        db_session.commit()

    with session_factory() as db_session:
        row = db_session.get(widgets_model, 1)
        assert row is not None
        assert row.id == 1
        assert row.name == 'alpha'
