"""
:mod:`tests.integration.database.test_i_database_engine` module.

Integration sanity tests for :mod:`etlplus.database.engine`.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from etlplus.database.engine import make_engine

# SECTION: MARKERS ========================================================== #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: TESTS ============================================================ #


def test_in_memory_sqlite_engine_connection_sanity() -> None:
    """In-memory sqlite engine should execute a trivial query."""
    engine = make_engine('sqlite+pysqlite:///:memory:')
    with engine.connect() as conn:
        assert conn.execute(text('SELECT 1')).scalar_one() == 1


def test_in_memory_sqlite_session_sanity() -> None:
    """Session bound to in-memory sqlite engine should execute SQL."""
    engine = make_engine('sqlite+pysqlite:///:memory:')
    session_factory = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
    )
    with session_factory() as db_session:
        assert db_session.execute(text('SELECT 1')).scalar_one() == 1
