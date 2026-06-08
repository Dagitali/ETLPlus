"""
:mod:`tests.integration.database.test_i_database_engine` module.

Integration sanity tests for :mod:`etlplus.database._engine`.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from etlplus.database import make_engine

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: MARKERS ========================================================== #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: TESTS ============================================================ #


@pytest.mark.parametrize(
    'via_session',
    (
        pytest.param(False, id='connection'),
        pytest.param(True, id='session'),
    ),
)
def test_in_memory_sqlite_executes_trivial_query(
    via_session: bool,
) -> None:
    """Test that in-memory sqlite executes SQL via connection and session."""
    engine = make_engine('sqlite+pysqlite:///:memory:')

    if via_session:
        session_factory = sessionmaker(
            bind=engine,
            autoflush=False,
            autocommit=False,
        )
        with session_factory() as db_session:
            assert db_session.execute(text('SELECT 1')).scalar_one() == 1
        return

    with engine.connect() as conn:
        assert conn.execute(text('SELECT 1')).scalar_one() == 1
