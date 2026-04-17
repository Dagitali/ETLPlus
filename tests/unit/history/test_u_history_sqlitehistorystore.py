"""
:mod:`tests.unit.history.test_u_history_sqlitehistorystore` module.

Unit tests for :class:`SQLiteHistoryStore`.
"""

from __future__ import annotations

from pathlib import Path

import etlplus.history._store as store_mod

from .pytest_history_store_support import sqlite_run_row

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestSQLiteHistoryStore:
    """Unit tests for :class:`SQLiteHistoryStore`."""

    def test_initializes_schema_and_meta(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that SQLite history stores create schema and schema-version
        metadata.
        """
        store = store_mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')

        conn = store._connect()
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'",
                )
            }
            schema_version = conn.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'",
            ).fetchone()
        finally:
            conn.close()

        assert {'meta', 'runs'} <= tables
        assert 'job_runs' in tables
        assert schema_version is not None
        assert schema_version[0] == str(store_mod.HISTORY_SCHEMA_VERSION)

    def test_persists_job_run_records(
        self,
        tmp_path: Path,
        sample_job_run_record: store_mod.JobRunRecord,
    ) -> None:
        """
        Test that SQLite job-run persistence round-trips stored job rows.
        """
        store = store_mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')

        store.record_job_run(sample_job_run_record)

        assert list(store.iter_job_runs()) == [sample_job_run_record.to_payload()]

    def test_round_trips_started_record(
        self,
        tmp_path: Path,
        sample_record: store_mod.RunRecord,
    ) -> None:
        """
        Test that SQLite run persistence round-trips started run rows.
        """
        store = store_mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')

        store.record_run_started(sample_record)

        assert list(store.iter_records()) == [sqlite_run_row(sample_record)]

    def test_updates_finished_record(
        self,
        tmp_path: Path,
        sample_record: store_mod.RunRecord,
    ) -> None:
        """
        Test that SQLite run persistence updates started rows with completion
        state.
        """
        store = store_mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')
        completion = store_mod.RunCompletion(
            run_id=sample_record.run_id,
            state=store_mod.RunState(
                status='succeeded',
                finished_at='2026-03-23T00:00:05Z',
                duration_ms=5000,
                result_summary={'rows': 10},
            ),
        )

        store.record_run_started(sample_record)
        store.record_run_finished(completion)

        assert list(store.iter_records()) == [
            sqlite_run_row(
                sample_record,
                status='succeeded',
                finished_at='2026-03-23T00:00:05Z',
                duration_ms=5000,
                result_summary={'rows': 10},
            ),
        ]
