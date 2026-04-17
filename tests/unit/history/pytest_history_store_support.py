"""
:mod:`tests.unit.history.pytest_history_store_support` module.

Shared support for history store unit tests.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from typing import cast

import etlplus.history._store as store_mod

# SECTION: FUNCTIONS ======================================================== #


def normalized_job_run_payload(**overrides: object) -> dict[str, object]:
    """Return one normalized persisted job-run payload."""
    return {
        'duration_ms': None,
        'error_message': None,
        'error_type': None,
        'finished_at': None,
        'job_name': None,
        'pipeline_name': None,
        'records_in': None,
        'records_out': None,
        'result_status': None,
        'result_summary': None,
        'run_id': None,
        'sequence_index': None,
        'skipped_due_to': None,
        'started_at': None,
        'status': None,
    } | overrides


def normalized_run_payload(**overrides: object) -> dict[str, object]:
    """Return one normalized persisted run payload."""
    return {
        'config_path': None,
        'config_sha256': None,
        'duration_ms': None,
        'error_message': None,
        'error_traceback': None,
        'error_type': None,
        'etlplus_version': None,
        'finished_at': None,
        'host': None,
        'job_name': None,
        'pid': None,
        'pipeline_name': None,
        'records_in': None,
        'records_out': None,
        'result_summary': None,
        'run_id': None,
        'started_at': None,
        'status': None,
    } | overrides


def sqlite_run_row(
    record: store_mod.RunRecord,
    /,
    **overrides: object,
) -> dict[str, object]:
    """Return one expected SQLite row for a persisted run record."""
    return {
        **record.to_payload(),
        'record_level': 'run',
        'result_status': None,
        'sequence_index': None,
        'skipped_due_to': None,
    } | overrides


# SECTION: CONTEXT MANAGERS ================================================= #


@contextmanager
def sqlite_row(columns: Mapping[str, object]) -> Iterator[sqlite3.Row]:
    """Yield one SQLite row containing the requested columns and values."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    try:
        columns_sql = ', '.join(f'{name} TEXT' for name in columns)
        placeholders = ', '.join('?' for _ in columns)
        selected_columns = ', '.join(columns)
        conn.execute(f'CREATE TABLE sample ({columns_sql})')
        conn.execute(
            f'INSERT INTO sample ({selected_columns}) VALUES ({placeholders})',
            tuple(columns.values()),
        )
        row = conn.execute(f'SELECT {selected_columns} FROM sample').fetchone()
        assert row is not None
        yield cast(sqlite3.Row, row)
    finally:
        conn.close()


# SECTION: CLASSES ========================================================== #


class MemoryHistoryStore(store_mod.HistoryStore):
    """Small in-memory history-store double for merge and filter tests."""

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    def iter_records(self) -> Iterator[dict[str, object]]:
        """Yield the injected persisted records."""
        yield from self._records

    def record_run_started(self, record: store_mod.RunRecord) -> None:
        _ = record

    def record_run_finished(self, completion: store_mod.RunCompletion) -> None:
        _ = completion

    def record_job_run(self, record: store_mod.JobRunRecord) -> None:
        _ = record
