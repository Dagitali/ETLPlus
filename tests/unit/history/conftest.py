"""Shared fixtures for history unit tests."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from typing import Any

import pytest

import etlplus.history._store as store_mod

from .pytest_history_store_support import MemoryHistoryStore
from .pytest_history_store_support import sqlite_row

# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='history_store_factory')
def history_store_factory_fixture() -> Callable[
    [list[dict[str, object]]],
    MemoryHistoryStore,
]:
    """Return a factory for lightweight in-memory history stores."""
    return MemoryHistoryStore


@pytest.fixture(name='sample_job_run_record')
def sample_job_run_record_fixture() -> store_mod.JobRunRecord:
    """Return one minimal persisted job-run record for store tests."""
    return store_mod.JobRunRecord(
        run_id='run-123',
        job_name='job-a',
        pipeline_name='pipeline-a',
        sequence_index=0,
        started_at='2026-03-23T00:00:00Z',
        finished_at='2026-03-23T00:00:05Z',
        duration_ms=5000,
        records_in=None,
        records_out=None,
        status='succeeded',
        result_status='ok',
        error_type=None,
        error_message=None,
        skipped_due_to=None,
        result_summary={'rows': 10},
    )


@pytest.fixture(name='sample_record')
def sample_record_fixture() -> store_mod.RunRecord:
    """Return one minimal started run record for store tests."""
    return store_mod.RunRecord(
        run_id='run-123',
        pipeline_name='pipeline-a',
        job_name='job-a',
        config_path='pipeline.yml',
        config_sha256='sha256',
        started_at='2026-03-23T00:00:00Z',
        records_in=None,
        records_out=None,
        state=store_mod.RunState(
            status='running',
            finished_at=None,
            duration_ms=None,
        ),
        host='host-a',
        pid=123,
        etlplus_version='1.0.3',
    )


@pytest.fixture(name='sqlite_row_factory')
def sqlite_row_factory_fixture() -> Callable[
    [Mapping[str, object]],
    Iterator[sqlite3.Row] | Any,
]:
    """Return a factory that materializes one temporary SQLite row."""
    return sqlite_row
