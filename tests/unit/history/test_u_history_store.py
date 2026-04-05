"""
:mod:`tests.unit.test_u_history_store` module.

Unit tests for :mod:`etlplus.history._store`.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from typing import cast

import pytest

import etlplus.history._store as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: HELPERS ========================================================== #


def build_sample_record() -> mod.RunRecord:
    """Build a minimal run record for store tests."""
    return mod.RunRecord(
        run_id='run-123',
        pipeline_name='pipeline-a',
        job_name='job-a',
        config_path='pipeline.yml',
        config_sha256='sha256',
        started_at='2026-03-23T00:00:00Z',
        records_in=None,
        records_out=None,
        state=mod.RunState(
            status='running',
            finished_at=None,
            duration_ms=None,
        ),
        host='host-a',
        pid=123,
        etlplus_version='1.0.3',
    )


def build_sample_job_run_record() -> mod.JobRunRecord:
    """Build a minimal persisted job-run record for store tests."""
    return mod.JobRunRecord(
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


@contextmanager
def sqlite_row(result_summary: str | None) -> Iterator[sqlite3.Row]:
    """Yield one sqlite row containing a ``result_summary`` column."""
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('CREATE TABLE sample (result_summary TEXT)')
        conn.execute(
            'INSERT INTO sample (result_summary) VALUES (?)',
            (result_summary,),
        )
        row = conn.execute('SELECT result_summary FROM sample').fetchone()
        assert row is not None
        yield cast(sqlite3.Row, row)
    finally:
        conn.close()


# SECTION: TESTS ============================================================ #


def test_build_run_record_delegates_to_run_record_build(
    monkeypatch: Any,
) -> None:
    """build_run_record should delegate straight through to RunRecord.build."""
    captured: dict[str, Any] = {}
    sentinel = object()

    def fake_build(**kwargs: Any) -> object:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(mod.RunRecord, 'build', staticmethod(fake_build))

    result = mod.build_run_record(
        run_id='run-123',
        config_path='pipeline.yml',
        started_at='2026-03-23T00:00:00Z',
        pipeline_name='pipeline-a',
        job_name='job-a',
        status='queued',
    )

    assert result is sentinel
    assert captured == {
        'run_id': 'run-123',
        'config_path': 'pipeline.yml',
        'started_at': '2026-03-23T00:00:00Z',
        'pipeline_name': 'pipeline-a',
        'job_name': 'job-a',
        'status': 'queued',
    }


def test_history_store_coerce_state_dir_defaults_without_environment(
    monkeypatch: Any,
) -> None:
    """State-dir coercion should fall back to the package default."""
    monkeypatch.delenv('ETLPLUS_STATE_DIR', raising=False)

    assert mod.HistoryStore._coerce_state_dir() == Path('~/.etlplus').expanduser()


def test_history_store_abstract_methods_raise_not_implemented() -> None:
    """Abstract base methods should raise NotImplementedError from their bodies."""
    record = build_sample_record()
    job_record = build_sample_job_run_record()
    completion = mod.RunCompletion(run_id='run-123', state=record.state)
    history_store = cast(mod.HistoryStore, object())

    with pytest.raises(NotImplementedError):
        mod.HistoryStore.iter_records(history_store)
    with pytest.raises(NotImplementedError):
        mod.HistoryStore.record_run_started(history_store, record)
    with pytest.raises(NotImplementedError):
        mod.HistoryStore.record_run_finished(history_store, completion)
    with pytest.raises(NotImplementedError):
        mod.HistoryStore.record_job_run(history_store, job_record)


def test_history_store_from_environment_selects_jsonl_backend(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """HistoryStore.from_environment should honor the configured backend."""
    monkeypatch.setenv('ETLPLUS_HISTORY_BACKEND', 'jsonl')
    monkeypatch.setenv('ETLPLUS_STATE_DIR', str(tmp_path))

    store = mod.HistoryStore.from_environment()

    assert isinstance(store, mod.JsonlHistoryStore)
    assert store.log_path == tmp_path / 'history.jsonl'


def test_history_store_from_environment_selects_sqlite_backend(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """HistoryStore.from_environment should default to the SQLite backend."""
    monkeypatch.delenv('ETLPLUS_HISTORY_BACKEND', raising=False)
    monkeypatch.setenv('ETLPLUS_STATE_DIR', str(tmp_path))

    store = mod.HistoryStore.from_environment()

    assert isinstance(store, mod.SQLiteHistoryStore)
    assert store.db_path == tmp_path / 'history.sqlite'


def test_history_store_from_environment_rejects_invalid_backend(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """HistoryStore.from_environment should reject unknown backends."""
    monkeypatch.setenv('ETLPLUS_HISTORY_BACKEND', 'csv')
    monkeypatch.setenv('ETLPLUS_STATE_DIR', str(tmp_path))

    with pytest.raises(ValueError, match='sqlite, jsonl'):
        mod.HistoryStore.from_environment()


def test_history_store_iter_runs_skips_missing_run_ids() -> None:
    """HistoryStore.iter_runs should ignore records without valid run ids."""

    class FakeStore(mod.HistoryStore):
        def iter_records(self) -> Any:
            yield {'run_id': None, 'status': 'ignored'}
            yield {'run_id': '', 'status': 'ignored'}
            yield {'run_id': 'run-123', 'status': 'running'}
            yield {'run_id': 'run-123', 'finished_at': None}

        def record_run_started(self, record: mod.RunRecord) -> None:
            del record

        def record_run_finished(self, completion: mod.RunCompletion) -> None:
            del completion

        def record_job_run(self, record: mod.JobRunRecord) -> None:
            del record

    runs = list(FakeStore().iter_runs())

    assert runs == [
        {
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
            'run_id': 'run-123',
            'started_at': None,
            'status': 'running',
        },
    ]


def test_iter_job_runs_merges_jsonl_job_records_by_run_and_job(
    tmp_path: Path,
) -> None:
    """
    Test that :meth:`HistoryStore.iter_job_runs` yields one normalized
    persisted job row.
    """
    path = tmp_path / 'history.jsonl'
    store = mod.JsonlHistoryStore(path)
    store.record_job_run(build_sample_job_run_record())

    assert list(store.iter_job_runs()) == [
        {
            'run_id': 'run-123',
            'job_name': 'job-a',
            'pipeline_name': 'pipeline-a',
            'sequence_index': 0,
            'started_at': '2026-03-23T00:00:00Z',
            'finished_at': '2026-03-23T00:00:05Z',
            'duration_ms': 5000,
            'records_in': None,
            'records_out': None,
            'status': 'succeeded',
            'result_status': 'ok',
            'error_type': None,
            'error_message': None,
            'skipped_due_to': None,
            'result_summary': {'rows': 10},
        },
    ]


def test_iter_runs_merges_append_events_into_one_run(
    tmp_path: Path,
) -> None:
    """HistoryStore.iter_runs should merge JSONL start/finish events."""
    path = tmp_path / 'history.jsonl'
    store = mod.JsonlHistoryStore(path)
    record = build_sample_record()

    store.record_run_started(record)
    store.record_run_finished(
        mod.RunCompletion(
            run_id=record.run_id,
            state=mod.RunState(
                status='succeeded',
                finished_at='2026-03-23T00:00:05Z',
                duration_ms=5000,
                result_summary={'rows': 10},
            ),
        ),
    )

    assert list(store.iter_runs()) == [
        {
            'config_path': 'pipeline.yml',
            'config_sha256': 'sha256',
            'duration_ms': 5000,
            'error_message': None,
            'error_traceback': None,
            'error_type': None,
            'etlplus_version': '1.0.3',
            'finished_at': '2026-03-23T00:00:05Z',
            'host': 'host-a',
            'job_name': 'job-a',
            'pid': 123,
            'pipeline_name': 'pipeline-a',
            'records_in': None,
            'records_out': None,
            'result_summary': {'rows': 10},
            'run_id': 'run-123',
            'started_at': '2026-03-23T00:00:00Z',
            'status': 'succeeded',
        },
    ]


def test_jsonl_history_store_appends_finished_records_as_ndjson(
    tmp_path: Path,
) -> None:
    """Finished records should be appended as one JSON object per line."""
    path = tmp_path / 'history.jsonl'
    store = mod.JsonlHistoryStore(path)

    store.record_run_finished(
        mod.RunCompletion(
            run_id='run-123',
            state=mod.RunState(
                status='success',
                finished_at='2026-03-23T00:00:05Z',
                duration_ms=5000,
                result_summary={'rows': 10},
            ),
        ),
    )

    lines = path.read_text(encoding='utf-8').splitlines()

    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        'duration_ms': 5000,
        'error_message': None,
        'error_traceback': None,
        'error_type': None,
        'finished_at': '2026-03-23T00:00:05Z',
        'record_level': 'run',
        'result_summary': {'rows': 10},
        'run_id': 'run-123',
        'schema_version': mod.HISTORY_SCHEMA_VERSION,
        'status': 'success',
    }


def test_jsonl_history_store_appends_job_run_records_as_ndjson(
    tmp_path: Path,
) -> None:
    """Test that job-run persistence appends one complete NDJSON line."""
    path = tmp_path / 'history.jsonl'
    store = mod.JsonlHistoryStore(path)

    store.record_job_run(build_sample_job_run_record())

    lines = path.read_text(encoding='utf-8').splitlines()

    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        'duration_ms': 5000,
        'error_message': None,
        'error_type': None,
        'finished_at': '2026-03-23T00:00:05Z',
        'job_name': 'job-a',
        'pipeline_name': 'pipeline-a',
        'record_level': 'job',
        'records_in': None,
        'records_out': None,
        'result_status': 'ok',
        'result_summary': {'rows': 10},
        'run_id': 'run-123',
        'schema_version': mod.HISTORY_SCHEMA_VERSION,
        'sequence_index': 0,
        'skipped_due_to': None,
        'started_at': '2026-03-23T00:00:00Z',
        'status': 'succeeded',
    }


def test_history_store_helper_functions_cover_none_and_json_paths(
    tmp_path: Path,
) -> None:
    """History helper functions should handle missing and JSON payload states."""
    missing_path = tmp_path / 'missing.yml'
    config_path = tmp_path / 'pipeline.yml'
    config_path.write_text('name: pipeline-a\n', encoding='utf-8')
    record = build_sample_record()

    serialized = mod._serialize_result_summary({'rows': 10})

    assert mod._deserialize_result_summary(None) is None
    assert mod._deserialize_result_summary(serialized) == {'rows': 10}
    assert mod._serialize_result_summary(None) is None
    assert mod._file_sha256(str(missing_path)) is None
    assert (
        mod._file_sha256(str(config_path))
        == hashlib.sha256(
            config_path.read_bytes(),
        ).hexdigest()
    )

    payload = mod._sqlite_record_payload(record)
    assert payload['result_summary'] is None

    with sqlite_row('{"rows": 10}') as row:
        assert mod._sqlite_row_payload(row)['result_summary'] == {'rows': 10}

    with sqlite_row(None) as row:
        assert mod._sqlite_row_payload(row)['result_summary'] is None


def test_jsonl_history_store_iter_records_returns_empty_when_missing(
    tmp_path: Path,
) -> None:
    """Streaming reads should be empty when the JSONL history file is absent."""
    store = mod.JsonlHistoryStore(tmp_path / 'missing.jsonl')

    assert not list(store.iter_records())


def test_jsonl_history_store_iter_records_uses_ndjson_load_line(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """Streaming reads should be delegated through NdjsonFile.load_line."""
    captured: list[tuple[str, int | None]] = []

    def fake_load_line(
        _self: object,
        text: str,
        *,
        options: object | None = None,
        line_number: int | None = None,
    ) -> dict[str, Any]:
        _ = options
        captured.append((text, line_number))
        return {'line': line_number, 'payload': text}

    monkeypatch.setattr(mod.NdjsonFile, 'load_line', fake_load_line)

    path = tmp_path / 'history.jsonl'
    path.write_text('{"id": 1}\n\n{"id": 2}\n', encoding='utf-8')
    store = mod.JsonlHistoryStore(path)

    assert list(store.iter_records()) == [
        {'line': 1, 'payload': '{"id": 1}'},
        {'line': 3, 'payload': '{"id": 2}'},
    ]
    assert captured == [('{"id": 1}', 1), ('{"id": 2}', 3)]


def test_jsonl_history_store_serializes_started_records_with_ndjson(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    """Started records should be serialized through NdjsonFile.dump_line."""
    captured: dict[str, Any] = {}

    def fake_dump_line(
        _self: object,
        data: object,
        *,
        options: object | None = None,
    ) -> str:
        captured['data'] = data
        captured['options'] = options
        return '{"serialized":true}\n'

    monkeypatch.setattr(mod.NdjsonFile, 'dump_line', fake_dump_line)

    path = tmp_path / 'history.jsonl'
    store = mod.JsonlHistoryStore(path)
    record = build_sample_record()

    store.record_run_started(record)

    assert captured['data'] == {
        'config_path': 'pipeline.yml',
        'config_sha256': 'sha256',
        'duration_ms': None,
        'error_message': None,
        'error_traceback': None,
        'error_type': None,
        'etlplus_version': '1.0.3',
        'finished_at': None,
        'host': 'host-a',
        'job_name': 'job-a',
        'pid': 123,
        'pipeline_name': 'pipeline-a',
        'record_level': 'run',
        'records_in': None,
        'records_out': None,
        'result_summary': None,
        'run_id': 'run-123',
        'started_at': '2026-03-23T00:00:00Z',
        'status': 'running',
    }
    assert captured['options'] is None
    assert path.read_text(encoding='utf-8') == '{"serialized":true}\n'


def test_run_record_build_populates_runtime_metadata(
    tmp_path: Path,
) -> None:
    """RunRecord.build should populate derived metadata consistently."""
    config_path = tmp_path / 'pipeline.yml'
    config_path.write_text('name: pipeline-a\n', encoding='utf-8')

    record = mod.RunRecord.build(
        run_id='run-123',
        config_path=str(config_path),
        started_at='2026-03-23T00:00:00Z',
        pipeline_name='pipeline-a',
        job_name='job-a',
    )

    assert record.run_id == 'run-123'
    assert record.pipeline_name == 'pipeline-a'
    assert record.job_name == 'job-a'
    assert record.state.status == 'running'
    assert record.config_sha256 == hashlib.sha256(config_path.read_bytes()).hexdigest()
    assert record.host is not None
    assert record.pid is not None


def test_sqlite_history_store_initializes_schema_and_meta(tmp_path: Path) -> None:
    """SQLiteHistoryStore should create its schema and schema-version record."""
    store = mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')

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
    assert schema_version[0] == str(mod.HISTORY_SCHEMA_VERSION)


def test_sqlite_history_store_persists_job_run_records(tmp_path: Path) -> None:
    """
    Test that :class:`SQLiteHistoryStore` persists and reads back job-run
    records.
    """
    store = mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')
    job_record = build_sample_job_run_record()

    store.record_job_run(job_record)

    assert list(store.iter_job_runs()) == [job_record.to_payload()]


def test_sqlite_history_store_round_trips_started_record(tmp_path: Path) -> None:
    """SQLiteHistoryStore should persist and read back started run records."""
    store = mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')
    record = build_sample_record()

    store.record_run_started(record)

    assert list(store.iter_records()) == [
        {
            **record.to_payload(),
            'config_path': 'pipeline.yml',
            'config_sha256': 'sha256',
            'host': 'host-a',
            'pid': 123,
            'etlplus_version': '1.0.3',
            'record_level': 'run',
            'result_status': None,
            'sequence_index': None,
            'skipped_due_to': None,
        },
    ]


def test_sqlite_history_store_updates_finished_record(tmp_path: Path) -> None:
    """SQLiteHistoryStore should update an existing run with completion state."""
    store = mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')
    record = build_sample_record()
    completion = mod.RunCompletion(
        run_id=record.run_id,
        state=mod.RunState(
            status='succeeded',
            finished_at='2026-03-23T00:00:05Z',
            duration_ms=5000,
            result_summary={'rows': 10},
        ),
    )

    store.record_run_started(record)
    store.record_run_finished(completion)

    assert list(store.iter_records()) == [
        {
            **record.to_payload(),
            'status': 'succeeded',
            'finished_at': '2026-03-23T00:00:05Z',
            'duration_ms': 5000,
            'result_summary': {'rows': 10},
            'record_level': 'run',
            'result_status': None,
            'sequence_index': None,
            'skipped_due_to': None,
        },
    ]
