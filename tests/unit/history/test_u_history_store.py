"""
:mod:`tests.unit.test_u_history_store` module.

Unit tests for :mod:`etlplus.history._store`.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import etlplus.history._store as mod

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


# SECTION: TESTS ============================================================ #


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
        'result_summary': {'rows': 10},
        'run_id': 'run-123',
        'schema_version': mod.HISTORY_SCHEMA_VERSION,
        'status': 'success',
    }


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
