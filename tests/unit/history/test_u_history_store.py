"""
:mod:`tests.unit.test_u_history_store` module.

Unit tests for :mod:`etlplus.history.store`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from etlplus.history import store as mod

# SECTION: HELPERS ========================================================== #


def build_sample_record() -> mod.RunRecord:
    """Build a minimal run record for store tests."""
    return mod.RunRecord(
        run_id='run-123',
        pipeline_name='pipeline-a',
        job_name='job-a',
        config_path='pipeline.yml',
        config_sha256='sha256',
        status='running',
        started_at='2026-03-23T00:00:00Z',
        finished_at=None,
        duration_ms=None,
        records_in=None,
        records_out=None,
        error_type=None,
        error_message=None,
        error_traceback=None,
        result_summary=None,
        host='host-a',
        pid=123,
        etlplus_version='1.0.3',
    )


# SECTION: TESTS ============================================================ #


def test_jsonl_history_store_appends_finished_records_as_ndjson(
    tmp_path: Path,
) -> None:
    """Finished records should be appended as one JSON object per line."""
    path = tmp_path / 'history.jsonl'
    store = mod.JsonlHistoryStore(path)

    store.record_run_finished(
        'run-123',
        status='success',
        finished_at='2026-03-23T00:00:05Z',
        duration_ms=5000,
        result_summary={'rows': 10},
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
