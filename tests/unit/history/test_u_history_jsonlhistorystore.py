"""
:mod:`tests.unit.history.test_u_history_jsonlhistorystore` module.

Unit tests for :class:`JsonlHistoryStore`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import etlplus.history._store as store_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestJsonlHistoryStore:
    """Unit tests for :class:`JsonlHistoryStore`."""

    def test_appends_finished_records_as_ndjson(
        self,
        tmp_path: Path,
        sample_completion: store_mod.RunCompletion,
    ) -> None:
        """
        Test that finished records append as one JSON object per NDJSON line.
        """
        path = tmp_path / 'history.jsonl'
        store = store_mod.JsonlHistoryStore(path)

        store.record_run_finished(sample_completion)

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
            'schema_version': store_mod.HISTORY_SCHEMA_VERSION,
            'status': 'succeeded',
        }

    def test_appends_job_run_records_as_ndjson(
        self,
        tmp_path: Path,
        sample_job_run_record: store_mod.JobRunRecord,
    ) -> None:
        """Test that job-run persistence appends one complete NDJSON line."""
        path = tmp_path / 'history.jsonl'
        store = store_mod.JsonlHistoryStore(path)

        store.record_job_run(sample_job_run_record)

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
            'schema_version': store_mod.HISTORY_SCHEMA_VERSION,
            'sequence_index': 0,
            'skipped_due_to': None,
            'started_at': '2026-03-23T00:00:00Z',
            'status': 'succeeded',
        }

    def test_iter_job_runs_merges_jsonl_job_records_by_run_and_job(
        self,
        tmp_path: Path,
        sample_job_run_record: store_mod.JobRunRecord,
    ) -> None:
        """
        Test that job-run iteration yields one normalized persisted job row.
        """
        store = store_mod.JsonlHistoryStore(tmp_path / 'history.jsonl')
        store.record_job_run(sample_job_run_record)

        assert list(store.iter_job_runs()) == [sample_job_run_record.to_payload()]

    def test_iter_runs_merges_append_events_into_one_run(
        self,
        tmp_path: Path,
        sample_completion: store_mod.RunCompletion,
        sample_record: store_mod.RunRecord,
    ) -> None:
        """Test that run iteration merges JSONL start and finish events."""
        store = store_mod.JsonlHistoryStore(tmp_path / 'history.jsonl')

        store.record_run_started(sample_record)
        store.record_run_finished(sample_completion)

        assert list(store.iter_runs()) == [
            sample_record.to_payload()
            | {
                'finished_at': '2026-03-23T00:00:05Z',
                'duration_ms': 5000,
                'result_summary': {'rows': 10},
                'status': 'succeeded',
            },
        ]

    def test_iter_records_returns_empty_when_missing(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that streaming reads are empty when the JSONL history file is
        absent.
        """
        store = store_mod.JsonlHistoryStore(tmp_path / 'missing.jsonl')

        assert not list(store.iter_records())

    def test_iter_records_uses_ndjson_load_line(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that streaming reads delegate through
        :meth:`NdjsonFile.load_line`.
        """
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

        monkeypatch.setattr(store_mod.NdjsonFile, 'load_line', fake_load_line)

        path = tmp_path / 'history.jsonl'
        path.write_text('{"id": 1}\n\n{"id": 2}\n', encoding='utf-8')
        store = store_mod.JsonlHistoryStore(path)

        assert list(store.iter_records()) == [
            {'line': 1, 'payload': '{"id": 1}'},
            {'line': 3, 'payload': '{"id": 2}'},
        ]
        assert captured == [('{"id": 1}', 1), ('{"id": 2}', 3)]

    def test_serializes_started_records_with_ndjson(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sample_record: store_mod.RunRecord,
    ) -> None:
        """
        Test that started records serialize through
        :meth:`NdjsonFile.dump_line`.
        """
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

        monkeypatch.setattr(store_mod.NdjsonFile, 'dump_line', fake_dump_line)

        path = tmp_path / 'history.jsonl'
        store = store_mod.JsonlHistoryStore(path)

        store.record_run_started(sample_record)

        assert captured['data'] == {
            **sample_record.to_payload(),
            'record_level': 'run',
        }
        assert captured['options'] is None
        assert path.read_text(encoding='utf-8') == '{"serialized":true}\n'
