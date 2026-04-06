"""
:mod:`tests.unit.history.test_u_history_store` module.

Unit tests for :mod:`etlplus.history._store`.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from typing import cast

import pytest

import etlplus.history._store as mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


pytestmark = pytest.mark.unit


# SECTION: HELPERS ========================================================== #


def _normalized_job_run_payload(**overrides: object) -> dict[str, object]:
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


def _normalized_run_payload(**overrides: object) -> dict[str, object]:
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


def _sqlite_run_row(
    record: mod.RunRecord,
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


class _MemoryHistoryStore(mod.HistoryStore):
    """Small in-memory history-store double for merge and filter tests."""

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    def iter_records(self) -> Iterator[dict[str, object]]:
        """Yield the injected persisted records."""
        yield from self._records

    def record_run_started(self, record: mod.RunRecord) -> None:
        _ = record

    def record_run_finished(self, completion: mod.RunCompletion) -> None:
        _ = completion

    def record_job_run(self, record: mod.JobRunRecord) -> None:
        _ = record


@contextmanager
def _sqlite_row(columns: Mapping[str, object]) -> Iterator[sqlite3.Row]:
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


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='history_store_factory')
def history_store_factory_fixture() -> Callable[
    [list[dict[str, object]]],
    _MemoryHistoryStore,
]:
    """Return a factory for lightweight in-memory history stores."""
    return _MemoryHistoryStore


@pytest.fixture(name='sample_record')
def sample_record_fixture() -> mod.RunRecord:
    """Return one minimal started run record for store tests."""
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


@pytest.fixture(name='sample_job_run_record')
def sample_job_run_record_fixture() -> mod.JobRunRecord:
    """Return one minimal persisted job-run record for store tests."""
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


@pytest.fixture(name='sqlite_row_factory')
def sqlite_row_factory_fixture() -> Callable[[Mapping[str, object]], Any]:
    """Return a factory that materializes one temporary SQLite row."""
    return _sqlite_row


# SECTION: TESTS ============================================================ #


class TestModuleHelpers:
    """Unit tests for module-level history-store helper functions."""

    def test_build_run_record_delegates_to_run_record_build(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that :func:`build_run_record` delegates straight through to
        :meth:`RunRecord.build`.
        """
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

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(None, None, id='missing-result-summary'),
            pytest.param('{"rows":10}', {'rows': 10}, id='json-result-summary'),
        ],
    )
    def test_deserialize_result_summary(
        self,
        payload: str | None,
        expected: object,
    ) -> None:
        """
        Test that result summaries decode JSON and preserve missing values.
        """
        assert mod._deserialize_result_summary(payload) == expected

    @pytest.mark.parametrize(
        ('payload', 'expected'),
        [
            pytest.param(None, None, id='none'),
            pytest.param('["seed", 1, "publish"]', ['seed', 'publish'], id='list'),
            pytest.param('{"seed": true}', None, id='non-list-json'),
        ],
    )
    def test_string_list_serialization_helpers(
        self,
        payload: str | None,
        expected: list[str] | None,
    ) -> None:
        """
        Test that string-list helpers deserialize only JSON lists of strings.
        """
        assert mod._deserialize_string_list(payload) == expected

    def test_serialize_string_list_returns_json_when_values_present(self) -> None:
        """Test that string-list serialization emits stable persisted JSON."""
        assert mod._serialize_string_list(['seed', 'publish']) == '["seed","publish"]'

    def test_file_sha256_returns_none_for_missing_path(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that SHA-256 lookup returns `None` when the file does not exist.
        """
        assert mod._file_sha256(str(tmp_path / 'missing.yml')) is None

    def test_file_sha256_returns_digest_for_existing_file(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that SHA-256 lookup hashes existing config files exactly once.
        """
        config_path = tmp_path / 'pipeline.yml'
        config_path.write_text('name: pipeline-a\n', encoding='utf-8')

        assert (
            mod._file_sha256(str(config_path))
            == hashlib.sha256(
                config_path.read_bytes(),
            ).hexdigest()
        )

    def test_sqlite_record_payload_serializes_missing_result_summary(
        self,
        sample_record: mod.RunRecord,
    ) -> None:
        """
        Test that SQLite run payloads preserve missing result summaries.
        """
        assert mod._sqlite_record_payload(sample_record)['result_summary'] is None

    @pytest.mark.parametrize(
        ('columns', 'expected'),
        [
            pytest.param(
                {'result_summary': '{"rows": 10}'},
                {'result_summary': {'rows': 10}},
                id='run-result-summary',
            ),
            pytest.param(
                {'result_summary': None},
                {'result_summary': None},
                id='missing-result-summary',
            ),
            pytest.param(
                {'skipped_due_to': '["seed"]'},
                {'skipped_due_to': ['seed']},
                id='job-skipped-due-to',
            ),
        ],
    )
    def test_sqlite_row_payload_decodes_optional_json_fields(
        self,
        sqlite_row_factory: Callable[[Mapping[str, object]], Any],
        columns: Mapping[str, object],
        expected: dict[str, object],
    ) -> None:
        """
        Test that SQLite row decoding deserializes optional JSON columns when
        present.
        """
        with sqlite_row_factory(columns) as row:
            assert mod._sqlite_row_payload(row) == expected

    def test_sqlite_job_run_payload_serializes_nested_optional_fields(self) -> None:
        """Test that SQLite job payloads JSON-encode nested summary and skip fields."""
        payload = mod._sqlite_job_run_payload(
            mod.JobRunRecord(
                run_id='run-123',
                job_name='publish',
                pipeline_name='pipeline-a',
                sequence_index=1,
                started_at=None,
                finished_at=None,
                duration_ms=None,
                records_in=None,
                records_out=None,
                status='skipped',
                skipped_due_to=['seed'],
                result_summary={'rows': 0},
            ),
        )

        assert payload['skipped_due_to'] == '["seed"]'
        assert payload['result_summary'] == '{"rows":0}'


class TestHistoryStoreBase:
    """Unit tests for the `HistoryStore` base class and merge helpers."""

    def test_history_store_coerce_state_dir_defaults_without_environment(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that state directory coercion falls back to the package default.
        """
        monkeypatch.delenv('ETLPLUS_STATE_DIR', raising=False)

        assert mod.HistoryStore._coerce_state_dir() == Path('~/.etlplus').expanduser()

    def test_history_store_abstract_methods_raise_not_implemented(
        self,
        sample_job_run_record: mod.JobRunRecord,
        sample_record: mod.RunRecord,
    ) -> None:
        """Test that abstract base-method bodies raise `NotImplementedError`."""
        completion = mod.RunCompletion(
            run_id=sample_record.run_id,
            state=sample_record.state,
        )
        history_store = cast(mod.HistoryStore, object())

        with pytest.raises(NotImplementedError):
            mod.HistoryStore.iter_records(history_store)
        with pytest.raises(NotImplementedError):
            mod.HistoryStore.record_run_started(history_store, sample_record)
        with pytest.raises(NotImplementedError):
            mod.HistoryStore.record_run_finished(history_store, completion)
        with pytest.raises(NotImplementedError):
            mod.HistoryStore.record_job_run(history_store, sample_job_run_record)

    @pytest.mark.parametrize(
        ('backend', 'expected_type', 'path_attr', 'path_name'),
        [
            pytest.param(
                'jsonl',
                mod.JsonlHistoryStore,
                'log_path',
                'history.jsonl',
                id='jsonl-backend',
            ),
            pytest.param(
                None,
                mod.SQLiteHistoryStore,
                'db_path',
                'history.sqlite',
                id='default-sqlite-backend',
            ),
        ],
    )
    def test_history_store_from_environment_selects_supported_backend(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        backend: str | None,
        expected_type: type[mod.HistoryStore],
        path_attr: str,
        path_name: str,
    ) -> None:
        """
        Test that environment-based backend selection resolves supported
        stores.
        """
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(tmp_path))
        if backend is None:
            monkeypatch.delenv('ETLPLUS_HISTORY_BACKEND', raising=False)
        else:
            monkeypatch.setenv('ETLPLUS_HISTORY_BACKEND', backend)

        store = mod.HistoryStore.from_environment()

        assert isinstance(store, expected_type)
        assert getattr(store, path_attr) == tmp_path / path_name

    def test_history_store_from_environment_rejects_invalid_backend(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unknown persisted-history backends fail fast."""
        monkeypatch.setenv('ETLPLUS_HISTORY_BACKEND', 'csv')
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(tmp_path))

        with pytest.raises(ValueError, match='sqlite, jsonl'):
            mod.HistoryStore.from_environment()

    def test_history_store_iter_runs_skips_missing_run_ids(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], _MemoryHistoryStore],
    ) -> None:
        """Test that run iteration ignores records without valid run identifiers."""
        runs = list(
            history_store_factory(
                [
                    {'run_id': None, 'status': 'ignored'},
                    {'run_id': '', 'status': 'ignored'},
                    {'run_id': 'run-123', 'status': 'running'},
                    {'run_id': 'run-123', 'finished_at': None},
                ],
            ).iter_runs(),
        )

        assert runs == [
            _normalized_run_payload(
                run_id='run-123',
                status='running',
            ),
        ]

    def test_history_store_iter_runs_skips_non_run_records(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], _MemoryHistoryStore],
    ) -> None:
        """
        Test that run iteration ignores persisted job-level records entirely.
        """
        runs = list(
            history_store_factory(
                [
                    {
                        'record_level': 'job',
                        'run_id': 'run-ignored',
                        'status': 'succeeded',
                    },
                    {
                        'record_level': 'run',
                        'run_id': 'run-123',
                        'status': 'running',
                    },
                ],
            ).iter_runs(),
        )

        assert runs == [
            _normalized_run_payload(
                run_id='run-123',
                status='running',
            ),
        ]

    def test_history_store_iter_job_runs_skips_invalid_or_incomplete_keys(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], _MemoryHistoryStore],
    ) -> None:
        """
        Test that job-run iteration ignores malformed or non-job persisted rows.
        """
        job_runs = list(
            history_store_factory(
                [
                    {
                        'record_level': 'run',
                        'run_id': 'run-parent',
                        'job_name': 'seed',
                        'status': 'ignored',
                    },
                    {
                        'record_level': 'job',
                        'run_id': None,
                        'job_name': 'seed',
                        'status': 'ignored',
                    },
                    {
                        'record_level': 'job',
                        'run_id': 'run-parent',
                        'job_name': '',
                        'status': 'ignored',
                    },
                    {
                        'record_level': 'job',
                        'run_id': 'run-parent',
                        'job_name': 'seed',
                        'status': 'running',
                        'sequence_index': 0,
                    },
                    {
                        'record_level': 'job',
                        'run_id': 'run-parent',
                        'job_name': 'seed',
                        'status': 'succeeded',
                        'sequence_index': 0,
                        'result_status': 'ok',
                    },
                ],
            ).iter_job_runs(),
        )

        assert job_runs == [
            _normalized_job_run_payload(
                run_id='run-parent',
                job_name='seed',
                sequence_index=0,
                status='succeeded',
                result_status='ok',
            ),
        ]


class TestJsonlHistoryStore:
    """Unit tests for the JSONL persisted-history backend."""

    def test_iter_job_runs_merges_jsonl_job_records_by_run_and_job(
        self,
        tmp_path: Path,
        sample_job_run_record: mod.JobRunRecord,
    ) -> None:
        """
        Test that job-run iteration yields one normalized persisted job row.
        """
        store = mod.JsonlHistoryStore(tmp_path / 'history.jsonl')
        store.record_job_run(sample_job_run_record)

        assert list(store.iter_job_runs()) == [sample_job_run_record.to_payload()]

    def test_iter_runs_merges_append_events_into_one_run(
        self,
        tmp_path: Path,
        sample_record: mod.RunRecord,
    ) -> None:
        """Test that run iteration merges JSONL start and finish events."""
        store = mod.JsonlHistoryStore(tmp_path / 'history.jsonl')

        store.record_run_started(sample_record)
        store.record_run_finished(
            mod.RunCompletion(
                run_id=sample_record.run_id,
                state=mod.RunState(
                    status='succeeded',
                    finished_at='2026-03-23T00:00:05Z',
                    duration_ms=5000,
                    result_summary={'rows': 10},
                ),
            ),
        )

        assert list(store.iter_runs()) == [
            sample_record.to_payload()
            | {
                'finished_at': '2026-03-23T00:00:05Z',
                'duration_ms': 5000,
                'result_summary': {'rows': 10},
                'status': 'succeeded',
            },
        ]

    def test_jsonl_history_store_appends_finished_records_as_ndjson(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that finished records append as one JSON object per NDJSON line.
        """
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
        self,
        tmp_path: Path,
        sample_job_run_record: mod.JobRunRecord,
    ) -> None:
        """Test that job-run persistence appends one complete NDJSON line."""
        path = tmp_path / 'history.jsonl'
        store = mod.JsonlHistoryStore(path)

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
            'schema_version': mod.HISTORY_SCHEMA_VERSION,
            'sequence_index': 0,
            'skipped_due_to': None,
            'started_at': '2026-03-23T00:00:00Z',
            'status': 'succeeded',
        }

    def test_jsonl_history_store_iter_records_returns_empty_when_missing(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that streaming reads are empty when the JSONL history file is
        absent.
        """
        store = mod.JsonlHistoryStore(tmp_path / 'missing.jsonl')

        assert not list(store.iter_records())

    def test_jsonl_history_store_iter_records_uses_ndjson_load_line(
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
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        sample_record: mod.RunRecord,
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

        monkeypatch.setattr(mod.NdjsonFile, 'dump_line', fake_dump_line)

        path = tmp_path / 'history.jsonl'
        store = mod.JsonlHistoryStore(path)

        store.record_run_started(sample_record)

        assert captured['data'] == {
            **sample_record.to_payload(),
            'record_level': 'run',
        }
        assert captured['options'] is None
        assert path.read_text(encoding='utf-8') == '{"serialized":true}\n'


class TestRunRecord:
    """Unit tests for `RunRecord` construction helpers."""

    def test_run_record_build_populates_runtime_metadata(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that :meth:`RunRecord.build` populates derived runtime metadata.
        """
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
        assert (
            record.config_sha256
            == hashlib.sha256(
                config_path.read_bytes(),
            ).hexdigest()
        )
        assert record.host is not None
        assert record.pid is not None


class TestSQLiteHistoryStore:
    """Unit tests for the SQLite persisted-history backend."""

    def test_sqlite_history_store_initializes_schema_and_meta(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that SQLite history stores create schema and schema-version
        metadata.
        """
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

    def test_sqlite_history_store_persists_job_run_records(
        self,
        tmp_path: Path,
        sample_job_run_record: mod.JobRunRecord,
    ) -> None:
        """
        Test that SQLite job-run persistence round-trips stored job rows.
        """
        store = mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')

        store.record_job_run(sample_job_run_record)

        assert list(store.iter_job_runs()) == [sample_job_run_record.to_payload()]

    def test_sqlite_history_store_round_trips_started_record(
        self,
        tmp_path: Path,
        sample_record: mod.RunRecord,
    ) -> None:
        """
        Test that SQLite run persistence round-trips started run rows.
        """
        store = mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')

        store.record_run_started(sample_record)

        assert list(store.iter_records()) == [_sqlite_run_row(sample_record)]

    def test_sqlite_history_store_updates_finished_record(
        self,
        tmp_path: Path,
        sample_record: mod.RunRecord,
    ) -> None:
        """
        Test that SQLite run persistence updates started rows with completion
        state.
        """
        store = mod.SQLiteHistoryStore(tmp_path / 'history.sqlite')
        completion = mod.RunCompletion(
            run_id=sample_record.run_id,
            state=mod.RunState(
                status='succeeded',
                finished_at='2026-03-23T00:00:05Z',
                duration_ms=5000,
                result_summary={'rows': 10},
            ),
        )

        store.record_run_started(sample_record)
        store.record_run_finished(completion)

        assert list(store.iter_records()) == [
            _sqlite_run_row(
                sample_record,
                status='succeeded',
                finished_at='2026-03-23T00:00:05Z',
                duration_ms=5000,
                result_summary={'rows': 10},
            ),
        ]
