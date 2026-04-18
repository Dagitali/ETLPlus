"""
:mod:`tests.unit.history.test_u_history_store_helpers` module.

Unit tests for :mod:`etlplus.history._store`.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast

import pytest

import etlplus.history._config as history_config_mod
import etlplus.history._store as store_mod

from .pytest_history_store_support import MemoryHistoryStore
from .pytest_history_store_support import normalized_job_run_payload

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestHistoryStoreMergeHelpers:
    """Unit tests for internal record-merge behavior."""

    def test_iter_job_runs_skips_records_without_merge_keys(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], MemoryHistoryStore],
    ) -> None:
        """Job-run merging should ignore rows missing ``run_id`` or ``job_name``."""
        store = history_store_factory(
            [
                normalized_job_run_payload(
                    record_level='job',
                    run_id='run-123',
                    job_name='job-a',
                    status='succeeded',
                ),
                normalized_job_run_payload(
                    record_level='job',
                    run_id=None,
                    job_name='job-b',
                    status='failed',
                ),
                normalized_job_run_payload(
                    record_level='job',
                    run_id='run-999',
                    job_name=None,
                    status='failed',
                ),
            ],
        )

        assert list(store.iter_job_runs()) == [
            normalized_job_run_payload(
                run_id='run-123',
                job_name='job-a',
                status='succeeded',
            ),
        ]


class TestHistoryStoreModuleHelpers:
    """Unit tests for :mod:`etlplus.history._store` helper functions."""

    def test_run_record_build_delegates_to_classmethod_implementation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that the preferred :meth:`RunRecord.build` entry point receives
        the supplied constructor inputs unchanged.
        """
        captured: dict[str, Any] = {}
        sentinel = object()

        def fake_build(**kwargs: Any) -> object:
            captured.update(kwargs)
            return sentinel

        monkeypatch.setattr(store_mod.RunRecord, 'build', staticmethod(fake_build))

        result = store_mod.RunRecord.build(
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
        assert store_mod._deserialize_result_summary(payload) == expected

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
        assert store_mod._deserialize_string_list(payload) == expected

    def test_serialize_string_list_returns_json_when_values_present(self) -> None:
        """Test that string-list serialization emits stable persisted JSON."""
        assert (
            store_mod._serialize_string_list(['seed', 'publish'])
            == '["seed","publish"]'
        )

    def test_file_sha256_returns_none_for_missing_path(
        self,
        tmp_path: Path,
    ) -> None:
        """
        Test that SHA-256 lookup returns `None` when the file does not exist.
        """
        assert store_mod._file_sha256(str(tmp_path / 'missing.yml')) is None

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
            store_mod._file_sha256(str(config_path))
            == hashlib.sha256(
                config_path.read_bytes(),
            ).hexdigest()
        )

    def test_sqlite_record_payload_serializes_missing_result_summary(
        self,
        sample_record: store_mod.RunRecord,
    ) -> None:
        """
        Test that SQLite run payloads preserve missing result summaries.
        """
        assert store_mod._sqlite_record_payload(sample_record)['result_summary'] is None

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
            assert store_mod._sqlite_row_payload(row) == expected

    def test_sqlite_job_run_payload_serializes_nested_optional_fields(self) -> None:
        """Test that SQLite job payloads JSON-encode nested summary and skip fields."""
        payload = store_mod._sqlite_job_run_payload(
            store_mod.JobRunRecord(
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


class TestResolvedHistoryStore:
    """Unit tests for resolved history configuration helpers."""

    def test_from_settings_rejects_invalid_backend(self) -> None:
        """Explicit store settings should fail fast for unsupported backends."""
        with pytest.raises(ValueError, match='sqlite, jsonl'):
            store_mod.HistoryStore.from_settings(backend='csv')

    def test_from_resolved_settings_rejects_unknown_backend(self) -> None:
        """Resolved settings should still fail fast for unsupported backends."""
        settings = cast(
            history_config_mod.ResolvedHistoryConfig,
            SimpleNamespace(
                enabled=True,
                backend='csv',
                state_dir=Path('/tmp/etlplus-state'),
                capture_tracebacks=False,
            ),
        )

        with pytest.raises(ValueError, match='sqlite, jsonl'):
            store_mod.HistoryStore._from_resolved_settings(settings)

    def test_resolve_applies_precedence(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that CLI overrides win, then env, then pipeline config."""
        resolved = history_config_mod.ResolvedHistoryConfig.resolve(
            history_config_mod.HistoryConfig(
                enabled=True,
                backend='sqlite',
                state_dir='./from-config',
                capture_tracebacks=False,
            ),
            env={
                'ETLPLUS_HISTORY_BACKEND': 'jsonl',
                'ETLPLUS_STATE_DIR': str(tmp_path / 'from-env'),
            },
            enabled=False,
            backend='sqlite',
            state_dir=tmp_path / 'from-cli',
            capture_tracebacks=True,
        )

        assert resolved.enabled is False
        assert resolved.backend == 'sqlite'
        assert resolved.state_dir == tmp_path / 'from-cli'
        assert resolved.capture_tracebacks is True


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

        record = store_mod.RunRecord.build(
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
