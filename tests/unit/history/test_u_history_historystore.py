"""
:mod:`tests.unit.history.test_u_history_historystore` module.

Unit tests for :class:`HistoryStore`.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

import etlplus.history._store as store_mod

from .pytest_history_store_support import MemoryHistoryStore
from .pytest_history_store_support import normalized_job_run_payload
from .pytest_history_store_support import normalized_run_payload

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument


pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestHistoryStore:
    """Unit tests for :class:`HistoryStore`."""

    def test_abstract_methods_raise_not_implemented(
        self,
        sample_job_run_record: store_mod.JobRunRecord,
        sample_record: store_mod.RunRecord,
        sample_completion: store_mod.RunCompletion,
    ) -> None:
        """Test that abstract base-method bodies raise `NotImplementedError`."""
        history_store = cast(store_mod.HistoryStore, object())

        with pytest.raises(NotImplementedError):
            store_mod.HistoryStore.iter_records(history_store)
        with pytest.raises(NotImplementedError):
            store_mod.HistoryStore.record_run_started(history_store, sample_record)
        with pytest.raises(NotImplementedError):
            store_mod.HistoryStore.record_run_finished(
                history_store,
                sample_completion,
            )
        with pytest.raises(NotImplementedError):
            store_mod.HistoryStore.record_job_run(history_store, sample_job_run_record)

    @pytest.mark.parametrize(
        ('backend', 'path_name'),
        [
            pytest.param('jsonl', 'history.jsonl', id='jsonl'),
            pytest.param('sqlite', 'history.sqlite', id='sqlite'),
        ],
    )
    def test_backends_normalize_to_same_documented_run_and_job_shapes(
        self,
        tmp_path: Path,
        sample_job_run_record: store_mod.JobRunRecord,
        sample_completion: store_mod.RunCompletion,
        sample_record: store_mod.RunRecord,
        backend: str,
        path_name: str,
    ) -> None:
        """
        Both backends should normalize persisted run and job data to the same
        documented shapes.
        """
        path = tmp_path / path_name
        store: store_mod.HistoryStore = (
            store_mod.JsonlHistoryStore(path)
            if backend == 'jsonl'
            else store_mod.SQLiteHistoryStore(path)
        )
        expected_run = sample_record.to_payload() | {
            'duration_ms': 5000,
            'finished_at': '2026-03-23T00:00:05Z',
            'result_summary': {'rows': 10},
            'status': 'succeeded',
        }
        expected_job_run = sample_job_run_record.to_payload()

        store.record_run_started(sample_record)
        store.record_run_finished(sample_completion)
        store.record_job_run(sample_job_run_record)

        assert list(store.iter_runs()) == [expected_run]
        assert list(store.iter_job_runs()) == [expected_job_run]

    def test_coerce_state_dir_defaults_without_environment(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test that state directory coercion falls back to the package default.
        """
        monkeypatch.delenv('ETLPLUS_STATE_DIR', raising=False)

        assert (
            store_mod.HistoryStore._coerce_state_dir()
            == Path('~/.etlplus').expanduser()
        )

    def test_from_environment_rejects_invalid_backend(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that unknown persisted-history backends fail fast."""
        monkeypatch.setenv('ETLPLUS_HISTORY_BACKEND', 'csv')
        monkeypatch.setenv('ETLPLUS_STATE_DIR', str(tmp_path))

        with pytest.raises(ValueError, match='sqlite, jsonl'):
            store_mod.HistoryStore.from_environment()

    def test_from_settings_selects_supported_backend(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that explicit store settings resolve supported backends."""
        sqlite_store = store_mod.HistoryStore.from_settings(
            backend='sqlite',
            state_dir=tmp_path / 'sqlite-state',
        )
        jsonl_store = store_mod.HistoryStore.from_settings(
            backend='jsonl',
            state_dir=tmp_path / 'jsonl-state',
        )

        assert isinstance(sqlite_store, store_mod.SQLiteHistoryStore)
        assert sqlite_store.db_path == tmp_path / 'sqlite-state' / 'history.sqlite'
        assert isinstance(jsonl_store, store_mod.JsonlHistoryStore)
        assert jsonl_store.log_path == tmp_path / 'jsonl-state' / 'history.jsonl'

    @pytest.mark.parametrize(
        ('backend', 'expected_type', 'path_attr', 'path_name'),
        [
            pytest.param(
                'jsonl',
                store_mod.JsonlHistoryStore,
                'log_path',
                'history.jsonl',
                id='jsonl-backend',
            ),
            pytest.param(
                None,
                store_mod.SQLiteHistoryStore,
                'db_path',
                'history.sqlite',
                id='default-sqlite-backend',
            ),
        ],
    )
    def test_from_environment_selects_supported_backend(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        backend: str | None,
        expected_type: type[store_mod.HistoryStore],
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

        store = store_mod.HistoryStore.from_environment()

        assert isinstance(store, expected_type)
        assert getattr(store, path_attr) == tmp_path / path_name

    def test_iter_job_runs_skips_invalid_or_incomplete_keys(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], MemoryHistoryStore],
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
            normalized_job_run_payload(
                run_id='run-parent',
                job_name='seed',
                sequence_index=0,
                status='succeeded',
                result_status='ok',
            ),
        ]

    def test_iter_runs_skips_missing_run_ids(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], MemoryHistoryStore],
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
            normalized_run_payload(
                run_id='run-123',
                status='running',
            ),
        ]

    def test_iter_runs_skips_non_run_records(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], MemoryHistoryStore],
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
            normalized_run_payload(
                run_id='run-123',
                status='running',
            ),
        ]
