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

    @pytest.mark.parametrize(
        ('method_name', 'arg_names'),
        [
            pytest.param('iter_records', (), id='iter-records'),
            pytest.param('record_run_started', ('sample_record',), id='run-started'),
            pytest.param(
                'record_run_finished',
                ('sample_completion',),
                id='run-finished',
            ),
            pytest.param(
                'record_job_run',
                ('sample_job_run_record',),
                id='job-run',
            ),
        ],
    )
    def test_abstract_methods_raise_not_implemented(
        self,
        sample_job_run_record: store_mod.JobRunRecord,
        sample_record: store_mod.RunRecord,
        sample_completion: store_mod.RunCompletion,
        method_name: str,
        arg_names: tuple[str, ...],
    ) -> None:
        """Test that abstract base-method bodies raise `NotImplementedError`."""
        history_store = cast(store_mod.HistoryStore, object())
        fixtures = {
            'sample_completion': sample_completion,
            'sample_job_run_record': sample_job_run_record,
            'sample_record': sample_record,
        }
        args = tuple(fixtures[name] for name in arg_names)

        with pytest.raises(NotImplementedError):
            getattr(store_mod.HistoryStore, method_name)(history_store, *args)

    @pytest.mark.parametrize(
        ('backend', 'path_name'),
        [
            ('jsonl', 'history.jsonl'),
            ('sqlite', 'history.sqlite'),
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

    @pytest.mark.parametrize(
        ('backend', 'expected_type', 'path_attr', 'path_name'),
        [
            ('jsonl', store_mod.JsonlHistoryStore, 'log_path', 'history.jsonl'),
            ('sqlite', store_mod.SQLiteHistoryStore, 'db_path', 'history.sqlite'),
        ],
    )
    def test_from_settings_selects_supported_backend(
        self,
        tmp_path: Path,
        backend: str,
        expected_type: type[store_mod.HistoryStore],
        path_attr: str,
        path_name: str,
    ) -> None:
        """Test that explicit store settings resolve supported backends."""
        state_dir = tmp_path / f'{backend}-state'

        store = store_mod.HistoryStore.from_settings(
            backend=backend,
            state_dir=state_dir,
        )

        assert isinstance(store, expected_type)
        assert getattr(store, path_attr) == state_dir / path_name

    @pytest.mark.parametrize(
        ('backend', 'expected_type', 'path_attr', 'path_name'),
        [
            ('jsonl', store_mod.JsonlHistoryStore, 'log_path', 'history.jsonl'),
            (None, store_mod.SQLiteHistoryStore, 'db_path', 'history.sqlite'),
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
        (job_run,) = history_store_factory(
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
        ).iter_job_runs()

        assert job_run == normalized_job_run_payload(
            run_id='run-parent',
            job_name='seed',
            sequence_index=0,
            status='succeeded',
            result_status='ok',
        )

    def test_iter_runs_skips_missing_run_ids(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], MemoryHistoryStore],
    ) -> None:
        """Test that run iteration ignores records without valid run identifiers."""
        (run,) = history_store_factory(
            [
                {'run_id': None, 'status': 'ignored'},
                {'run_id': '', 'status': 'ignored'},
                {'run_id': 'run-123', 'status': 'running'},
                {'run_id': 'run-123', 'finished_at': None},
            ],
        ).iter_runs()

        assert run == normalized_run_payload(run_id='run-123', status='running')

    def test_iter_runs_skips_non_run_records(
        self,
        history_store_factory: Callable[[list[dict[str, object]]], MemoryHistoryStore],
    ) -> None:
        """
        Test that run iteration ignores persisted job-level records entirely.
        """
        (run,) = history_store_factory(
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
        ).iter_runs()

        assert run == normalized_run_payload(run_id='run-123', status='running')
