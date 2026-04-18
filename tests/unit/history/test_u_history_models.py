"""
:mod:`tests.unit.history.test_u_history_models` module.

Unit tests for :mod:`etlplus.history._models`.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

import etlplus.history._store as store_mod

pytestmark = pytest.mark.unit


# SECTION: TESTS ============================================================ #


class TestJobRunRecord:
    """Unit tests for :class:`JobRunRecord`."""

    def test_to_payload_preserves_dataclass_fields(
        self,
        sample_job_run_record: store_mod.JobRunRecord,
    ) -> None:
        """
        Test that :meth:`to_payload` serializes the dataclass fields to the
        documented flat payload.
        """
        assert sample_job_run_record.to_payload() == {
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
        }


class TestRunCompletion:
    """Unit tests for :class:`RunCompletion`."""

    def test_to_payload_flattens_run_state(
        self,
        sample_completion: store_mod.RunCompletion,
    ) -> None:
        """
        Test that :meth:`to_payload` flattens the nested run state into one
        payload.
        """
        assert sample_completion.to_payload() == {
            'run_id': 'run-123',
            'status': 'succeeded',
            'finished_at': '2026-03-23T00:00:05Z',
            'duration_ms': 5000,
            'result_summary': {'rows': 10},
            'error_type': None,
            'error_message': None,
            'error_traceback': None,
        }


class TestRunRecord:
    """Unit tests for :class:`RunRecord`."""

    def test_build_populates_runtime_metadata(
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


class TestRunState:
    """Unit tests for :class:`RunState`."""

    def test_running_builds_default_in_flight_state(self) -> None:
        """
        Test that :meth:`running` defaults terminal metadata to missing values.
        """
        assert store_mod.RunState.running(status='queued') == store_mod.RunState(
            status='queued',
            finished_at=None,
            duration_ms=None,
            result_summary=None,
            error_type=None,
            error_message=None,
            error_traceback=None,
        )
