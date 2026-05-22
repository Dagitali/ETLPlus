"""
:mod:`tests.meta.test_m_contract_observability` module.

Contract tests for the stable observability surface.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

import etlplus.runtime as runtime_pkg
from etlplus.history import HistoryStore
from etlplus.history import RunCompletion
from etlplus.history import RunRecord
from etlplus.history._store import JobRunRecord
from tests.meta.pytest_meta_support import REPO_ROOT

# SECTION: MARKERS ========================================================== #


pytestmark = [pytest.mark.meta, pytest.mark.contract]


# SECTION: TYPE ALIASES ===================================================== #


type HistoryShapeCase = tuple[dict[str, object], str, set[str]]


# SECTION: CONSTANTS ======================================================== #


EXPECTED_EVENT_SCHEMA = 'etlplus.event.v1'
EXPECTED_EVENT_SCHEMA_VERSION = 1
EXPECTED_EVENT_LIFECYCLES = {
    'started',
    'completed',
    'failed',
}
EXPECTED_BASE_EVENT_FIELDS = {
    'command',
    'event',
    'lifecycle',
    'run_id',
    'schema',
    'schema_version',
    'timestamp',
}
EXPECTED_NORMALIZED_RUN_FIELDS = {
    'config_path',
    'config_sha256',
    'duration_ms',
    'error_message',
    'error_traceback',
    'error_type',
    'etlplus_version',
    'finished_at',
    'host',
    'job_name',
    'pid',
    'pipeline_name',
    'records_in',
    'records_out',
    'result_summary',
    'run_id',
    'started_at',
    'status',
}
EXPECTED_NORMALIZED_JOB_FIELDS = {
    'duration_ms',
    'error_message',
    'error_type',
    'finished_at',
    'job_name',
    'pipeline_name',
    'records_in',
    'records_out',
    'result_status',
    'result_summary',
    'run_id',
    'sequence_index',
    'skipped_due_to',
    'started_at',
    'status',
}


NORMALIZED_HISTORY_SHAPE_CASES: tuple[HistoryShapeCase, ...] = (
    (
        {
            'record_level': 'job',
            'run_id': 'run-123',
            'job_name': 'seed',
            'sequence_index': 0,
            'status': 'succeeded',
        },
        'iter_job_runs',
        EXPECTED_NORMALIZED_JOB_FIELDS,
    ),
    (
        {
            'record_level': 'run',
            'run_id': 'run-123',
            'status': 'running',
        },
        'iter_runs',
        EXPECTED_NORMALIZED_RUN_FIELDS,
    ),
)


# SECTION: HELPERS ========================================================== #


class _ContractHistoryStore(HistoryStore):
    """Small in-memory store used to assert normalized history shapes."""

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    def iter_records(self) -> Iterator[dict[str, Any]]:
        """Yield the injected record list verbatim."""
        yield from self._records

    def record_run_started(self, record: RunRecord) -> None:
        _ = record

    def record_run_finished(self, completion: RunCompletion) -> None:
        _ = completion

    def record_job_run(self, record: JobRunRecord) -> None:
        _ = record


# SECTION: TESTS ============================================================ #


class TestStructuredEventContract:
    """Meta-level contract tests for the stable event envelope."""

    @pytest.mark.parametrize('lifecycle', sorted(EXPECTED_EVENT_LIFECYCLES))
    def test_build_keeps_required_base_fields_for_documented_lifecycles(
        self,
        lifecycle: str,
    ) -> None:
        """
        Test that documented lifecycle values always build the stable base
        envelope.
        """
        event = runtime_pkg.RuntimeEvents.build(
            command='run',
            lifecycle=lifecycle,
            run_id='run-123',
            timestamp='2026-04-06T00:00:00+00:00',
        )

        assert EXPECTED_BASE_EVENT_FIELDS == set(event)
        assert event['schema'] == EXPECTED_EVENT_SCHEMA
        assert event['schema_version'] == EXPECTED_EVENT_SCHEMA_VERSION
        assert event['event'] == f'run.{lifecycle}'

    def test_published_guides_keep_structured_event_contract_entrypoints(self) -> None:
        """
        Test that the published docs keep the dedicated structured-events guide
        wired in.
        """
        guide_path = REPO_ROOT / 'docs/source/guides/structured-events.md'
        guides_index_path = REPO_ROOT / 'docs/source/guides/index.md'

        assert guide_path.exists()
        assert 'etlplus.event.v1' in guide_path.read_text(encoding='utf-8')
        assert 'structured-events' in guides_index_path.read_text(encoding='utf-8')

    def test_runtime_package_keeps_stable_event_schema_identifiers(self) -> None:
        """
        Test that the runtime package keeps the documented event identifiers.
        """
        assert runtime_pkg.EVENT_SCHEMA == EXPECTED_EVENT_SCHEMA
        assert runtime_pkg.EVENT_SCHEMA_VERSION == EXPECTED_EVENT_SCHEMA_VERSION


class TestNormalizedHistoryContract:
    """Meta-level contract tests for normalized persisted history shapes."""

    @pytest.mark.parametrize(
        ('source_record', 'iterator_name', 'expected_fields'),
        NORMALIZED_HISTORY_SHAPE_CASES,
        ids=lambda value: value if isinstance(value, str) else None,
    )
    def test_iter_records_keep_stable_normalized_field_sets(
        self,
        source_record: dict[str, object],
        iterator_name: str,
        expected_fields: set[str],
    ) -> None:
        """
        Test that normalized history records keep documented stable field sets.
        """
        iterator = getattr(_ContractHistoryStore([source_record]), iterator_name)
        record = next(iterator())

        assert expected_fields == set(record)
