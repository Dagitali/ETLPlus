"""
:mod:`tests.unit.cli._handlers.pytest_cli_history_support` module.

Shared helper seams for CLI history handler unit tests.
"""

from __future__ import annotations

from typing import Any

import pytest

from etlplus.cli._handlers import _history_view as history_view_mod

from ..conftest import CaptureIo

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: FUNCTIONS ======================================================== #


def assert_emit_markdown_table(
    calls: CaptureIo,
    rows: list[dict[str, object]],
    *,
    columns: tuple[str, ...],
) -> None:
    """Assert that :func:`emit_markdown_table` was called once as expected."""
    assert calls['emit_json'] == []
    assert calls['emit_markdown_table'] == [((rows,), {'columns': columns})]


def normalized_run(**overrides: object) -> dict[str, object]:
    """Build one normalized run payload with stable default ``None`` fields."""
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


def normalized_job(**overrides: object) -> dict[str, object]:
    """Build one normalized job payload with stable default ``None`` fields."""
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


def patch_history_store_records(
    monkeypatch: pytest.MonkeyPatch,
    records: list[dict[str, object]],
) -> None:
    """Patch :class:`HistoryStore` to yield one fixed record sequence."""

    class _FakeHistoryStore(history_view_mod.HistoryStore):
        def iter_records(self) -> Any:
            return iter(records)

        def record_run_started(self, record: object) -> None:
            _ = record

        def record_run_finished(self, completion: object) -> None:
            _ = completion

        def record_job_run(self, record: object) -> None:
            _ = record

    monkeypatch.setattr(
        history_view_mod.HistoryStore,
        'from_environment',
        _FakeHistoryStore,
    )


def report_row(**overrides: object) -> dict[str, object]:
    """Build one aggregated history report row with zero/``None`` defaults."""
    return {
        'avg_duration_ms': None,
        'failed': 0,
        'group': None,
        'last_started_at': None,
        'max_duration_ms': None,
        'min_duration_ms': None,
        'other': 0,
        'running': 0,
        'runs': 0,
        'success_rate_pct': None,
        'succeeded': 0,
    } | overrides


def report_summary(**overrides: object) -> dict[str, object]:
    """Build one aggregated history report summary payload."""
    return {
        key: value
        for key, value in report_row(**overrides).items()
        if key not in {'group', 'last_started_at'}
    }
