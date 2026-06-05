"""
:mod:`tests.unit.runtime.test_u_runtime_events` module.

Unit tests for :mod:`etlplus.runtime._events`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from typing import cast
from uuid import UUID

import pytest

import etlplus.runtime._events as events_mod
import etlplus.telemetry as telemetry_mod
from tests.pytest_shared_support import STRUCTURED_EVENT_BASE_FIELDS

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TYPE ALIASES ===================================================== #


type EventBuildArgs = dict[str, object]
type EventExtraFields = dict[str, object]


# SECTION: CONSTANTS ======================================================== #


RUN_ID = 'run-123'
FROZEN_TIMESTAMP = '2025-01-01T00:00:00+00:00'


# SECTION: HELPERS ========================================================== #


def frozen_timestamp() -> str:
    """Return a stable timestamp for runtime event tests."""
    return FROZEN_TIMESTAMP


def serialized_event(_codec: object, _event: object) -> str:
    """Return stable serialized event text for stderr emission tests."""
    return 'SERIALIZED'


# SECTION: TESTS ============================================================ #


class TestRuntimeEvents:
    """Unit tests for structured runtime event helpers."""

    def test_build_preserves_additive_command_specific_fields(self) -> None:
        """
        Test that stable base fields coexist with additive command-specific
        context.
        """
        event = events_mod.RuntimeEvents.build(
            command='run',
            lifecycle='completed',
            run_id=RUN_ID,
            config_path='pipeline.yml',
            continue_on_fail=False,
            pipeline_name='customer-sync',
            result_status='success',
            run_all=True,
        )

        assert STRUCTURED_EVENT_BASE_FIELDS.issubset(event)
        assert event['config_path'] == 'pipeline.yml'
        assert event['continue_on_fail'] is False
        assert event['pipeline_name'] == 'customer-sync'
        assert event['result_status'] == 'success'
        assert event['run_all'] is True

    @pytest.mark.parametrize(
        ('kwargs', 'expected_extra'),
        [
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'started',
                    'run_id': RUN_ID,
                    'timestamp': FROZEN_TIMESTAMP,
                    'job': 'daily',
                },
                {'job': 'daily'},
                id='explicit-timestamp',
            ),
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'completed',
                    'run_id': RUN_ID,
                },
                {},
                id='implicit-utc-timestamp',
            ),
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'failed',
                    'run_id': RUN_ID,
                    'error_type': 'RuntimeError',
                    'error_message': 'boom',
                    'status': 'error',
                },
                {
                    'error_type': 'RuntimeError',
                    'error_message': 'boom',
                    'status': 'error',
                },
                id='failed-with-shared-fields',
            ),
        ],
    )
    def test_build_returns_expected_event_envelope(
        self,
        monkeypatch: pytest.MonkeyPatch,
        kwargs: EventBuildArgs,
        expected_extra: EventExtraFields,
    ) -> None:
        """
        Test that build emits the stable event envelope and resolves
        timestamps.
        """
        monkeypatch.setattr(
            events_mod.RuntimeEvents,
            'utc_now_iso',
            staticmethod(frozen_timestamp),
        )
        event = events_mod.RuntimeEvents.build(**cast(Any, kwargs))

        assert STRUCTURED_EVENT_BASE_FIELDS.issubset(event)
        assert event == {
            'command': 'run',
            'event': f'run.{kwargs["lifecycle"]}',
            'lifecycle': kwargs['lifecycle'],
            'run_id': RUN_ID,
            'schema': events_mod.EVENT_SCHEMA,
            'schema_version': events_mod.EVENT_SCHEMA_VERSION,
            'timestamp': kwargs.get('timestamp', FROZEN_TIMESTAMP),
            **expected_extra,
        }

    def test_create_run_id_returns_uuid_text(self) -> None:
        """Test that run identifiers are emitted as UUID text."""
        UUID(events_mod.RuntimeEvents.create_run_id())

    def test_emit_always_forwards_events_to_runtime_telemetry(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Runtime event emission should also feed the telemetry bridge."""
        captured: list[dict[str, object]] = []

        monkeypatch.setattr(
            telemetry_mod.RuntimeTelemetry,
            'emit_event',
            classmethod(
                lambda _cls, event: captured.append(dict(event)),
            ),
        )

        events_mod.RuntimeEvents.emit(
            {'event': 'run.started', 'run_id': RUN_ID},
            event_format=None,
        )

        assert captured == [{'event': 'run.started', 'run_id': RUN_ID}]

    @pytest.mark.parametrize(
        ('event_format', 'expected_err'),
        [
            pytest.param(None, '', id='disabled-without-format'),
            pytest.param('text', '', id='disabled-for-text'),
            pytest.param('jsonl', 'SERIALIZED\n', id='jsonl-to-stderr'),
        ],
    )
    def test_emit_writes_only_for_jsonl(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
        event_format: str | None,
        expected_err: str,
    ) -> None:
        """
        Test that runtime events only emit serialized output for ``jsonl``.
        """
        monkeypatch.setattr(
            events_mod.JsonCodec,
            'serialize',
            serialized_event,
        )

        events_mod.RuntimeEvents.emit(
            {'event': 'run.started'},
            event_format=event_format,
        )

        captured = capsys.readouterr()
        assert captured.out == ''
        assert captured.err == expected_err

    def test_utc_now_iso_returns_iso8601_text(self) -> None:
        """Test that the timestamp helper returns parseable ISO-8601 text."""
        assert datetime.fromisoformat(events_mod.RuntimeEvents.utc_now_iso())
