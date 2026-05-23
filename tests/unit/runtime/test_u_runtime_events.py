"""
:mod:`tests.unit.runtime.test_u_runtime_events` module.

Unit tests for :mod:`etlplus.runtime._events`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import pytest

import etlplus.runtime._events as events_mod
import etlplus.telemetry as telemetry_mod
from tests.pytest_shared_support import STRUCTURED_EVENT_BASE_FIELDS

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

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
            run_id='run-123',
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
        'kwargs',
        [
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'started',
                    'run_id': 'run-123',
                    'timestamp': '2025-01-01T00:00:00+00:00',
                    'job': 'daily',
                },
                id='explicit-timestamp',
            ),
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'completed',
                    'run_id': 'run-123',
                },
                id='implicit-utc-timestamp',
            ),
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'failed',
                    'run_id': 'run-123',
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
        kwargs: dict[str, Any],
    ) -> None:
        """
        Test that build emits the stable event envelope and resolves
        timestamps.
        """
        frozen_timestamp = '2025-01-01T00:00:00+00:00'
        monkeypatch.setattr(
            events_mod.RuntimeEvents,
            'utc_now_iso',
            staticmethod(lambda: frozen_timestamp),
        )
        event = events_mod.RuntimeEvents.build(**kwargs)

        assert STRUCTURED_EVENT_BASE_FIELDS.issubset(event)
        assert event == {
            'command': 'run',
            'event': f'run.{kwargs["lifecycle"]}',
            'lifecycle': kwargs['lifecycle'],
            'run_id': 'run-123',
            'schema': events_mod.EVENT_SCHEMA,
            'schema_version': events_mod.EVENT_SCHEMA_VERSION,
            'timestamp': kwargs.get('timestamp', frozen_timestamp),
            **({'job': 'daily'} if 'job' in kwargs else {}),
            **(
                {
                    'error_type': 'RuntimeError',
                    'error_message': 'boom',
                    'status': 'error',
                }
                if kwargs['lifecycle'] == 'failed'
                else {}
            ),
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
            {'event': 'run.started', 'run_id': 'run-123'},
            event_format=None,
        )

        assert captured == [{'event': 'run.started', 'run_id': 'run-123'}]

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
            lambda _codec, event: 'SERIALIZED',
        )

        events_mod.RuntimeEvents.emit(
            {'event': 'run.started'},
            event_format=event_format,
        )

        captured = capsys.readouterr()
        assert captured.err == expected_err

    def test_utc_now_iso_returns_iso8601_text(self) -> None:
        """Test that the timestamp helper returns parseable ISO-8601 text."""
        assert datetime.fromisoformat(events_mod.RuntimeEvents.utc_now_iso())
