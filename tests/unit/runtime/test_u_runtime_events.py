"""
:mod:`tests.unit.runtime.test_u_runtime_events` module.

Unit tests for :mod:`etlplus.runtime._events`.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import pytest

import etlplus.runtime._events as events_mod

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: TESTS ============================================================ #


class TestRuntimeEvents:
    """Unit tests for structured runtime event helpers."""

    def test_build_uses_explicit_timestamp_and_extra_fields(self) -> None:
        """Test that build returns the stable envelope with extra fields merged."""
        event = events_mod.RuntimeEvents.build(
            command='run',
            lifecycle='started',
            run_id='run-123',
            timestamp='2025-01-01T00:00:00+00:00',
            job='daily',
        )

        assert event == {
            'command': 'run',
            'event': 'run.started',
            'job': 'daily',
            'lifecycle': 'started',
            'run_id': 'run-123',
            'schema': events_mod.EVENT_SCHEMA,
            'schema_version': events_mod.EVENT_SCHEMA_VERSION,
            'timestamp': '2025-01-01T00:00:00+00:00',
        }

    def test_build_uses_utc_timestamp_when_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that build falls back to the shared UTC timestamp helper."""
        monkeypatch.setattr(
            events_mod.RuntimeEvents,
            'utc_now_iso',
            staticmethod(lambda: '2025-01-01T00:00:00+00:00'),
        )

        event = events_mod.RuntimeEvents.build(
            command='run',
            lifecycle='completed',
            run_id='run-123',
        )

        assert event['timestamp'] == '2025-01-01T00:00:00+00:00'

    def test_create_run_id_returns_uuid_text(self) -> None:
        """Test that run identifiers are emitted as UUID text."""
        UUID(events_mod.RuntimeEvents.create_run_id())

    def test_emit_skips_when_jsonl_is_not_requested(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that event emission is disabled for non-jsonl formats."""
        events_mod.RuntimeEvents.emit({'event': 'run.started'}, event_format=None)
        events_mod.RuntimeEvents.emit({'event': 'run.started'}, event_format='text')

        captured = capsys.readouterr()
        assert captured.err == ''

    def test_emit_writes_serialized_jsonl_to_stderr(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Test that jsonl emission writes the serialized payload to STDERR."""
        monkeypatch.setattr(events_mod, 'serialize_json', lambda event: 'SERIALIZED')

        events_mod.RuntimeEvents.emit({'event': 'run.started'}, event_format='jsonl')

        captured = capsys.readouterr()
        assert captured.err == 'SERIALIZED\n'

    def test_utc_now_iso_returns_iso8601_text(self) -> None:
        """Test that the timestamp helper returns parseable ISO-8601 text."""
        assert datetime.fromisoformat(events_mod.RuntimeEvents.utc_now_iso())
