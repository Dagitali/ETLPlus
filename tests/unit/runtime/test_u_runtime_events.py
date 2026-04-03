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

# SECTION: HELPERS ========================================================== #


@pytest.fixture
def frozen_runtime_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Freeze runtime-event timestamps at one stable ISO-8601 value."""
    timestamp = '2025-01-01T00:00:00+00:00'
    monkeypatch.setattr(
        events_mod.RuntimeEvents,
        'utc_now_iso',
        staticmethod(lambda: timestamp),
    )
    return timestamp


# SECTION: TESTS ============================================================ #


class TestRuntimeEvents:
    """Unit tests for structured runtime event helpers."""

    @pytest.mark.parametrize(
        ('kwargs', 'expected_timestamp'),
        [
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'started',
                    'run_id': 'run-123',
                    'timestamp': '2025-01-01T00:00:00+00:00',
                    'job': 'daily',
                },
                '2025-01-01T00:00:00+00:00',
                id='explicit-timestamp',
            ),
            pytest.param(
                {
                    'command': 'run',
                    'lifecycle': 'completed',
                    'run_id': 'run-123',
                },
                '2025-01-01T00:00:00+00:00',
                id='implicit-utc-timestamp',
            ),
        ],
    )
    def test_build_returns_expected_event_envelope(
        self,
        kwargs: dict[str, str],
        frozen_runtime_timestamp: str,
        expected_timestamp: str,
    ) -> None:
        """Build should emit the stable event envelope and resolve timestamps."""
        event = events_mod.RuntimeEvents.build(**kwargs)

        assert event == {
            'command': 'run',
            'event': f'run.{kwargs["lifecycle"]}',
            'lifecycle': kwargs['lifecycle'],
            'run_id': 'run-123',
            'schema': events_mod.EVENT_SCHEMA,
            'schema_version': events_mod.EVENT_SCHEMA_VERSION,
            'timestamp': (
                frozen_runtime_timestamp
                if 'timestamp' not in kwargs
                else expected_timestamp
            ),
            **({'job': 'daily'} if 'job' in kwargs else {}),
        }

    def test_create_run_id_returns_uuid_text(self) -> None:
        """Test that run identifiers are emitted as UUID text."""
        UUID(events_mod.RuntimeEvents.create_run_id())

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
        """Runtime events should only emit serialized output for ``jsonl``."""
        monkeypatch.setattr(events_mod, 'serialize_json', lambda event: 'SERIALIZED')

        events_mod.RuntimeEvents.emit(
            {'event': 'run.started'},
            event_format=event_format,
        )

        captured = capsys.readouterr()
        assert captured.err == expected_err

    def test_utc_now_iso_returns_iso8601_text(self) -> None:
        """Test that the timestamp helper returns parseable ISO-8601 text."""
        assert datetime.fromisoformat(events_mod.RuntimeEvents.utc_now_iso())
