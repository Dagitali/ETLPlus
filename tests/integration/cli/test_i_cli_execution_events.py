"""
:mod:`tests.integration.cli.test_i_cli_execution_events` module.

Integration coverage for structured execution events across CLI commands.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

from etlplus.runtime import EVENT_SCHEMA
from etlplus.runtime import EVENT_SCHEMA_VERSION

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]


# SECTION: HELPERS ========================================================== #


def _parse_event_lines(stderr: str) -> list[dict[str, Any]]:
    """Parse JSONL event output from STDERR."""
    return [json.loads(line) for line in stderr.splitlines() if line.strip()]


def _assert_lifecycle(
    lines: list[dict[str, Any]],
    *,
    command: str,
) -> None:
    """Assert the stable event envelope for one command invocation."""
    assert [line['event'] for line in lines] == [
        f'{command}.started',
        f'{command}.completed',
    ]
    assert all(line['command'] == command for line in lines)
    assert all(line['schema'] == EVENT_SCHEMA for line in lines)
    assert all(line['schema_version'] == EVENT_SCHEMA_VERSION for line in lines)
    assert all(isinstance(line['run_id'], str) for line in lines)
    assert lines[0]['run_id'] == lines[1]['run_id']


# SECTION: TESTS ============================================================ #


class TestCliExecutionEvents:
    """Structured event coverage for execution-oriented commands."""

    def test_extract_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        json_payload_file: Path,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test that ``extract --event-format jsonl`` emits stable events."""
        code, out, err = cli_invoke(
            ('extract', '--event-format', 'jsonl', str(json_payload_file)),
        )

        assert code == 0
        assert parse_json_output(out) == sample_records
        _assert_lifecycle(_parse_event_lines(err), command='extract')

    def test_load_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Test that ``load --event-format jsonl`` emits stable events."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        out_path = tmp_path / 'load-events.json'

        code, out, err = cli_invoke(
            (
                'load',
                '--event-format',
                'jsonl',
                '--target-type',
                'file',
                str(out_path),
            ),
        )

        assert code == 0
        assert parse_json_output(out)['status'] == 'success'
        _assert_lifecycle(_parse_event_lines(err), command='load')

    def test_transform_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        operations_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``transform --event-format jsonl`` emits stable events."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'transform',
                '--event-format',
                'jsonl',
                '--operations',
                operations_json,
                '-',
                '-',
            ),
        )

        assert code == 0
        assert isinstance(parse_json_output(out), list)
        _assert_lifecycle(_parse_event_lines(err), command='transform')

    def test_validate_emits_jsonl_events(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        rules_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that ``validate --event-format jsonl`` emits stable events."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'validate',
                '--event-format',
                'jsonl',
                '--rules',
                rules_json,
                '-',
            ),
        )

        assert code == 0
        assert parse_json_output(out)['valid'] is True
        _assert_lifecycle(_parse_event_lines(err), command='validate')
