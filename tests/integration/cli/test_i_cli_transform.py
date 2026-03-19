"""
:mod:`tests.integration.cli.test_i_cli_transform` module.

Integration-scope smoke tests for the ``etlplus transform`` CLI command.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING
from typing import Any

import pytest

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser
    from tests.integration.cli.conftest import RemoteStorageHarness

# SECTION: MARKS ============================================================ #


pytestmark = [pytest.mark.integration, pytest.mark.smoke]

# SECTION: TESTS ============================================================ #


class TestCliTransform:
    """Smoke tests for the ``etlplus transform`` CLI command."""

    def test_stdin_select(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        operations_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test transforming a STDIN payload and projecting selected fields."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(
            ('transform', '--operations', operations_json, '-', '-'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        expected = [{'id': rec['id']} for rec in sample_records]
        assert payload == expected

    def test_stdin_to_remote_file_target(
        self,
        cli_invoke: CliInvoke,
        operations_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        remote_storage_harness: RemoteStorageHarness,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test transforming STDIN data and writing the result to a remote URI."""
        target_uri = 's3://bucket/transform-output.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'transform',
                '--operations',
                operations_json,
                '-',
                target_uri,
            ),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'Data transformed and saved to {target_uri}'
        assert remote_storage_harness.read_json(target_uri) == [
            {'id': rec['id']} for rec in sample_records
        ]
