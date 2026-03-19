"""
:mod:`tests.integration.cli.test_i_cli_validate` module.

Integration-scope smoke tests for the ``etlplus validate`` CLI command.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING

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


class TestCliValidate:
    """Smoke tests for the ``etlplus validate`` CLI command."""

    def test_stdin_payload(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        rules_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test validating a STDIN payload with basic rules."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(('validate', '--rules', rules_json, '-'))
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload['valid'] is True

    def test_stdin_payload_to_remote_output(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, object]],
        remote_storage_harness: RemoteStorageHarness,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test validating STDIN data and writing validated output to a remote URI."""
        target_uri = 's3://bucket/validate-output.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))

        code, out, err = cli_invoke(
            (
                'validate',
                '--rules',
                rules_json,
                '--output',
                target_uri,
                '-',
            ),
        )

        assert code == 0
        assert err.strip() == ''
        assert out.strip() == f'ValidationDict result saved to {target_uri}'
        assert remote_storage_harness.read_json(target_uri) == sample_records
