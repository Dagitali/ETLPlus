"""
:mod:`tests.smoke.test_s_cli` module.

Smoke test suite for core CLI flows.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke


# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.smoke


# SECTION: TESTS ============================================================ #


class TestCli:
    """Smoke tests for core CLI flows."""

    def test_extract_json_file(
        self,
        cli_invoke: CliInvoke,
        json_payload_file: Path,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test extracting JSON from a file and emit matching payload."""
        code, out, err = cli_invoke(('extract', str(json_payload_file)))
        assert code == 0
        assert err.strip() == ''
        payload = json.loads(out)
        assert payload == sample_records

    def test_load_stdin_to_file(
        self,
        cli_invoke: CliInvoke,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test loading stdin payload into a JSON file target."""
        out_path = tmp_path / 'out.json'
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(
            ('load', str(out_path), '--target-type', 'file'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = json.loads(out)
        assert payload.get('status') == 'success'
        assert out_path.exists()
        assert json.loads(out_path.read_text()) == sample_records

    def test_transform_stdin_select(
        self,
        cli_invoke: CliInvoke,
        operations_json: str,
        sample_records_json: str,
        sample_records: list[dict[str, Any]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test transforming stdin payload and project selected fields."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(
            ('transform', '--operations', operations_json, '-', '-'),
        )
        assert code == 0
        assert err.strip() == ''
        payload = json.loads(out)
        expected = [{'id': rec['id']} for rec in sample_records]
        assert payload == expected

    def test_validate_stdin_payload(
        self,
        cli_invoke: CliInvoke,
        rules_json: str,
        sample_records_json: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test validating stdin payload with basic rules."""
        monkeypatch.setattr(sys, 'stdin', io.StringIO(sample_records_json))
        code, out, err = cli_invoke(('validate', '--rules', rules_json, '-'))
        assert code == 0
        assert err.strip() == ''
        payload = json.loads(out)
        assert payload['valid'] is True
