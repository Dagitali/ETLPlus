"""
:mod:`tests.smoke.test_s_cli_load` module.

Smoke test suite for the ``etlplus load`` CLI command.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonFileParser
    from tests.conftest import JsonOutputParser

# SECTION: TESTS ============================================================ #


class TestCliLoad:
    """Smoke test suite for the ``etlplus load`` CLI command."""

    def test_load_stdin_to_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        parse_json_file: JsonFileParser,
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
        payload = parse_json_output(out)
        assert payload.get('status') == 'success'
        assert out_path.exists()
        assert parse_json_file(out_path) == sample_records
