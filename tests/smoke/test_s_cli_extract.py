"""
:mod:`tests.smoke.test_s_cli_extract` module.

Smoke tests for the ``etlplus extract`` CLI command.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser

# SECTION: TESTS ============================================================ #


class TestCliExtract:
    """Smoke tests for the ``etlplus extract`` CLI command."""

    def test_extract_json_file(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
        json_payload_file: Path,
        sample_records: list[dict[str, Any]],
    ) -> None:
        """Test extracting JSON from a file and emit matching payload."""
        code, out, err = cli_invoke(('extract', str(json_payload_file)))
        assert code == 0
        assert err.strip() == ''
        payload = parse_json_output(out)
        assert payload == sample_records
