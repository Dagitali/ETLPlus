"""
:mod:`tests.smoke.test_s_cli_transform` module.

Smoke test suite for the ``etlplus transform`` CLI command.
"""

from __future__ import annotations

import io
import sys
from typing import TYPE_CHECKING
from typing import Any

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke
    from tests.conftest import JsonOutputParser


# SECTION: MARKERS ========================================================== #


# Directory-level marker for smoke tests.
pytestmark = pytest.mark.smoke


# SECTION: TESTS ============================================================ #


class TestCliTransform:
    """Smoke test suite for the ``etlplus transform`` CLI command."""

    def test_stdin_select(
        self,
        cli_invoke: CliInvoke,
        parse_json_output: JsonOutputParser,
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
        payload = parse_json_output(out)
        expected = [{'id': rec['id']} for rec in sample_records]
        assert payload == expected
