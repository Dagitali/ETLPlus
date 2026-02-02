"""
:mod:`tests.smoke.test_s_cli_validate` module.

Smoke test suite for the ``etlplus validate`` CLI command.
"""

from __future__ import annotations

import io
import json
import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from tests.conftest import CliInvoke


# SECTION: HELPERS ========================================================== #


pytestmark = pytest.mark.smoke


# SECTION: TESTS ============================================================ #


class TestCliValidate:
    """Smoke test suite for the ``etlplus validate`` CLI command."""

    def test_stdin_payload(
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
