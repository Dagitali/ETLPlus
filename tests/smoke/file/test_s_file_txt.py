"""Smoke tests for etlplus.file.txt."""

from __future__ import annotations

from typing import Any

from etlplus.file import txt as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTxt(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.txt."""

    module = mod
    file_name = 'data.txt'

    def build_payload(
        self,
        *,
        sample_record: dict[str, Any],
        sample_records: list[dict[str, Any]],  # noqa: ARG002
    ) -> object:
        """Build a text payload from sample record values."""
        text = '\n'.join(str(value) for value in sample_record.values())
        return {'text': text}
