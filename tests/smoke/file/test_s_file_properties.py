"""Smoke tests for etlplus.file.properties."""

from __future__ import annotations

from typing import Any

from etlplus.file import properties as mod
from tests.smoke.file.conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestProperties(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.properties."""

    module = mod
    file_name = 'data.properties'

    def build_payload(
        self,
        *,
        sample_record: dict[str, Any],
        sample_records: list[dict[str, Any]],  # noqa: ARG002
    ) -> object:
        """Build string-only properties payload from one sample record."""
        return {key: str(value) for key, value in sample_record.items()}
