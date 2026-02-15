"""Smoke tests for etlplus.file.properties."""

from __future__ import annotations

from etlplus.file import properties as mod
from etlplus.types import JSONData
from etlplus.types import JSONDict
from etlplus.types import JSONList

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestProperties(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.properties."""

    module = mod
    file_name = 'data.properties'

    def build_payload(
        self,
        *,
        sample_record: JSONDict,
        sample_records: JSONList,  # noqa: ARG002
    ) -> JSONData:
        """Build string-only properties payload from one sample record."""
        return {key: str(value) for key, value in sample_record.items()}
