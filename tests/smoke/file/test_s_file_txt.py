"""Smoke tests for etlplus.file.txt."""

from __future__ import annotations

from etlplus.file import txt as mod
from etlplus.types import JSONData
from etlplus.types import JSONDict
from etlplus.types import JSONList

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestTxt(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.txt."""

    module = mod
    file_name = 'data.txt'

    def build_payload(
        self,
        *,
        sample_record: JSONDict,
        sample_records: JSONList,  # noqa: ARG002
    ) -> JSONData:
        """Build a text payload from sample record values."""
        text = '\n'.join(str(value) for value in sample_record.values())
        return {'text': text}
