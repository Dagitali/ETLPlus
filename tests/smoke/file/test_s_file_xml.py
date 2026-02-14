"""Smoke tests for etlplus.file.xml."""

from __future__ import annotations

from etlplus.file import xml as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXml(SmokeRoundtripModuleContract):
    """Smoke tests for etlplus.file.xml."""

    module = mod
    file_name = 'data.xml'
    payload = {'root': {'text': 'hello'}}
    write_kwargs = {'root_tag': 'root'}
