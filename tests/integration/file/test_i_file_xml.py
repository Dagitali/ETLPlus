"""
:mod:`tests.integration.file.test_i_file_xml` module.

Integration smoke tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from etlplus.file import xml as mod

from .conftest import SmokeRoundtripModuleContract

# SECTION: TESTS ============================================================ #


class TestXml(SmokeRoundtripModuleContract):
    """Integration smoke tests for :mod:`etlplus.file.xml`."""

    module = mod
    payload = {'root': {'text': 'hello'}}
    write_kwargs = {'root_tag': 'root'}
