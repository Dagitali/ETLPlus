"""
:mod:`tests.unit.file.test_u_file_xml` module.

Unit tests for :mod:`etlplus.file.xml`.
"""

from __future__ import annotations

from etlplus.file import xml as mod
from tests.unit.file.conftest import XmlModuleContract

# SECTION: TESTS ============================================================ #


class TestXml(XmlModuleContract):
    """Unit tests for :mod:`etlplus.file.xml`."""

    module = mod
    format_name = 'xml'
    root_tag = 'rows'
