"""
:mod:`tests.unit.file.conftest` module.

Directory-local pytest configuration for unit tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

import pytest

from .pytest_file_stubs import make_import_error_reader_fixture
from .pytest_file_stubs import make_import_error_writer_fixture
from .pytest_file_stubs import make_pandas_stub_fixture
from .pytest_file_stubs import make_records_frame_fixture
from .pytest_file_stubs import optional_module_stub_fixture

# SECCION: EXPORT =========================================================== #


__all__ = [
    'make_import_error_reader_fixture',
    'make_import_error_writer_fixture',
    'make_pandas_stub_fixture',
    'make_records_frame_fixture',
    'optional_module_stub_fixture',
]


# SECTION: MA

# Directory-level marker for unit tests.
pytestmark = [pytest.mark.unit]
