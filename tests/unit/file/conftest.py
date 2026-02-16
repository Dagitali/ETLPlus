"""
:mod:`tests.unit.file.conftest` module.

Directory-local pytest configuration for unit tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

from collections.abc import Callable
from collections.abc import Generator

import pytest

import etlplus.file._imports as import_helpers

from .pytest_file_support import PandasModuleStub
from .pytest_file_support import RecordsFrameStub
from .pytest_file_support import make_import_error_reader_module
from .pytest_file_support import make_import_error_writer_module

# SECTION: TYPE ALIASES ===================================================== #


type OptionalModuleInstaller = Callable[[dict[str, object]], None]


# SECTION: EXPORTS ========================================================== #


__all__ = [
    'make_import_error_reader_fixture',
    'make_import_error_writer_fixture',
    'make_pandas_stub_fixture',
    'make_records_frame_fixture',
    'optional_module_stub_fixture',
]


# SECTION: MARKERS ========================================================== #


# Directory-level marker for unit tests.
pytestmark = [pytest.mark.unit]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(name='make_import_error_reader')
def make_import_error_reader_fixture() -> Callable[[str], object]:
    """Return factory for module-like objects with failing reader methods."""
    return make_import_error_reader_module


@pytest.fixture(name='make_import_error_writer')
def make_import_error_writer_fixture() -> Callable[[], object]:
    """Return factory for pandas-like objects with failing write paths."""
    return make_import_error_writer_module


@pytest.fixture(name='make_pandas_stub')
def make_pandas_stub_fixture() -> Callable[
    [RecordsFrameStub],
    PandasModuleStub,
]:
    """Return factory for :class:`PandasModuleStub` test doubles."""
    return PandasModuleStub


@pytest.fixture(name='make_records_frame')
def make_records_frame_fixture() -> Callable[
    [list[dict[str, object]]],
    RecordsFrameStub,
]:
    """Return factory for :class:`RecordsFrameStub` test doubles."""
    return RecordsFrameStub


@pytest.fixture(name='optional_module_stub')
def optional_module_stub_fixture() -> Generator[OptionalModuleInstaller]:
    """Install optional dependency stubs and restore import cache afterward."""
    cache = import_helpers._MODULE_CACHE  # pylint: disable=protected-access
    original = dict(cache)
    cache.clear()

    def _install(mapping: dict[str, object]) -> None:
        cache.update(mapping)

    try:
        yield _install
    finally:
        cache.clear()
        cache.update(original)
