"""
:mod:`tests.unit.file.pytest_file_contract_bases` module.

Reusable contract base classes for unit tests of :mod:`etlplus.file`.
"""

from __future__ import annotations

from pathlib import Path

from etlplus.types import JSONData

from .pytest_file_contract_mixins import OptionalModuleInstaller
from .pytest_file_contract_mixins import PathMixin
from .pytest_file_contract_utils import make_payload

# SECTION: EXPORTS ========================================================== #


__all__ = [
    # Classes
    'DelimitedCategoryContractBase',
    'ScientificCategoryContractBase',
    'SemiStructuredCategoryContractBase',
    'SpreadsheetCategoryContractBase',
]


# SECTION: CLASSES ========================================================== #


class DelimitedCategoryContractBase(PathMixin):
    """
    Shared base contract for delimited/text category modules.
    """

    sample_rows: JSONData = [{'id': 1}]


class ScientificCategoryContractBase(PathMixin):
    """
    Shared base contract for scientific dataset handlers/modules.
    """

    dataset_key: str = 'data'


class SemiStructuredCategoryContractBase(PathMixin):
    """
    Shared base contract for semi-structured text modules.
    """

    # pylint: disable=unused-argument

    sample_read_text: str = ''
    expected_read_payload: JSONData = make_payload('dict')
    dict_payload: JSONData = make_payload('dict')

    def setup_read_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for read tests."""

    def setup_write_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Install optional dependencies needed for write tests."""

    def assert_read_contract_result(
        self,
        result: JSONData,
    ) -> None:
        """Assert module-specific read contract expectations."""
        assert result == self.expected_read_payload

    def assert_write_contract_result(
        self,
        path: Path,
    ) -> None:
        """Assert module-specific write contract expectations."""
        assert path.exists()


class SpreadsheetCategoryContractBase(PathMixin):
    """
    Shared base contract for spreadsheet format handlers.
    """

    dependency_hint: str
    read_engine: str | None = None
    write_engine: str | None = None
