"""
:mod:`tests.unit.file.pytest_file_contracts_dataset` module.

Scientific/semi-structured dataset contract suites for unit tests of
:mod:`etlplus.file`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.types import JSONData

from .pytest_file_contract_bases import ScientificCategoryContractBase
from .pytest_file_contract_bases import SemiStructuredCategoryContractBase
from .pytest_file_contract_mixins import EmptyWriteReturnsZeroMixin
from .pytest_file_contract_mixins import PathMixin
from .pytest_file_contract_mixins import ReadOnlyWriteGuardMixin
from .pytest_file_contract_mixins import ScientificReadOnlyUnknownDatasetMixin
from .pytest_file_contract_mixins import ScientificSingleDatasetHandlerMixin
from .pytest_file_contract_mixins import SemiStructuredReadMixin
from .pytest_file_contract_mixins import SemiStructuredWriteDictMixin
from .pytest_file_contract_utils import Operation
from .pytest_file_contract_utils import (
    call_module_operation as _call_module_operation,
)
from .pytest_file_contract_utils import make_payload
from .pytest_file_types import OptionalModuleInstaller

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'RDataModuleContract',
    'ReadOnlyScientificDatasetModuleContract',
    'SemiStructuredReadModuleContract',
    'SemiStructuredWriteDictModuleContract',
    'SingleDatasetHandlerContract',
    'SingleDatasetPlaceholderContract',
    'SingleDatasetWritableContract',
]


# SECTION: CLASSES ========================================================== #

class RDataModuleContract(PathMixin):
    """Reusable contract suite for R-data wrapper modules (RDA/RDS)."""

    writer_missing_pattern: str
    write_payload: JSONData = make_payload('list')

    def _install_optional_dependencies(
        self,
        optional_module_stub: OptionalModuleInstaller,
        *,
        pyreadr_stub: object,
    ) -> None:
        """Install module stubs required by R-data contract tests."""
        optional_module_stub(
            {
                'pyreadr': pyreadr_stub,
                'pandas': self.build_pandas_stub(),
            },
        )

    def build_frame(
        self,
        records: list[dict[str, object]],
    ) -> object:
        """Build a frame-like stub from row records."""
        raise NotImplementedError

    def build_pandas_stub(self) -> object:
        """Build pandas module stub."""
        raise NotImplementedError

    def build_pyreadr_stub(
        self,
        result: dict[str, object],
    ) -> object:
        """Build pyreadr module stub."""
        raise NotImplementedError

    def build_reader_only_stub(self) -> object:
        """Build pyreadr-like stub without writer methods."""
        raise NotImplementedError

    def assert_write_success(
        self,
        pyreadr_stub: object,
        path: Path,
    ) -> None:
        """Assert module-specific write success behavior."""
        _ = pyreadr_stub
        _ = path

    def test_read_empty_result_returns_empty_list(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading empty R-data results returning an empty list."""
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_pyreadr_stub({}),
        )

        assert self.module_handler.read(self.format_path(tmp_path)) == []

    def test_read_single_value_coerces_to_records(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading one R object coercing to JSON records."""
        frame = self.build_frame([{'id': 1}])
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_pyreadr_stub({'data': frame}),
        )

        assert self.module_handler.read(self.format_path(tmp_path)) == [
            {'id': 1},
        ]

    def test_read_multiple_values_returns_mapping(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test reading multiple R objects returning key-mapped payloads."""
        result: dict[str, object] = {'one': {'id': 1}, 'two': [{'id': 2}]}
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_pyreadr_stub(result),
        )

        assert self.module_handler.read(self.format_path(tmp_path)) == result

    def test_write_raises_when_writer_missing(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test writing failing when pyreadr writer methods are unavailable."""
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=self.build_reader_only_stub(),
        )

        with pytest.raises(ImportError, match=self.writer_missing_pattern):
            self.module_handler.write(
                self.format_path(tmp_path),
                self.write_payload,
            )

    def test_write_happy_path_uses_writer(
        self,
        tmp_path: Path,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Test writing delegating to pyreadr writer methods."""
        pyreadr = self.build_pyreadr_stub({})
        self._install_optional_dependencies(
            optional_module_stub,
            pyreadr_stub=pyreadr,
        )
        path = self.format_path(tmp_path)

        written = self.module_handler.write(path, self.write_payload)

        assert written == 1
        self.assert_write_success(pyreadr, path)


class ReadOnlyScientificDatasetModuleContract(
    ScientificCategoryContractBase,
    ScientificReadOnlyUnknownDatasetMixin,
    ReadOnlyWriteGuardMixin,
):
    """
    Reusable contract suite for read-only scientific dataset handlers.
    """


class SemiStructuredReadModuleContract(
    SemiStructuredCategoryContractBase,
    SemiStructuredReadMixin,
):
    """
    Reusable read contract suite for semi-structured text modules.
    """


class SemiStructuredWriteDictModuleContract(
    SemiStructuredCategoryContractBase,
    SemiStructuredWriteDictMixin,
):
    """
    Reusable write contract suite for semi-structured text modules.
    """


class SingleDatasetHandlerContract(
    ScientificCategoryContractBase,
    ScientificSingleDatasetHandlerMixin,
):
    """Reusable contract suite for single-dataset scientific handlers."""


class SingleDatasetWritableContract(
    EmptyWriteReturnsZeroMixin,
    SingleDatasetHandlerContract,
):
    """
    Reusable suite for writable single-dataset scientific handlers.
    """

    assert_file_not_created_on_empty_write = True


class SingleDatasetPlaceholderContract(SingleDatasetHandlerContract):
    """
    Reusable suite for placeholder single-dataset scientific handlers.
    """

    @pytest.mark.parametrize('operation', ['read', 'write'])
    def test_module_level_placeholders_raise_not_implemented(
        self,
        tmp_path: Path,
        operation: Operation,
    ) -> None:
        """Test placeholder read/write behavior for module-level wrappers."""
        path = self.format_path(tmp_path)
        with pytest.raises(NotImplementedError, match='not implemented yet'):
            _call_module_operation(
                self.module,
                operation=operation,
                path=path,
            )
