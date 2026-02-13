"""
:mod:`tests.unit.file.conftest` module.

Shared fixtures, contracts, and stubs for pytest-based unit tests of
:mod:`etlplus.file`.
"""

from __future__ import annotations

import pytest

from tests.unit.file.pytest_file_contract_classes import (
    ArchiveWrapperCoreDispatchModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    BinaryCodecModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    BinaryDependencyModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    BinaryKeyedPayloadModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    DelimitedModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    EmbeddedDatabaseModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    OptionalModuleInstaller,
)
from tests.unit.file.pytest_file_contract_classes import (
    PandasColumnarModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import PathMixin
from tests.unit.file.pytest_file_contract_classes import (
    PyarrowGatedPandasColumnarModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    PyarrowMissingDependencyMixin,
)
from tests.unit.file.pytest_file_contract_classes import RDataModuleContract
from tests.unit.file.pytest_file_contract_classes import (
    ReadOnlyScientificDatasetModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    ReadOnlyWriteGuardMixin,
)
from tests.unit.file.pytest_file_contract_classes import RoundtripSpec
from tests.unit.file.pytest_file_contract_classes import (
    RoundtripUnitModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    SemiStructuredReadModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    SemiStructuredWriteDictModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    SingleDatasetPlaceholderContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    SingleDatasetWritableContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    SpreadsheetCategoryContractBase,
)
from tests.unit.file.pytest_file_contract_classes import (
    SpreadsheetReadImportErrorMixin,
)
from tests.unit.file.pytest_file_contract_classes import StubModuleContract
from tests.unit.file.pytest_file_contract_classes import TextRowModuleContract
from tests.unit.file.pytest_file_contract_classes import (
    WritableSpreadsheetModuleContract,
)
from tests.unit.file.pytest_file_contract_classes import (
    assert_single_dataset_rejects_non_default_key,
)
from tests.unit.file.pytest_file_contract_classes import (
    patch_dependency_resolver_unreachable,
)
from tests.unit.file.pytest_file_contract_classes import (
    patch_dependency_resolver_value,
)
from tests.unit.file.pytest_file_support import BinaryCodecStub
from tests.unit.file.pytest_file_support import ContextManagerSelfMixin
from tests.unit.file.pytest_file_support import CoreDispatchFileStub
from tests.unit.file.pytest_file_support import DictRecordsFrameStub
from tests.unit.file.pytest_file_support import PandasModuleStub
from tests.unit.file.pytest_file_support import PandasReadSasStub
from tests.unit.file.pytest_file_support import PyreadrStub
from tests.unit.file.pytest_file_support import PyreadstatTabularStub
from tests.unit.file.pytest_file_support import RDataNoWriterStub
from tests.unit.file.pytest_file_support import RDataPandasStub
from tests.unit.file.pytest_file_support import RecordsFrameStub
from tests.unit.file.pytest_file_support import SpreadsheetSheetFrameStub
from tests.unit.file.pytest_file_support import SpreadsheetSheetPandasStub

# Directory-level marker for unit tests.
pytestmark = pytest.mark.unit

__all__ = [
    'ArchiveWrapperCoreDispatchModuleContract',
    'BinaryCodecModuleContract',
    'BinaryCodecStub',
    'BinaryDependencyModuleContract',
    'BinaryKeyedPayloadModuleContract',
    'ContextManagerSelfMixin',
    'CoreDispatchFileStub',
    'DelimitedModuleContract',
    'DictRecordsFrameStub',
    'EmbeddedDatabaseModuleContract',
    'OptionalModuleInstaller',
    'PandasColumnarModuleContract',
    'PandasModuleStub',
    'PandasReadSasStub',
    'PathMixin',
    'PyarrowGatedPandasColumnarModuleContract',
    'PyarrowMissingDependencyMixin',
    'PyreadrStub',
    'PyreadstatTabularStub',
    'RDataModuleContract',
    'RDataNoWriterStub',
    'RDataPandasStub',
    'ReadOnlyScientificDatasetModuleContract',
    'ReadOnlyWriteGuardMixin',
    'RecordsFrameStub',
    'RoundtripSpec',
    'RoundtripUnitModuleContract',
    'SemiStructuredReadModuleContract',
    'SemiStructuredWriteDictModuleContract',
    'SingleDatasetPlaceholderContract',
    'SingleDatasetWritableContract',
    'SpreadsheetCategoryContractBase',
    'SpreadsheetReadImportErrorMixin',
    'SpreadsheetSheetFrameStub',
    'SpreadsheetSheetPandasStub',
    'StubModuleContract',
    'TextRowModuleContract',
    'WritableSpreadsheetModuleContract',
    'assert_single_dataset_rejects_non_default_key',
    'patch_dependency_resolver_unreachable',
    'patch_dependency_resolver_value',
]
