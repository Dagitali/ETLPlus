"""
:mod:`tests.unit.file.pytest_file_contract_classes` module.

Compatibility re-exports for unit file contract classes, mixins, and helpers.
"""

from __future__ import annotations

from tests.unit.file.pytest_file_contract_contracts import (
    ArchiveWrapperCoreDispatchModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    BinaryCodecModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    BinaryDependencyModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    BinaryKeyedPayloadModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    DelimitedModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    EmbeddedDatabaseModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    PandasColumnarModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    PyarrowGatedPandasColumnarModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    PyarrowMissingDependencyMixin,
)
from tests.unit.file.pytest_file_contract_contracts import RDataModuleContract
from tests.unit.file.pytest_file_contract_contracts import (
    ReadOnlyScientificDatasetModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    SemiStructuredReadModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    SemiStructuredWriteDictModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    SingleDatasetPlaceholderContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    SingleDatasetWritableContract,
)
from tests.unit.file.pytest_file_contract_contracts import StubModuleContract
from tests.unit.file.pytest_file_contract_contracts import (
    TextRowModuleContract,
)
from tests.unit.file.pytest_file_contract_contracts import (
    WritableSpreadsheetModuleContract,
)
from tests.unit.file.pytest_file_contract_mixins import OptionalModuleInstaller
from tests.unit.file.pytest_file_contract_mixins import PathMixin
from tests.unit.file.pytest_file_contract_mixins import ReadOnlyWriteGuardMixin
from tests.unit.file.pytest_file_contract_mixins import RoundtripSpec
from tests.unit.file.pytest_file_contract_mixins import (
    RoundtripUnitModuleContract,
)
from tests.unit.file.pytest_file_contract_mixins import (
    SpreadsheetCategoryContractBase,
)
from tests.unit.file.pytest_file_contract_mixins import (
    SpreadsheetReadImportErrorMixin,
)
from tests.unit.file.pytest_file_contract_utils import (
    assert_single_dataset_rejects_non_default_key,
)
from tests.unit.file.pytest_file_contract_utils import (
    patch_dependency_resolver_unreachable,
)
from tests.unit.file.pytest_file_contract_utils import (
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
