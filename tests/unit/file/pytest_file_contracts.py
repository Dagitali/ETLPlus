"""
:mod:`tests.unit.file.pytest_file_contracts` module.

Compatibility facade re-exporting domain contract suites for
:mod:`etlplus.file` unit tests.
"""

from __future__ import annotations

from .pytest_file_contracts_binary import (
    ArchiveWrapperCoreDispatchModuleContract,
)
from .pytest_file_contracts_binary import BinaryCodecModuleContract
from .pytest_file_contracts_binary import BinaryDependencyModuleContract
from .pytest_file_contracts_binary import BinaryKeyedPayloadModuleContract
from .pytest_file_contracts_binary import StubModuleContract
from .pytest_file_contracts_dataset import RDataModuleContract
from .pytest_file_contracts_dataset import (
    ReadOnlyScientificDatasetModuleContract,
)
from .pytest_file_contracts_dataset import SemiStructuredReadModuleContract
from .pytest_file_contracts_dataset import (
    SemiStructuredWriteDictModuleContract,
)
from .pytest_file_contracts_dataset import SingleDatasetHandlerContract
from .pytest_file_contracts_dataset import SingleDatasetPlaceholderContract
from .pytest_file_contracts_dataset import SingleDatasetWritableContract
from .pytest_file_contracts_tabular import DelimitedModuleContract
from .pytest_file_contracts_tabular import EmbeddedDatabaseModuleContract
from .pytest_file_contracts_tabular import PandasColumnarModuleContract
from .pytest_file_contracts_tabular import (
    PyarrowGatedPandasColumnarModuleContract,
)
from .pytest_file_contracts_tabular import PyarrowMissingDependencyMixin
from .pytest_file_contracts_tabular import TextRowModuleContract
from .pytest_file_contracts_tabular import WritableSpreadsheetModuleContract

# SECTION: EXPORTS ========================================================== #


__all__ = [
    'ArchiveWrapperCoreDispatchModuleContract',
    'BinaryCodecModuleContract',
    'BinaryDependencyModuleContract',
    'BinaryKeyedPayloadModuleContract',
    'DelimitedModuleContract',
    'EmbeddedDatabaseModuleContract',
    'PandasColumnarModuleContract',
    'PyarrowGatedPandasColumnarModuleContract',
    'RDataModuleContract',
    'ReadOnlyScientificDatasetModuleContract',
    'SemiStructuredReadModuleContract',
    'SemiStructuredWriteDictModuleContract',
    'SingleDatasetHandlerContract',
    'SingleDatasetPlaceholderContract',
    'SingleDatasetWritableContract',
    'StubModuleContract',
    'TextRowModuleContract',
    'PyarrowMissingDependencyMixin',
    'WritableSpreadsheetModuleContract',
]
