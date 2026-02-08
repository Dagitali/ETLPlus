"""
:mod:`tests.unit.file.test_u_file_scientific_dataset_keys` module.

Focused unit tests for scientific handler dataset-key validation behavior.
"""

from __future__ import annotations

from types import ModuleType

from etlplus.file import mat as mat_mod
from etlplus.file import sylk as sylk_mod
from etlplus.file import zsav as zsav_mod
from etlplus.file.base import ScientificDatasetFileHandlerABC
from tests.unit.file.conftest import ScientificStubDatasetKeysContract

# SECTION: INTERNAL CONSTANTS =============================================== #


_SCIENTIFIC_STUB_MODULES: list[
    tuple[ModuleType, type[ScientificDatasetFileHandlerABC], str]
] = [
    (mat_mod, mat_mod.MatFile, 'mat'),
    (sylk_mod, sylk_mod.SylkFile, 'sylk'),
    (zsav_mod, zsav_mod.ZsavFile, 'zsav'),
]


# SECTION: TESTS ============================================================ #


class TestScientificStubDatasetKeys(ScientificStubDatasetKeysContract):
    """
    Unit tests for dataset-key validation in stub-backed scientific files.
    """

    module_cases = _SCIENTIFIC_STUB_MODULES
