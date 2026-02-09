"""
:mod:`tests.unit.file.test_u_file_scientific_dataset_keys` module.

Focused unit tests for scientific handler dataset-key validation behavior.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Literal

import pytest

from etlplus.file import mat as mat_mod
from etlplus.file import sylk as sylk_mod
from etlplus.file import zsav as zsav_mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import WriteOptions

# SECTION: INTERNAL CONSTANTS =============================================== #


_SCIENTIFIC_STUB_MODULES: list[
    tuple[
        ModuleType,
        type[ScientificDatasetFileHandlerABC],
        str,
    ]
] = [
    (mat_mod, mat_mod.MatFile, 'mat'),
    (sylk_mod, sylk_mod.SylkFile, 'sylk'),
    (zsav_mod, zsav_mod.ZsavFile, 'zsav'),
]


type Operation = Literal['read', 'write']


class ScientificStubDatasetKeysContract:
    """Reusable contract suite for stub-backed scientific dataset keys."""

    module_cases: list[
        tuple[
            ModuleType,
            type[ScientificDatasetFileHandlerABC],
            str,
        ]
    ]

    def _assert_stub_not_called(
        self,
        module: ModuleType,
        monkeypatch: pytest.MonkeyPatch,
        *,
        operation: Operation | None = None,
    ) -> None:
        """Patch module stub operations to fail if they are called."""
        stub_module = module.stub
        if operation in (None, 'read'):
            monkeypatch.setattr(
                stub_module,
                'read',
                lambda *_, **__: (_ for _ in ()).throw(AssertionError),
            )
        if operation in (None, 'write'):
            monkeypatch.setattr(
                stub_module,
                'write',
                lambda *_, **__: (_ for _ in ()).throw(AssertionError),
            )

    def test_dataset_methods_honor_options_dataset_selector(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read_dataset/write_dataset honoring options-based selectors."""
        for module, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            self._assert_stub_not_called(module, monkeypatch)
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.read_dataset(
                    Path('ignored.file'),
                    options=ReadOptions(dataset='unknown'),
                )
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.write_dataset(
                    Path('ignored.file'),
                    [],
                    options=WriteOptions(dataset='unknown'),
                )

    def test_list_datasets_returns_single_default_key(self) -> None:
        """Test list_datasets exposing only the default dataset key."""
        for _, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            assert handler.list_datasets(Path('ignored.file')) == ['data']

    @pytest.mark.parametrize(
        ('operation', 'method_name'),
        [('read', 'read_dataset'), ('write', 'write_dataset')],
        ids=['read_dataset', 'write_dataset'],
    )
    def test_dataset_methods_reject_unknown_key_without_calling_stub(
        self,
        monkeypatch: pytest.MonkeyPatch,
        operation: Operation,
        method_name: str,
    ) -> None:
        """Test dataset methods rejecting unknown keys before stub I/O."""
        for module, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            self._assert_stub_not_called(
                module,
                monkeypatch,
                operation=operation,
            )
            method = getattr(handler, method_name)
            with pytest.raises(ValueError, match='supports only dataset key'):
                args: tuple[object, ...] = (Path('ignored.file'),)
                if operation == 'write':
                    args = (*args, [])
                method(*args, dataset='unknown')

    def test_read_and_write_options_route_unknown_dataset_to_validation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test option-based selectors following the same validation path."""
        for module, handler_cls, _ in self.module_cases:
            handler = handler_cls()
            self._assert_stub_not_called(module, monkeypatch)
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.read(
                    Path('ignored.file'),
                    options=ReadOptions(dataset='unknown'),
                )
            with pytest.raises(ValueError, match='supports only dataset key'):
                handler.write(
                    Path('ignored.file'),
                    [],
                    options=WriteOptions(dataset='unknown'),
                )


# SECTION: TESTS ============================================================ #


class TestScientificStubDatasetKeys(ScientificStubDatasetKeysContract):
    """
    Unit tests for dataset-key validation in stub-backed scientific files.
    """

    module_cases = _SCIENTIFIC_STUB_MODULES
