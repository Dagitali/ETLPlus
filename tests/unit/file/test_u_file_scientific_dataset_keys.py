"""
:mod:`tests.unit.file.test_u_file_scientific_dataset_keys` module.

Focused unit tests for scientific handler dataset-key validation behavior.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Literal
from typing import cast

import pytest

from etlplus.file import mat as mat_mod
from etlplus.file import sylk as sylk_mod
from etlplus.file import zsav as zsav_mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import WriteOptions

# SECTION: INTERNAL CONSTANTS =============================================== #


_SCIENTIFIC_STUB_MODULES: list[
    tuple[ModuleType, type[ScientificDatasetFileHandlerABC], str]
] = [
    (mat_mod, mat_mod.MatFile, 'mat'),
    (sylk_mod, sylk_mod.SylkFile, 'sylk'),
    (zsav_mod, zsav_mod.ZsavFile, 'zsav'),
]


type ScientificModuleCase = tuple[
    ModuleType,
    type[ScientificDatasetFileHandlerABC],
    str,
]
type Operation = Literal['read', 'write']


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='scientific_module_case',
    params=_SCIENTIFIC_STUB_MODULES,
    ids=[case[2] for case in _SCIENTIFIC_STUB_MODULES],
)
def scientific_module_case_fixture(
    request: pytest.FixtureRequest,
) -> ScientificModuleCase:
    """Parametrize scientific-stub dataset-key tests by file format."""
    return cast(ScientificModuleCase, request.param)


# SECTION: TESTS ============================================================ #


class TestScientificStubDatasetKeys:
    """
    Unit tests for dataset-key validation in stub-backed scientific files.
    """

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
                self._raise_stub_called,
            )
        if operation in (None, 'write'):
            monkeypatch.setattr(
                stub_module,
                'write',
                self._raise_stub_called,
            )

    def test_dataset_methods_honor_options_dataset_selector(
        self,
        monkeypatch: pytest.MonkeyPatch,
        scientific_module_case: ScientificModuleCase,
    ) -> None:
        """Test read_dataset/write_dataset honoring options-based selectors."""
        module, handler_cls, _ = scientific_module_case
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

    def test_list_datasets_returns_single_default_key(
        self,
        scientific_module_case: ScientificModuleCase,
    ) -> None:
        """Test list_datasets exposing only the default dataset key."""
        _, handler_cls, _ = scientific_module_case
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
        scientific_module_case: ScientificModuleCase,
    ) -> None:
        """Test dataset methods rejecting unknown keys before stub I/O."""
        module, handler_cls, _ = scientific_module_case
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
        scientific_module_case: ScientificModuleCase,
    ) -> None:
        """Test option-based selectors following the same validation path."""
        module, handler_cls, _ = scientific_module_case
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

    @staticmethod
    def _raise_stub_called(
        *_args: object,
        **_kwargs: object,
    ) -> object:
        """
        Raise when a stubbed scientific I/O function is unexpectedly used.
        """
        raise AssertionError('stub operation should not be called')
