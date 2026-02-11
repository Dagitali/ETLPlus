"""
:mod:`tests.unit.file.test_u_file_scientific_dataset_keys` module.

Focused unit tests for scientific handler dataset-key validation behavior.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from typing import cast

import pytest

from etlplus.file import mat as mat_mod
from etlplus.file import sylk as sylk_mod
from etlplus.file import zsav as zsav_mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.stub import StubFileHandlerABC

# SECTION: INTERNAL CONSTANTS =============================================== #


_SCIENTIFIC_STUB_MODULES: list[
    tuple[type[ScientificDatasetFileHandlerABC], str]
] = [
    (mat_mod.MatFile, 'mat'),
    (sylk_mod.SylkFile, 'sylk'),
    (zsav_mod.ZsavFile, 'zsav'),
]


type DatasetSelectorMode = Literal['dataset_kwarg', 'options']
type Operation = Literal['read', 'write']
type ScientificModuleCase = tuple[
    type[ScientificDatasetFileHandlerABC],
    str,
]


# SECTION: FIXTURES ========================================================= #


@pytest.fixture(
    name='scientific_module_case',
    params=_SCIENTIFIC_STUB_MODULES,
    ids=[case[1] for case in _SCIENTIFIC_STUB_MODULES],
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
        monkeypatch: pytest.MonkeyPatch,
        *,
        operation: Operation | None = None,
    ) -> None:
        """Patch stub base operations to fail if they are called."""
        if operation in (None, 'read'):
            monkeypatch.setattr(
                StubFileHandlerABC,
                'read',
                self._raise_stub_called,
            )
        if operation in (None, 'write'):
            monkeypatch.setattr(
                StubFileHandlerABC,
                'write',
                self._raise_stub_called,
            )

    @pytest.mark.parametrize(
        ('operation', 'method_name', 'selector_mode'),
        [
            ('read', 'read_dataset', 'dataset_kwarg'),
            ('write', 'write_dataset', 'dataset_kwarg'),
            ('read', 'read_dataset', 'options'),
            ('write', 'write_dataset', 'options'),
            ('read', 'read', 'options'),
            ('write', 'write', 'options'),
        ],
        ids=[
            'read_dataset+dataset',
            'write_dataset+dataset',
            'read_dataset+options',
            'write_dataset+options',
            'read+options',
            'write+options',
        ],
    )
    def test_methods_reject_unknown_dataset_key_before_stub_io(
        self,
        monkeypatch: pytest.MonkeyPatch,
        operation: Operation,
        method_name: str,
        selector_mode: DatasetSelectorMode,
        scientific_module_case: ScientificModuleCase,
    ) -> None:
        """Test unknown dataset keys are validated before stub operations."""
        handler_cls, _ = scientific_module_case
        handler = handler_cls()
        self._assert_stub_not_called(
            monkeypatch,
            operation=operation,
        )
        method = getattr(handler, method_name)
        args: tuple[object, ...] = (Path('ignored.file'),)
        if operation == 'write':
            args = (*args, [])

        kwargs: dict[str, object] = {}
        if selector_mode == 'dataset_kwarg':
            kwargs['dataset'] = 'unknown'
        elif operation == 'read':
            kwargs['options'] = ReadOptions(dataset='unknown')
        else:
            kwargs['options'] = WriteOptions(dataset='unknown')

        with pytest.raises(ValueError, match='supports only dataset key'):
            method(*args, **kwargs)

    def test_list_datasets_returns_single_default_key(
        self,
        scientific_module_case: ScientificModuleCase,
    ) -> None:
        """Test list_datasets exposing only the default dataset key."""
        handler_cls, _ = scientific_module_case
        handler = handler_cls()
        assert handler.list_datasets(Path('ignored.file')) == ['data']

    @staticmethod
    def _raise_stub_called(
        *_args: object,
        **_kwargs: object,
    ) -> object:
        """
        Raise when a stubbed scientific I/O function is unexpectedly used.
        """
        raise AssertionError('stub operation should not be called')
