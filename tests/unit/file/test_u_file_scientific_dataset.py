"""
:mod:`tests.unit.file.test_u_file_scientific_dataset` module.

Focused unit tests for scientific dataset-key helper behavior.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from etlplus.file import _scientific_dataset as scientific_dataset_mod
from etlplus.file import mat as mat_mod
from etlplus.file import sylk as sylk_mod
from etlplus.file import zsav as zsav_mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import WriteOptions
from etlplus.file.stub import StubFileHandlerABC

# SECTION: PRAGMAS ========================================================== #

# pylint: disable=import-outside-toplevel,protected-access,unused-argument

# SECTION: INTERNAL CONSTANTS =============================================== #


_SCIENTIFIC_STUB_MODULES: tuple[object, ...] = (
    pytest.param((mat_mod.MatFile, 'mat'), id='mat'),
    pytest.param((sylk_mod.SylkFile, 'sylk'), id='sylk'),
    pytest.param((zsav_mod.ZsavFile, 'zsav'), id='zsav'),
)


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
)
def scientific_module_case_fixture(
    request: pytest.FixtureRequest,
) -> ScientificModuleCase:
    """Parametrize scientific-stub dataset-key tests by file format."""
    case = request.param
    assert isinstance(case, tuple)
    assert len(case) == 2
    handler_cls, format_name = case
    assert isinstance(format_name, str)
    assert issubclass(handler_cls, ScientificDatasetFileHandlerABC)
    return handler_cls, format_name


# SECTION: TESTS ============================================================ #


class TestScientificStubDatasetKeys:
    """
    Unit tests for dataset-key validation in stub-backed scientific files.
    """

    def test_list_datasets_returns_single_default_key(
        self,
        scientific_module_case: ScientificModuleCase,
    ) -> None:
        """
        Test that :meth:`list_datasets` exposes only the default dataset key.
        """
        handler_cls, _ = scientific_module_case
        handler = handler_cls()
        assert handler.list_datasets(Path('ignored.file')) == ['data']

    @pytest.mark.parametrize(
        ('operation', 'method_name', 'selector_mode'),
        [
            pytest.param(
                'read',
                'read_dataset',
                'dataset_kwarg',
                id='read_dataset+dataset',
            ),
            pytest.param(
                'write',
                'write_dataset',
                'dataset_kwarg',
                id='write_dataset+dataset',
            ),
            pytest.param(
                'read',
                'read_dataset',
                'options',
                id='read_dataset+options',
            ),
            pytest.param(
                'write',
                'write_dataset',
                'options',
                id='write_dataset+options',
            ),
            pytest.param('read', 'read', 'options', id='read+options'),
            pytest.param('write', 'write', 'options', id='write+options'),
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
        """
        Test that unknown dataset keys are validated before stub operations.
        """
        handler_cls, _ = scientific_module_case
        handler = handler_cls()

        def _raise_stub_called(*_args: object, **_kwargs: object) -> object:
            raise AssertionError('stub operation should not be called')

        monkeypatch.setattr(StubFileHandlerABC, operation, _raise_stub_called)
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


class TestScientificDatasetHelpers:
    """Unit tests for low-level scientific dataset-key helper functions."""

    def test_normalize_store_dataset_keys(self) -> None:
        """Test that store-key normalization removing leading separators."""
        keys = scientific_dataset_mod.normalize_store_dataset_keys(
            ['/data', 'features', '//labels'],
        )
        assert keys == ['data', 'features', 'labels']

    @pytest.mark.parametrize(
        ('keys', 'dataset', 'expected'),
        [
            pytest.param(['data', 'features'], None, 'data', id='default-key'),
            pytest.param(
                ['data', 'features'],
                'features',
                'features',
                id='explicit-key',
            ),
            pytest.param([], None, None, id='empty-key-set'),
            pytest.param(['features'], None, 'features', id='single-key-fallback'),
        ],
    )
    def test_resolve_store_dataset_key_success_cases(
        self,
        keys: list[str],
        dataset: str | None,
        expected: str | None,
    ) -> None:
        """Test store dataset key resolution success cases."""
        assert (
            scientific_dataset_mod.resolve_store_dataset_key(
                keys,
                dataset=dataset,
                default_key='data',
                format_name='HDF5',
            )
            == expected
        )

    @pytest.mark.parametrize(
        ('keys', 'dataset', 'match'),
        [
            pytest.param(
                ['data', 'features'],
                'missing',
                "HDF5 dataset 'missing' not found",
                id='missing-explicit-key',
            ),
            pytest.param(
                ['features', 'labels'],
                None,
                'Multiple datasets found',
                id='ambiguous-key-set',
            ),
        ],
    )
    def test_resolve_store_dataset_key_error_cases(
        self,
        keys: list[str],
        dataset: str | None,
        match: str,
    ) -> None:
        """Test store dataset key resolution error cases."""
        with pytest.raises(ValueError, match=match):
            scientific_dataset_mod.resolve_store_dataset_key(
                keys,
                dataset=dataset,
                default_key='data',
                format_name='HDF5',
            )
