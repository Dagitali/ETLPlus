"""
:mod:`tests.unit.file.test_u_file_scientific_dataset_keys` module.

Focused unit tests for scientific handler dataset-key validation behavior.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pytest

from etlplus.file import mat as mat_mod
from etlplus.file import sylk as sylk_mod
from etlplus.file import zsav as zsav_mod
from etlplus.file.base import ReadOptions
from etlplus.file.base import ScientificDatasetFileHandlerABC
from etlplus.file.base import WriteOptions

# SECTION: INTERNAL CONSTANTS =============================================== #


_SCIENTIFIC_STUB_MODULES: list[
    tuple[ModuleType, type[ScientificDatasetFileHandlerABC]]
] = [
    (mat_mod, mat_mod.MatFile),
    (sylk_mod, sylk_mod.SylkFile),
    (zsav_mod, zsav_mod.ZsavFile),
]


# SECTION: TESTS ============================================================ #


class TestScientificStubDatasetKeys:
    """
    Unit tests for dataset-key validation in stub-backed scientific files.
    """

    @pytest.mark.parametrize(
        ('module', 'handler_cls'),
        _SCIENTIFIC_STUB_MODULES,
        ids=['mat', 'sylk', 'zsav'],
    )
    def test_dataset_methods_honor_options_dataset_selector(
        self,
        module: ModuleType,
        handler_cls: type[ScientificDatasetFileHandlerABC],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test read_dataset/write_dataset honoring dataset selectors from
        options.
        """
        handler = handler_cls()
        monkeypatch.setattr(
            module.stub,
            'read',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )
        monkeypatch.setattr(
            module.stub,
            'write',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )

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

    @pytest.mark.parametrize(
        ('module', 'handler_cls'),
        _SCIENTIFIC_STUB_MODULES,
        ids=['mat', 'sylk', 'zsav'],
    )
    def test_list_datasets_returns_single_default_key(
        self,
        module: ModuleType,
        handler_cls: type[ScientificDatasetFileHandlerABC],
    ) -> None:
        """Test list_datasets exposing only the default dataset key."""
        _ = module
        handler = handler_cls()

        assert handler.list_datasets(Path('ignored.file')) == ['data']

    @pytest.mark.parametrize(
        ('module', 'handler_cls'),
        _SCIENTIFIC_STUB_MODULES,
        ids=['mat', 'sylk', 'zsav'],
    )
    def test_read_dataset_rejects_unknown_key_without_calling_stub(
        self,
        module: ModuleType,
        handler_cls: type[ScientificDatasetFileHandlerABC],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read_dataset rejecting unknown keys before stub reads."""
        handler = handler_cls()
        monkeypatch.setattr(
            module.stub,
            'read',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )

        with pytest.raises(ValueError, match='supports only dataset key'):
            handler.read_dataset(Path('ignored.file'), dataset='unknown')

    @pytest.mark.parametrize(
        ('module', 'handler_cls'),
        _SCIENTIFIC_STUB_MODULES,
        ids=['mat', 'sylk', 'zsav'],
    )
    def test_write_dataset_rejects_unknown_key_without_calling_stub(
        self,
        module: ModuleType,
        handler_cls: type[ScientificDatasetFileHandlerABC],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test write_dataset rejecting unknown keys before stub writes."""
        handler = handler_cls()
        monkeypatch.setattr(
            module.stub,
            'write',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )

        with pytest.raises(ValueError, match='supports only dataset key'):
            handler.write_dataset(
                Path('ignored.file'),
                [],
                dataset='unknown',
            )

    @pytest.mark.parametrize(
        ('module', 'handler_cls'),
        _SCIENTIFIC_STUB_MODULES,
        ids=['mat', 'sylk', 'zsav'],
    )
    def test_read_and_write_options_route_unknown_dataset_to_validation(
        self,
        module: ModuleType,
        handler_cls: type[ScientificDatasetFileHandlerABC],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Test option-based dataset selectors following the same validation.
        """
        handler = handler_cls()
        monkeypatch.setattr(
            module.stub,
            'read',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )
        monkeypatch.setattr(
            module.stub,
            'write',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )

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
