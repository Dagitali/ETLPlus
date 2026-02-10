"""
:mod:`tests.unit.file.test_u_file_sas7bdat` module.

Unit tests for :mod:`etlplus.file.sas7bdat`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etlplus.file import sas7bdat as mod
from etlplus.file.base import ReadOptions
from tests.unit.file.conftest import DictRecordsFrameStub
from tests.unit.file.conftest import OptionalModuleInstaller
from tests.unit.file.conftest import PandasReadSasStub
from tests.unit.file.conftest import ReadOnlyScientificDatasetModuleContract

# SECTION: TESTS ============================================================ #


class TestSas7bdatReadOnly(ReadOnlyScientificDatasetModuleContract):
    """Read-only scientific contract tests for :mod:`etlplus.file.sas7bdat`."""

    # pylint: disable=unused-variable

    module = mod
    handler_cls = mod.Sas7bdatFile
    format_name = 'sas7bdat'
    unknown_dataset_error_pattern = 'supports only dataset key'

    def prepare_unknown_dataset_env(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        optional_module_stub: OptionalModuleInstaller,
    ) -> None:
        """Ensure dataset-key validation occurs before optional imports."""
        _ = tmp_path
        monkeypatch.setattr(
            mod,
            'get_dependency',
            lambda *_, **__: (_ for _ in ()).throw(AssertionError),
        )
        monkeypatch.setattr(
            mod,
            'get_pandas',
            lambda *_: (_ for _ in ()).throw(AssertionError),
        )


class TestSas7bdatRead:
    """Unit tests for :func:`etlplus.file.sas7bdat.read`."""

    def test_list_datasets_returns_default_key(self) -> None:
        """Test list_datasets exposing the single supported key."""
        assert mod.Sas7bdatFile().list_datasets(Path('ignored.sas7bdat')) == [
            'data',
        ]

    def test_read_falls_back_when_format_kwarg_not_supported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test read fallback when pandas rejects the format keyword."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame, fail_on_format_kwarg=True)
        monkeypatch.setattr(mod, 'get_dependency', lambda *_, **__: object())
        monkeypatch.setattr(mod, 'get_pandas', lambda *_: pandas)

        result = mod.read(tmp_path / 'data.sas7bdat')

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': tmp_path / 'data.sas7bdat', 'format': 'sas7bdat'},
            {'path': tmp_path / 'data.sas7bdat'},
        ]

    def test_read_dataset_accepts_default_key_via_options(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test option-based default dataset selection for read_dataset."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame)
        monkeypatch.setattr(mod, 'get_dependency', lambda *_, **__: object())
        monkeypatch.setattr(mod, 'get_pandas', lambda *_: pandas)

        result = mod.Sas7bdatFile().read_dataset(
            tmp_path / 'data.sas7bdat',
            options=ReadOptions(dataset='data'),
        )

        assert result == [{'id': 1}]

    def test_read_uses_format_hint(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that read requests the SAS7BDAT format hint when supported."""
        frame = DictRecordsFrameStub([{'id': 1}])
        pandas = PandasReadSasStub(frame)
        monkeypatch.setattr(mod, 'get_dependency', lambda *_, **__: object())
        monkeypatch.setattr(mod, 'get_pandas', lambda *_: pandas)

        result = mod.read(tmp_path / 'data.sas7bdat')

        assert result == [{'id': 1}]
        assert pandas.read_calls == [
            {'path': tmp_path / 'data.sas7bdat', 'format': 'sas7bdat'},
        ]
